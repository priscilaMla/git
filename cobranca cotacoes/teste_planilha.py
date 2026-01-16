import psycopg2
import pandas as pd
import streamlit as st
import io
import re

#streamlit run teste_planilha.py

def _limpar_prefixo_whatsapp(linha: str) -> str:
    """
    Remove prefixos do WhatsApp do tipo:
    [07:10, 09/09/2025] 24021Ancora > Edson): N99162
    ou qualquer coisa antes do último ':'
    Retorna o trecho após o último ':'.
    """
    if ':' in linha:
        linha = linha.rsplit(':', 1)[1]
    return linha.strip().rstrip(')').strip()

def _clean_code(s: str) -> str:
    # mantém só letras e números; remove '-' e demais símbolos
    return re.sub(r'[^A-Z0-9]', '', s.upper())

def parse_produtos(texto):
    produtos = []
    for linha in texto.strip().split("\n"):
        linha = linha.strip()
        if not linha:
            continue

        linha = _limpar_prefixo_whatsapp(linha)

        partes = [p.strip() for p in linha.split(',', 2)]  # até 3 partes
        produto_bruto = partes[0].upper()
        qtd = 1
        fantasia = None

        if len(partes) == 2:
            try:
                qtd = int(partes[1])
                fantasia = None
            except ValueError:
                qtd = 1
                fantasia = partes[1]
        elif len(partes) == 3:
            try:
                qtd = int(partes[1])
            except ValueError:
                qtd = 1
            fantasia = partes[2]

         # extrai "fora" e "dentro" dos parênteses, ex: N-96033(BTC-03109)
        m = re.search(r'\(([^)]+)\)', produto_bruto)
        dentro = m.group(1) if m else None
        fora = re.sub(r'\(.*\)', '', produto_bruto)  # remove o "(...)"

        candidatos = []
        # PRIORIDADE: dentro do parênteses, depois fora
        for pedaco in [dentro, fora]:
            if pedaco:
                limp = _clean_code(pedaco)
                if limp and limp not in candidatos:
                    candidatos.append(limp)

        # mantém compatibilidade: "produto" vira o primeiro candidato (se houver)
        if not candidatos:
            candidatos = [_clean_code(produto_bruto)]

        produto = candidatos[0]

        produtos.append({"produto": produto, "quantidade": qtd, "marca": fantasia})
    return produtos

def mapear_para_num_fab(codigos):
    conn = psycopg2.connect(
        host="srvdados",
        database="postgres",
        user="compras",
        password="pecist@compr@s2024"
    )
    cur = conn.cursor()

    cur.execute('''
        SELECT num_orig, num_fab
        FROM "D-1".produto
        WHERE num_orig = ANY(%s) OR num_fab = ANY(%s)
    ''', (codigos, codigos))

    resultados = cur.fetchall()
    cur.close()
    conn.close()

    mapa = {}
    for num_orig, num_fab in resultados:
        mapa[num_orig.upper()] = num_fab.upper()
        mapa[num_fab.upper()] = num_fab.upper()
    return mapa

def buscar_substituto_mais_barato(num_fab_base: str, sigladesc: str):
    """
    Dado um num_fab (do item consultado) e a sigla do cliente,
    encontra entre os produtos com o MESMO num_orig o MAIS BARATO
    (já aplicando regra de preço para cliente 'O', se aplicável)
    que TENHA estoque na loja '08'.

    Retorna: (preco_unitario, marca_fantasia) ou (None, None) se não achar.
    """
    conn = psycopg2.connect(
        host="srvdados",
        database="postgres",
        user="compras",
        password="pecist@compr@s2024"
    )
    cur = conn.cursor()

    # 1) Descobre o num_orig do produto base
    cur.execute('''
        SELECT num_orig
        FROM "D-1".produto
        WHERE num_fab = %s
          AND (in_lixeira IS NULL OR in_lixeira != 'S')
          AND (in_inativo IS NULL OR in_inativo != 'S')
          AND (in_foralin IS NULL OR in_foralin != 'S')
        LIMIT 1
    ''', (num_fab_base,))
    row = cur.fetchone()
    if not row or not row[0]:
        cur.close(); conn.close()
        return (None, None)
    num_orig = row[0]

    # 2) Lista TODOS os equivalentes (mesmo num_orig) com preço e estoque
    #    e escolhe o mais barato com estoque > 0
    if sigladesc == 'O':
        query_equivalentes = '''
            SELECT 
                pro.num_fab,
                f.fantasia,
                prd.p_venda * (1 + COALESCE(ft.pc_acre_o, 0) / 100.0) AS preco_ajustado,
                COALESCE(pl.estoque, 0) AS estoque
            FROM "D-1".produto pro
            JOIN "D-1".prd_tipo prd 
                ON prd.codpro = pro.codpro AND prd.cd_tploja = '02'
            JOIN "D-1".for_tipo ft 
                ON pro.codfor = ft.cd_fornece AND ft.cd_tploja = prd.cd_tploja
            JOIN "D-1".fornec f 
                ON ft.cd_fornece = f.codfor
            LEFT JOIN "D-1".prd_loja pl
                ON pl.codpro = pro.codpro AND pl.cd_loja = '08'
            WHERE pro.num_orig = %s
              AND (pro.in_lixeira IS NULL OR pro.in_lixeira != 'S')
              AND (pro.in_inativo IS NULL OR pro.in_inativo != 'S')
              AND (pro.in_foralin IS NULL OR pro.in_foralin != 'S')
        '''
    else:
        query_equivalentes = '''
            SELECT 
                pro.num_fab,
                f.fantasia,
                prd.p_venda AS preco_ajustado,
                COALESCE(pl.estoque, 0) AS estoque
            FROM "D-1".produto pro
            JOIN "D-1".prd_tipo prd 
                ON prd.codpro = pro.codpro AND prd.cd_tploja = '02'
            JOIN "D-1".for_tipo ft 
                ON pro.codfor = ft.cd_fornece AND ft.cd_tploja = prd.cd_tploja
            JOIN "D-1".fornec f 
                ON ft.cd_fornece = f.codfor
            LEFT JOIN "D-1".prd_loja pl
                ON pl.codpro = pro.codpro AND pl.cd_loja = '08'
            WHERE pro.num_orig = %s
              AND (pro.in_lixeira IS NULL OR pro.in_lixeira != 'S')
              AND (pro.in_inativo IS NULL OR pro.in_inativo != 'S')
              AND (pro.in_foralin IS NULL OR pro.in_foralin != 'S')
        '''
    cur.execute(query_equivalentes, (num_orig,))
    rows = cur.fetchall()

    cur.close(); conn.close()

    # Filtra os que têm estoque > 0 e escolhe o menor preço
    candidatos = [(preco, marca) for (_num_fab, marca, preco, estoque) in rows if estoque and estoque > 0 and preco is not None]
    if not candidatos:
        return (None, None)
    preco_min, marca_barata = min(candidatos, key=lambda x: x[0])
    return (preco_min, marca_barata)

def buscar_precos_e_estoque(cliente, codigos_num_fab, marcas_digitadas):
    conn = psycopg2.connect(
        host="srvdados",
        database="postgres",
        user="compras",
        password="pecist@compr@s2024"
    )
    cur = conn.cursor()

    # 1) Buscar sigladesc
    cur.execute('SELECT sigladesc FROM "D-1".cliente WHERE codcli = %s', (cliente,))
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        # nada encontrado para o cliente
        return {}, None

    sigladesc = row[0]  # <<< FALTAVA ISSO

    resultados = {}      # <<< E TAMBÉM FALTAVA ISSO

    # 2) Preço e estoque do próprio num_fab
    for num_fab, marca_digitada in zip(codigos_num_fab, marcas_digitadas):
        if not num_fab:
            resultados[num_fab] = {"preco": None, "estoque": None, "marca": None}
            continue

        if sigladesc == 'O':
            query_preco = '''
                SELECT prd.p_venda * (1 + MAX(ft.pc_acre_o) / 100.0) AS preco_ajustado,
                       f.fantasia
                FROM "D-1".prd_tipo prd
                JOIN "D-1".produto pro ON prd.codpro = pro.codpro
                JOIN "D-1".for_tipo ft ON pro.codfor = ft.cd_fornece AND ft.cd_tploja = prd.cd_tploja
                JOIN "D-1".fornec f ON ft.cd_fornece = f.codfor
                WHERE prd.cd_tploja = '02'
                  AND (f.fantasia = %s OR %s IS NULL)
                  AND pro.num_fab = %s
                  AND (pro.in_lixeira IS NULL OR pro.in_lixeira != 'S')
                  AND (pro.in_inativo IS NULL OR pro.in_inativo != 'S')
                  AND (pro.in_foralin IS NULL OR pro.in_foralin != 'S')
                GROUP BY prd.p_venda, f.fantasia
            '''
        else:
            query_preco = '''
                SELECT prd.p_venda,
                       f.fantasia
                FROM "D-1".prd_tipo prd
                JOIN "D-1".produto pro ON prd.codpro = pro.codpro
                JOIN "D-1".for_tipo ft ON pro.codfor = ft.cd_fornece AND ft.cd_tploja = prd.cd_tploja
                JOIN "D-1".fornec f ON ft.cd_fornece = f.codfor
                WHERE prd.cd_tploja = '02'
                  AND (f.fantasia = %s OR %s IS NULL)
                  AND pro.num_fab = %s
                  AND (pro.in_lixeira IS NULL OR pro.in_lixeira != 'S')
                  AND (pro.in_inativo IS NULL OR pro.in_inativo != 'S')
                  AND (pro.in_foralin IS NULL OR pro.in_foralin != 'S')
            '''
        cur.execute(query_preco, (marca_digitada, marca_digitada, num_fab))
        preco_result = cur.fetchone()

        if preco_result:
            preco = preco_result[0]
            marca_real = preco_result[1]
        else:
            preco = None
            marca_real = None

        query_estoque = '''
            SELECT prd.estoque,
                   f.fantasia
            FROM "D-1".prd_loja prd
            JOIN "D-1".produto pro ON prd.codpro = pro.codpro
            JOIN "D-1".fornec f ON pro.codfor = f.codfor
            WHERE prd.cd_loja = '08'
              AND pro.num_fab = %s
              AND (f.fantasia = %s OR %s IS NULL)
        '''
        cur.execute(query_estoque, (num_fab, marca_digitada, marca_digitada))
        estoque_result = cur.fetchone()
        estoque_atual = estoque_result[0] if estoque_result else None

        resultados[num_fab] = {
            "preco": preco,
            "estoque": estoque_atual,
            "marca": marca_real
        }

    cur.close()
    conn.close()

    # <<< AGORA RETORNA OS DOIS
    return resultados, sigladesc

st.title("Consulta de Preços")

cliente = st.text_input("Código do Cliente")
produtos_texto = st.text_area(
    "Produtos (formato: código,quantidade,marca (opcional). Produtos sem quantidade serão considerados como 1)",
    height=200
)

if st.button("Consultar preços"):
    if not cliente:
        st.error("Por favor, informe o cliente.")
    elif not produtos_texto.strip():
        st.error("Por favor, informe os produtos.")
    else:
        produtos = parse_produtos(produtos_texto)
        codigos_entrada = [p["produto"] for p in produtos]
        quantidades = [p["quantidade"] for p in produtos]
        marcas_digitadas = [p["marca"] for p in produtos]

        mapa_codigos = mapear_para_num_fab(codigos_entrada)
        codigos_num_fab = [mapa_codigos.get(c, None) for c in codigos_entrada]

        resultados, sigladesc = buscar_precos_e_estoque(cliente, codigos_num_fab, marcas_digitadas)

        dados = []
        for entrada, num_fab, qtd, marca_digitada in zip(codigos_entrada, codigos_num_fab, quantidades, marcas_digitadas):
            if num_fab is None:
            # Produto não existe no cadastro
                dados.append({
                    'Código Digitado': entrada,
                    'Num_Fab': '',
                    'Marca': marca_digitada if marca_digitada else '',
                    'Marca_kaizen':marca_kaizen,
                    'Quantidade': qtd,
                    'Preço Unitário': None,
                    'Total': None,
                    'Total_marca_kaizen': None,
                    'Observação': 'Não cadastrado'
                })
                continue

            r = resultados.get(num_fab, {"preco": None, "estoque": None, "marca": None})
            preco_unit = r["preco"]
            estoque_disponivel = r["estoque"]
            marca_real = r["marca"]

            total_marca_kaizen = None
            marca_kaizen = marca_real if marca_real else ''

            if preco_unit is None or estoque_disponivel is None or estoque_disponivel == 0 or (qtd and estoque_disponivel is not None and qtd > estoque_disponivel):
                # Sem estoque suficiente -> buscar similar mais barato (mesmo num_orig)
                preco_sub, marca_sub = buscar_substituto_mais_barato(num_fab, sigladesc)
                if preco_sub is not None:
                    total_marca_kaizen = preco_sub * (qtd or 0)
                    marca_kaizen = marca_sub or marca_kaizen
                preco = None
                total = None
                if estoque_disponivel is None or estoque_disponivel == 0:
                    observacao = "Sem estoque"
                else:
                    observacao = f"Solicitado {qtd}, disponível {estoque_disponivel}"
            else:
                # Tem estoque suficiente do próprio item
                preco = preco_unit
                total = preco * qtd
                observacao = ""

            dados.append({
                'Código Digitado': entrada,
                'Num_Fab': num_fab,
                'Marca': marca_digitada if marca_digitada else '',
                'Marca_kaizen': marca_real if marca_real else '',                
                'Qtde': qtd,
                'Preço Unit': preco,
                'Total': total,
                'Total_marca_kaizen': total_marca_kaizen,
                'Obs': observacao
            })

        df = pd.DataFrame(dados)
        st.dataframe(df)