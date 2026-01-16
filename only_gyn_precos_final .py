from tkinter import N
import pandas as pd 
from sqlalchemy import create_engine
import re

# Conexão com o banco de dados
db = create_engine("postgresql+psycopg2://compras:pecist%40compr%40s2024@srvdados:5432/postgres")

vendedor = 'victor'

# Mensagem de entrada 
mensagens = [
  """
Código do cliente: I5785
Código do Produto: 0204082081
Marca: BOSCH
Concorrente: AUTO NORTE 
Preço concorrente: 20,71
"""
]

# Função para extrair os dados após os dois-pontos
def extrair_valores(mensagem):
    padrao = r"[:]\s*(?:\.*\s*)([^\n]*)"
    valores = re.findall(padrao, mensagem)
    return valores


# Dicionário de substituições
sub = {
    'SPEED': 'SPEEDBRAKE',
    'HIPER FREIOS': 'HIPPER FRE',
    'HIPPER FREIOS': 'HIPPER FRE',
    'MULTIQUALITA': 'MULTIQUALI',
    'VETOR': 'VTO',
    'METAL': 'METAL LEVE',
    '.MTE': 'MTE',
    'FRASLE': 'FRAS-LE',
    'RAINHA DAS SETE': 'RAINHA DA',
    'PHIPS': 'PHILIPS',
    'MAXOM': 'MAXON',
    'KAIABA': 'KYB',
    'MAGNETI MARELLI': 'MAGNETI',
    'KAYABA': 'KYB',
    'SHADECK': 'SCHADEK',
    'SCHADEK': 'SCHADEK',
    'MONROE': 'MONR&AXIOS',
    'AXIOS': 'MONR&AXIOS',
    'ATE' : 'ATE/VDO',
    'SUN' : 'SUN ELETRI',
    'SCHADECK': 'SCHADEK'
}

def dar_desconto(codcli, sigla, vendedor, cod_prod, variacao_percentual, preco_concorrente):
    print("-- Mensagem para quem dá desconto--")
    print(f"Cliente/Tipo: {codcli}/{sigla}")
    print(f"Vendedor: {vendedor}")
    print(f"Produto: {cod_prod}")
    print(f"Desconto: {variacao_percentual}%")
    print(f"Preço Final: R$ {preco_concorrente}")

def processar_mensagem(mensagem):
    # Limitar a mensagem às 5 primeiras linhas
    linhas = mensagem.strip().split("\n")
    mensagem_limited = "\n".join(linhas[:5])  # Pega apenas as 5 primeiras linhas

    valores_extraidos = extrair_valores(mensagem_limited)

    if len(valores_extraidos) >= 3:
        cod_cliente = valores_extraidos[0].upper().strip()
        cod_prod = valores_extraidos[1].upper().strip()
        concorrente = valores_extraidos[3].upper().strip()
        marca = valores_extraidos[2].strip().upper()

        # Substituir as marcas usando o dicionário 'sub'
        for key, value in sub.items():
            if key in marca:
                marca = value
                break  # Se uma correspondência for encontrada, substitui e sai do loop

        # Substituir "MAGNET" por "COFAP" se a marca começar com "MAGNET"
        if marca.startswith("MAGNET"):
            marca = "COFAP"

        # Remove qualquer texto (como "R$") antes do número
        preco_concorrente = float(re.sub(r'[^\d.,]', '', valores_extraidos[-1]).replace(',', '.').strip())

        # Verificação do estoque do produto na loja 08
        query_estoque = """
        SELECT pl.estoque
        FROM "H-1".prd_loja pl
        JOIN "D-1".produto p ON pl.codpro = p.codpro
        WHERE pl.cd_loja = '08' AND p.num_fab = %(cod_prod)s;
        """
        resultado_estoque = pd.read_sql(query_estoque, db, params={"cod_prod": cod_prod})

        if not resultado_estoque.empty and resultado_estoque.loc[0, 'estoque'] > 0:
            # Produto tem estoque suficiente na loja 08, pode continuar o processamento

            # Consulta do cliente
            query_cli = """
            SELECT codcli, sigladesc
            FROM "D-1".cliente
            WHERE codcli = %(cod_cliente)s;
            """
            result_cli = pd.read_sql(query_cli, db, params={"cod_cliente": cod_cliente})

            if result_cli.empty:
                print(f"Cliente {cod_cliente} não encontrado.")
                return None  

            codcli = result_cli.loc[0, "codcli"]
            sigla = result_cli.loc[0, "sigladesc"]

            # Verificando se sigladesc é nulo e tratando 
            if pd.isnull(sigla):  
                sigla = "" 

            sigla = sigla.strip().upper()  

            if not sigla:
                coluna_acre = None  # Não buscar a coluna pc_acre se sigla for vazia ou nula
            else:
                # Caso contrário, a sigladesc foi encontrada, então utilizamos a lógica de sigla do cliente
                coluna_acre = f"pc_acre_{sigla.lower()}" if sigla != 'K' else "pc_acre_k"

            # Função para consultar o produto e calcular a variação
            def consultar_produto_por_marca(cod_prod, marca):
                query_prod = f"""
                SELECT 
                    num_fab,
                    p_venda,
                    {coluna_acre if coluna_acre else 'NULL'} AS pc_acre,
                    p_promo,  
                    p_compra,
                    pc_ipi,
                    pc_subtri,
                    pc_outdes,
                    pc_royalt,
                    p.pc_frete,
                    pc_financ,
                    pc_creicm,
                    pc_piscof,
                    pc_ircsll,
                    pc_debicm,
                    forn.fantasia,
                    g.grupo
                FROM 
                    "H-1".prd_tipo p
                JOIN 
                    "D-1".for_tipo f ON p.cd_tploja = f.cd_tploja
                JOIN 
                    "D-1".produto prod ON p.codpro = prod.codpro
                JOIN 
                    "D-1".fornec forn ON f.cd_fornece = forn.codfor 
                JOIN
                    "D-1".grupo g ON prod.codgru = g.codgru
                WHERE 
                    prod.num_fab = %(cod_prod)s
                    AND f.cd_tploja = '02'     
                    AND forn.fantasia = %(marca)s;
                """
                return pd.read_sql(query_prod, db, params={"cod_prod": cod_prod, "marca": marca})

            # Processar a consulta
            result_prod = consultar_produto_por_marca(cod_prod, marca)
            grupo = result_prod['grupo'].values[0] if not result_prod.empty else None
            if result_prod.empty:
                print(f"Produto {cod_prod} da marca {marca} não encontrado.")
                return None  

            p_venda = result_prod['p_venda'].values[0]

            # p_promo só será considerado se for maior que 0
            p_promo = result_prod["p_promo"].dropna().min()
            if pd.isnull(p_promo) or p_promo <= 0:
                p_promo = None

            # Aplicar desconto
            pc_acre_k_min = result_prod['pc_acre'].min() if coluna_acre else 0
            p_venda_com_desconto = p_venda * (1 + (pc_acre_k_min / 100)) if not pd.isnull(pc_acre_k_min) else p_venda

            # Calcular o menor valor entre o com desconto e a promoção 
            if p_promo is not None:
                p_venda_final = min(p_venda_com_desconto, p_promo)
            else:
                p_venda_final = p_venda_com_desconto

            # Calcular a variação percentual entre o preço do concorrente e o novo preço de venda ajustado
            variacao_percentual = round(abs((preco_concorrente - p_venda_final) / p_venda_final) * 100, 2)

            # Cálculo do Custo
            p_compra = result_prod['p_compra'].values[0]
            custo = p_compra

            # Calcular o custo acumulado com base nas porcentagens
            for col in ['pc_ipi', 'pc_subtri', 'pc_outdes', 'pc_frete', 'pc_financ', 'pc_royalt']:
                percentual = result_prod[col].values[0]

                if not pd.isnull(percentual):
                    if col == 'pc_royalt':
                        custo += p_compra * (percentual / 100)  # Só o royalty usa o valor base
                    else:
                        custo += custo * (percentual / 100)  # Os demais usam o custo acumulado

            # Subtrair a porcentagem de pc_creicm (com base no valor de compra inicial)
            if not pd.isnull(result_prod['pc_creicm'].values[0]):
                custo -= p_compra * (result_prod['pc_creicm'].values[0] / 100)

            # Calcular os impostos com base no preço do concorrente
            impostos = 0
            nosso_impostos = 0
            for col in ['pc_piscof', 'pc_ircsll', 'pc_debicm']:
                if not pd.isnull(result_prod[col].values[0]):
                    impostos += preco_concorrente * (result_prod[col].values[0] / 100)
                    nosso_impostos += p_venda_final * (result_prod[col].values[0] / 100)

            # Calcular a margem (MCP)
            nossa_margem_reais = p_venda_final - (custo + nosso_impostos)
            nossa_mcp_percentual = (nossa_margem_reais / p_venda_final) * 100
        
            margem_reais = preco_concorrente - (custo + impostos)
            mcp_percentual = (margem_reais / preco_concorrente) * 100

            # Exibir a mensagem final
            print(f"\n--- Resultado para o Produto {cod_prod} ---")
            print(f"Cliente/Tipo: {codcli}/{sigla}")
            print(f"Vendedor: {vendedor}")
            print(f"Concorrente: {concorrente}")
            print(f"Preço do Concorrente: R$ {preco_concorrente}")
            print(f"Marca: {marca}") 
            print(f"Produto: {grupo}")
            print(f"Variação Percentual: {variacao_percentual}%")
            print(f"Nossa MCP: {nossa_mcp_percentual:.4f}%")
            print(f"MCP: {mcp_percentual:.4f}%")
            print("\n")

            if mcp_percentual >= 3:
                dar_desconto(codcli, sigla, vendedor, cod_prod, variacao_percentual, preco_concorrente)
            elif nossa_mcp_percentual < 3:
                print(f"MCP do item já é menor que 3%: {nossa_mcp_percentual:.4f}%*")
            else:
                # Verificar se a MCP é menor que 3% e ajustar o preço de venda, se necessário
                if mcp_percentual < 3:
                    # Verificar se o nosso preço do produto para o cliente é menor que R$30
                    if p_venda_final  < 30:
                        margem_desejada = 0.031  # Nova margem desejada

                    else:
                        margem_desejada = 0.0305  # Usar a margem anterior se não for menor que 30 reais

                    impostos_percentuais = sum([
                        result_prod[col].values[0] for col in ['pc_piscof', 'pc_ircsll', 'pc_debicm']
                        if not pd.isnull(result_prod[col].values[0])
                    ]) / 100

                    limite_denominador = 1 - impostos_percentuais - margem_desejada

                    # Proteção contra divisão inválida
                    if limite_denominador <= 0:
                        print(" Erro: Limite denominador inválido para o cálculo do novo preço de venda.")
                    else:
                        novo_preco_venda = round(custo / limite_denominador, 2)

                        impostos_novos = novo_preco_venda * impostos_percentuais
                        mcp_final = ((novo_preco_venda - (custo + impostos_novos)) / novo_preco_venda) * 100

                        nova_variacao_percentual = round(((novo_preco_venda - p_venda_final) / p_venda_final) * 100, 2)

                        print(f"*Não conseguimos fazer pelo preço!! Ajuste calculado*")
                        print(f"Novo Preço de Venda: R$ {novo_preco_venda}")
                        print(f"Nova Variação Percentual: {nova_variacao_percentual}%")
                        print(f"MCP Final: {mcp_final:.4f}%")
                        dar_desconto(codcli, sigla, vendedor, cod_prod, nova_variacao_percentual, novo_preco_venda)
        else:
            print(f"\nNão há estoque para o produto {cod_prod} na loja 08.")

# Processar todas as mensagens de entrada
for mensagem in mensagens:
    processar_mensagem(mensagem)
