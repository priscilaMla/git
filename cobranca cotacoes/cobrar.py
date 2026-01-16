import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

conn = create_engine("postgresql+psycopg2://compras:pecist%40compr%40s2024@srvdados:5432/postgres")

def vendas():
    query = """
        SELECT DISTINCT
            pp.codcli,
            tpd.operador
        FROM 
            "D-1".prod_ped pp
        JOIN 
            "D-1".tpd013 tpd ON pp.codvde = tpd.codoper
        JOIN 
            "D-1".cliente cl ON pp.codcli = cl.codcli
        JOIN 
            "D-1".orcame orc ON pp.codcli = orc.codcli
        WHERE	
            pp.dt_emissao = CURRENT_DATE - INTERVAL '1 days' -- mudar para 3 dias na segunda --
            AND orc.in_tipodes = 'O'
            AND pp.cd_loja = '08'
        ORDER BY
            pp.codcli ASC;
    """
    try:
        with conn.connect() as connection:
            df_vendas = pd.read_sql_query(query, connection)
            return df_vendas
    except Exception as e:
        print(f"Erro ao realizar consulta: {e}")
        return None
    
def cobranca(vendas):
    hoje = datetime.now()
    ontem = hoje - timedelta(days=1) ###### mudar para 3 dias na segunda ######

    with open ('cobranca.txt','w', encoding='utf-8') as file:
        if not vendas:
            file.write("Nenhum vendedor encontrado.")
            return
        
        vendedores = {}
        for codcli, vendedor in vendas.items():
            if vendedor not in vendedores:
                vendedores[vendedor] = []
            vendedores[vendedor].append(codcli)
        
        for vendedor, codcli in vendedores.items():
            if len(codcli) > 1: 
                codcli = ', '.join(codcli)
                file.write(f"Bom dia, @{vendedor}! Pode me mandar a cotação da venda feita no dia {ontem.strftime('%d/%m')} dos clientes {codcli} aqui no grupo?\n") 
            else:    
                codcli = codcli[0]
                file.write(f"Bom dia, @{vendedor}! Pode me mandar a cotação da venda feita no dia {ontem.strftime('%d/%m')} do cliente {codcli} aqui no grupo?\n")
            file.write('\n')
def main():
    df_vendas = vendas()
    if df_vendas is not None and not df_vendas.empty:
        print("Consulta realizada com sucesso!")
        codcli_vendedor = dict(zip(df_vendas['codcli'].astype(str), df_vendas['operador']))
        cobranca(codcli_vendedor)
        print("Cobranças salvas em cobranca.txt")
    else:
        print("Erro ao realizar a consulta. Nenhum dado encontrado.")

if __name__ == "__main__":
    main()