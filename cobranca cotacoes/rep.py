import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

db = create_engine("postgresql+psycopg2://compras:pecist%40compr%40s2024@srvdados:5432/postgres")

def intervalo_venda():
    # 3 dias na segunda, 1 dia nos outros dias
    if datetime.now().weekday() == 0:  # segunda-feira
        return 3
    else:
        return 1
    
def buscar_venda():
    dias_atras_v = intervalo_venda()
    query = f"""
        SELECT DISTINCT 
            pp.codcli,tpd.operador
        FROM 
            "D-1".prod_ped pp
        JOIN 
            "D-1".tpd013 tpd ON pp.codvde = tpd.codoper
        JOIN
            "D-1".cliente cl ON pp.codcli = cl.codcli
        JOIN 
            "D-1".orcame orc ON pp.codcli = orc.codcli
        WHERE	
            pp.dt_emissao = CURRENT_DATE - INTERVAL '{dias_atras_v} day'
            AND orc.in_tipodes = 'O'
            AND pp.cd_loja = '08'
        ORDER BY
            pp.codcli ASC;
        """
    try:
      with db.connect() as connection:
        df_vendas =  pd.read_sql_query(query,connection)
        return df_vendas
    except Exception as e:
        print(f"Erro ao realizar consulta: {e}")
        return None

def intervalo_orcamento():
    # 4 dias na segunda e terça, 2 dias nos outros dias
    wd = datetime.now().weekday()
    if wd == 0 or wd == 1:  # segunda ou terça
        return 4
    else:
        return 2

def buscar_orcamento():
    dias_atras_orc = intervalo_orcamento()
    query =f"""
        SELECT DISTINCT
            po.codcli,tpd.operador
        FROM
            "D-1".prod_orc po
        JOIN 
            "D-1".cliente cli ON po.codcli = cli.codcli
        JOIN
            "D-1".tpd013 tpd ON po.codvde = tpd.codoper
        JOIN
            "D-1".orcame orc ON po.codcli = orc.codcli
        WHERE
            orc.in_tipodes = 'O' 
            AND po.dt_emissao >= CURRENT_DATE - INTERVAL '{dias_atras_orc} day'                                                                     
            AND po.cd_loja = '08'
        ORDER BY
            po.codcli ASC;
        """
    try:
        with db.connect() as connection:
            df_orcamento = pd.read_sql_query(query,connection)
            return df_orcamento
    except Exception as e:
        print(f"Erro ao realizar a consulta: {e}")
        return None
    
def report(vendedores_venda,vendedores_orcamento):
    with open("repo.txt","w", encoding='utf-8') as file:
        if not vendedores_venda and not vendedores_orcamento:
            file.write("Nenhum vendedor encontrado")
            return
  
        for codcli, vendedor in vendedores_venda.items() :
            if codcli in vendedores_orcamento:
                file.write(f'Vendedor: {vendedor}\n')
                file.write(f'Cliente: {codcli}\n')
                file.write('Erro: Não enviou orçamento no grupo\n')
            else:
                file.write(f'Vendedor: {vendedor}\n')
                file.write(f'Cliente: {codcli}\n')
                file.write('Erro: Não enviou orçamento no grupo e não salvou orçamento no SIAC\n')
            file.write('\n')

def main():

    df_v = buscar_venda()
    df_o = buscar_orcamento()

    vendedores_venda = {}
    vendedores_orcamento = {}

    if df_v is not None and not df_v.empty:
        vendedores_venda = dict(zip(df_v['codcli'],df_v['operador']))
    else:
        print("Nenhum venda foi encontrada")  

    if df_o is not None and not df_o.empty:
        vendedores_orcamento = dict(zip(df_o['codcli'],df_o['operador']))
    else: 
        print("Nenhum orçamento foi encontrada")

    report(vendedores_venda,vendedores_orcamento)
    print("Os resultados foram salvos em repo.txt")

if __name__ == "__main__":
    main()
