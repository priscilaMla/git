# %%
import pandas as pd
import numpy as np
import psycopg2

# %%
df = pd.read_csv(
    "acompanhamento_suitability(Planilha1).csv",
    sep=";",
    encoding="latin1"
)

df.columns = df.columns.astype(str).str.strip()
df = df.loc[:, ~df.columns.str.contains("Unnamed", na=False)]
df = df.loc[:, df.columns != ""]  # remove coluna vazia do ;; se existir

print(df.head())
print(df.columns)

# %%
# limpar Cliente
df["Cliente"] = df["Cliente"].astype(str).str.strip()

# manter ordem original
df["ID_ordem"] = range(len(df))

# %%
# limpar tipo/tamanho e criar classificacao
df["tipo"] = df["tipo"].astype(str).str.strip()
df["tamanho"] = df["tamanho"].astype(str).str.strip()

df["classificacao"] = (df["tipo"] + " " + df["tamanho"]).astype(str)
df["classificacao"] = df["classificacao"].str.replace(r"\s+", " ", regex=True).str.strip()

# remover Não qualificado
df = df[df["classificacao"] != "Não qualificado"].copy()

print(df[["Cliente", "tipo", "tamanho", "classificacao"]].head())

# renomear coluna procura se existir
if "Unnamed: 2" in df.columns:
    df = df.rename(columns={"Unnamed: 2": "procura"})

# %%
# contagem por classificacao
contagem = df["classificacao"].value_counts()
percentual = df["classificacao"].value_counts(normalize=True) * 100

print("\n=== Quantidade por classificação ===")
print(contagem)

print("\n=== Percentual por classificação ===")
print(percentual)

# %%
# conexão
conn_params = {
    "host": "srvdados",
    "database": "postgres",
    "user": "compras",
    "password": "pecist@compr@s2024"
}

cli = '''
select
  codcli,
  sum(preco) as preco,
  sum(qtde_ven) as qtde,
  sum(qtde_ven * preco) as faturamento
from "D-1".prod_ped
where cd_loja = '08' and dt_emissao >= '2024-10-01'
group by codcli
'''

with psycopg2.connect(**conn_params) as conn:
    clientes = pd.read_sql(cli, conn)

# %%
# limpar chave e faturamento
clientes["codcli"] = clientes["codcli"].astype(str).str.strip()
clientes["faturamento"] = pd.to_numeric(clientes["faturamento"], errors="coerce").fillna(0)

# remover faturamento 0 na query
antes = clientes["codcli"].nunique()
clientes = clientes[clientes["faturamento"] > 0].copy()
depois = clientes["codcli"].nunique()
print(f"Clientes removidos por faturamento zero (na query): {antes - depois}")

clientes = clientes.rename(columns={"codcli": "Cliente"})
clientes_merge = clientes[["Cliente", "faturamento", "qtde", "preco"]].copy()

# %%
# merge faturamento no df
df = df.merge(clientes_merge, on="Cliente", how="left")
df["faturamento"] = df["faturamento"].fillna(0)

# remover do df clientes sem faturamento (0 ou ausente)
antes_df = df["Cliente"].nunique()
clientes_ativos = df.loc[df["faturamento"] > 0, "Cliente"].drop_duplicates()
df = df[df["Cliente"].isin(clientes_ativos)].copy()
depois_df = df["Cliente"].nunique()

print(f"Clientes removidos do DF por faturamento zero/ausente na query: {antes_df - depois_df}")
print("Clientes restantes no DF:", depois_df)

print(df[["Cliente", "classificacao", "faturamento"]].head())

# %%
# embaralhar depois de limpar
df = df.sample(frac=1, random_state=42).reset_index(drop=True)

# %%
# sorteio por cliente+classificacao (aleatório, mesma lógica)
base_clientes = (
    df[["Cliente", "classificacao"]]
    .drop_duplicates()
    .sample(frac=1, random_state=42)
    .reset_index(drop=True)
)

grupos = []

for classe, sub in base_clientes.groupby("classificacao"):
    sub = sub.sample(frac=1, random_state=42).reset_index(drop=True)

    n = len(sub)
    tamanho = n // 3
    sobra = n % 3

    gA = sub.iloc[0:tamanho]
    gB = sub.iloc[tamanho:2*tamanho]
    gC = sub.iloc[2*tamanho:3*tamanho]

    if sobra >= 1:
        gA = pd.concat([gA, sub.iloc[3*tamanho:3*tamanho+1]])
    if sobra >= 2:
        gB = pd.concat([gB, sub.iloc[3*tamanho+1:3*tamanho+2]])
    if sobra == 3:
        gC = pd.concat([gC, sub.iloc[3*tamanho+2:3*tamanho+3]])

    gA = gA.assign(grupo="A")
    gB = gB.assign(grupo="B")
    gC = gC.assign(grupo="C")

    grupos.extend([gA, gB, gC])

df_grupos = pd.concat(grupos).reset_index(drop=True)

print("\n=== Qtd de clientes por grupo ===")
print(df_grupos["grupo"].value_counts())

print("\n=== Qtd de clientes por grupo e classificação ===")
print(df_grupos.groupby(["grupo", "classificacao"]).size())

# %%
# >>> AQUI ESTÁ A CORREÇÃO-CHAVE <<<
# se o CSV já tem coluna 'grupo', remove antes do merge pra não virar grupo_x/grupo_y
if "grupo" in df.columns:
    df = df.drop(columns=["grupo"])

resultado = df.merge(
    df_grupos[["Cliente", "classificacao", "grupo"]],
    on=["Cliente", "classificacao"],
    how="left"
)

# voltar ordem original
resultado = resultado.sort_values("ID_ordem").reset_index(drop=True)

print("\nChecagem resultado:")
print(resultado[["Cliente", "classificacao", "grupo", "faturamento"]].head())

print("\nLinhas sem grupo:", resultado["grupo"].isna().sum())


# %% RESUMO: quantidade de clientes e faturamento por classificacao e grupo

# garante 1 cliente por linha no resumo (pra não duplicar se o cliente aparecer em várias linhas)
base_resumo = resultado[["Cliente", "classificacao", "grupo", "faturamento"]].drop_duplicates(
    subset=["Cliente", "classificacao", "grupo"]
)

resumo = (
    base_resumo
    .groupby(["classificacao", "grupo"])
    .agg(
        qtd_clientes=("Cliente", "nunique"),
        faturamento_total=("faturamento", "sum")
    )
    .reset_index()
)

# organizar bonitinho em pivot (lado a lado)
qtd_pivot = resumo.pivot(index="classificacao", columns="grupo", values="qtd_clientes").fillna(0).astype(int)
fat_pivot = resumo.pivot(index="classificacao", columns="grupo", values="faturamento_total").fillna(0)

print("\n================ RESUMO POR CLASSIFICAÇÃO ================")
for classe in qtd_pivot.index:
    print(f"\n{classe}")
    for g in ["A", "B", "C"]:
        qtd = int(qtd_pivot.loc[classe].get(g, 0))
        fat = float(fat_pivot.loc[classe].get(g, 0))
        print(f"  Grupo {g}: {qtd} clientes | faturamento {fat:,.2f}")

print("\n==========================================================")
