import pandas as pd
import numpy as np

df = pd.read_csv("cli.csv")

print(df.columns)

df["grupo_fat"] = pd.qcut(
    df["faturamento"],
    q=4,
    labels=False,
    duplicates="drop"
)

df["grupo_fat"] = "grupo_" + (df["grupo_fat"] + 1).astype(str)

print(df["grupo_fat"].value_counts().sort_index())

np.random.seed(42)

df["grupo_ab"] = (
    df.groupby("grupo_fat", observed=True)
      .transform(lambda x: np.random.permutation(
          ["A"] * (len(x)//2) + ["B"] * (len(x) - len(x)//2)
      ))["faturamento"]
)

print(df["grupo_ab"].value_counts())
print(pd.crosstab(df["grupo_fat"], df["grupo_ab"]))

df["grupo"] = df["grupo_ab"]

# (Opcional) remover colunas auxiliares
df_final = df.drop(columns=["grupo_fat", "grupo_ab"])

# Conferência rápida
print(df_final.head())

# Salvar mantendo a ordem original
df["grupo"] = df["grupo_ab"]

# (Opcional) remover colunas auxiliares
df_final = df.drop(columns=["grupo_fat", "grupo_ab"])


# Salvar mantendo a ordem original
df_final.to_csv("cli_com_grupo_AB.csv", index=False)


