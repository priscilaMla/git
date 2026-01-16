import pandas as pd
import matplotlib.pyplot as plt

gas = pd.read_excel("gasolina.xlsx")

print(gas.columns)

gas_filtrado = gas[gas['PRODUTO'] == 'GASOLINA COMUM']

gasgroup = gas_filtrado.groupby('ANO')['PREÇO MÉDIO REVENDA'].mean().reset_index()

plt.figure(figsize=(10,6))
plt.plot(gasgroup['ANO'], gasgroup['PREÇO MÉDIO REVENDA'], marker = 'o', linestyle= '-', color= 'b')

plt.title('Evolução do Preço Médio de Revenda - Gasolina Comum')
plt.xlabel('Ano')
plt.ylabel('Preço Médio (R$)')
plt.grid(True)

# 5. Exibir
plt.show()

##feriados 
import pandas as pd

lista_feriados = [
    '2019-01-01','2019-03-04','2019-03-05','2019-04-19','2019-04-21','2019-05-01','2019-06-20','2019-09-07','2019-10-12','2019-11-02','2019-11-15','2019-12-25',
'2020-01-01','2020-02-24','2020-02-25','2020-04-10','2020-04-21','2020-05-01','2020-06-11','2020-09-07','2020-10-12','2020-11-02','2020-11-15','2020-12-25',
'2021-01-01','2021-02-15','2021-02-16','2021-04-02','2021-04-21','2021-05-01','2021-06-03','2021-09-07','2021-10-12','2021-11-02','2021-11-15','2021-12-25',
'2022-01-01','2022-02-28','2022-03-01','2022-04-15','2022-04-21','2022-05-01','2022-06-16','2022-09-07','2022-10-12','2022-11-02','2022-11-15','2022-12-25',
'2023-01-01','2023-02-20','2023-02-21','2023-04-07','2023-04-21','2023-05-01','2023-06-08','2023-09-07','2023-10-12','2023-11-02','2023-11-15','2023-12-25',
'2024-01-01','2024-02-12','2024-02-13','2024-03-29','2024-04-21','2024-05-01','2024-05-30','2024-09-07','2024-10-12','2024-11-02','2024-11-15','2024-11-20','2024-12-25',
'2025-01-01','2025-03-03','2025-03-04','2025-04-08','2025-04-21','2025-05-01','2025-09-07','2025-10-12','2025-11-02','2025-11-15','2025-11-20','2025-12-25' 
]

ferias_intervalos = [
    ('2019-01-03', '2019-02-11'),
    ('2019-06-09', '2019-06-28'),
    ('2019-12-21', '2020-02-10'),
    ('2020-06-06', '2020-06-22'),
    ('2020-12-16', '2021-03-08'),
    ('2021-06-19', '2021-07-01'),
    ('2021-12-22', '2022-02-09'),
    ('2022-06-11', '2022-06-30'),  
    ('2022-12-23', '2023-02-10'),
    ('2023-07-12', '2023-07-27'),
    ('2023-12-22', '2024-02-18'),
    ('2024-07-11', '2024-07-28'),
    ('2024-12-20', '2025-02-10'),
    ('2025-07-09', '2025-07-27'),
    ('2025-12-19', '2025-12-31')
]

# 2. Gerar o intervalo de datas
datas = pd.date_range(start='2019-01-01', end='2025-12-31')
df_calendario = pd.DataFrame({'DATA': datas})

# 3. Converter lista de feriados
feriados_dt = pd.to_datetime(lista_feriados)

# 4. Inicializar coluna
df_calendario['TIPO_DIA'] = 'DIA COMUM'


# 5. Marcar FERIADO
df_calendario.loc[
    df_calendario['DATA'].isin(feriados_dt),'TIPO_DIA'] = 'FERIADO'

# 6. PRÉ FERIADO (dia anterior)
pre_feriados = feriados_dt - pd.Timedelta(days=1)

df_calendario.loc[
    (df_calendario['DATA'].isin(pre_feriados)) & (df_calendario['TIPO_DIA'] == 'DIA COMUM'),
    'TIPO_DIA'
] = 'PRE FERIADO'

# 7. PÓS FERIADO (dia seguinte)
pos_feriados = feriados_dt + pd.Timedelta(days=1)

df_calendario.loc[
    (df_calendario['DATA'].isin(pos_feriados)) & (df_calendario['TIPO_DIA'] == 'DIA COMUM'),
    'TIPO_DIA'
] = 'POS FERIADO'

ferias_intervalos_dt = [
    (pd.to_datetime(inicio), pd.to_datetime(fim))
    for inicio, fim in ferias_intervalos
]

# Marcar FERIAS somente onde ainda é DIA COMUM
for inicio, fim in ferias_intervalos_dt:
    df_calendario.loc[
        (df_calendario['DATA'] >= inicio) &
        (df_calendario['DATA'] <= fim) &
        (df_calendario['TIPO_DIA'] == 'DIA COMUM'),
        'TIPO_DIA'
    ] = 'FERIAS'

df_calendario['DIA_SEMANA'] = df_calendario['DATA'].dt.day_name(locale='pt_BR')

# Visualizar
print(df_calendario.head(50))

df_calendario['DATA'] = df_calendario['DATA'].dt.date

#df_calendario.to_excel("calendario.xlsx", index=False)