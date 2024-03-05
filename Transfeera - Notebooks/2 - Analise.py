# Databricks notebook source
# MAGIC %md
# MAGIC # Preparando dataframes
# MAGIC Deixando alguns filtros para simplificar visualização

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import Row
import pandas as pd
import matplotlib.pyplot as plt
from datetime import timedelta
import numpy as np
import calendar
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

# filtros
id_cliente_filtro = 1
ano_filtro = 2023
mes_filtro = 12

fact_transacoes_df = (
    spark.read.format("delta")
    .table("dw.fact_transacoes")
    .orderBy("data_movimentacao", "id_cliente")
)

dim_clientes_df = spark.read.format("delta").table("dw.dim_clientes")

dim_contratos_df = spark.read.format("delta").table("dw.dim_contratos")

fact_transacoes_dia_df = (
    spark.read.format("delta")
    .table("dw.fact_transacoes_dia")
    .orderBy("data_movimentacao", "id_cliente")
)

dim_tempo_df = spark.read.format("delta").table("dw.dim_tempo").orderBy("data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataframe com todos os dados de um cliente.
# MAGIC Incluil labels com datas de todos os dias

# COMMAND ----------

faturamento = (
    fact_transacoes_dia_df.join(
        dim_tempo_df,
        fact_transacoes_dia_df["data_movimentacao"] == dim_tempo_df["data"],
    )
    .filter(f"id_cliente = {id_cliente_filtro}")
    .orderBy("dim_tempo.data", "id_cliente")
)


data_labels = [data["data"] for data in dim_tempo_df.collect()]

# COMMAND ----------

# MAGIC %md
# MAGIC #Analise de volume
# MAGIC Inclui dados de mes e ano

# COMMAND ----------

cliente_df = faturamento

volume_values = cliente_df.select("quantidade_movimentacoes").collect()
volume_values = [v[0] if v[0] is not None else 0 for v in volume_values]

plt.figure(figsize=(20, 8))

plt.bar(range(len(volume_values)), volume_values, label="Volume")

plt.xticks(range(len(data_labels)), data_labels, rotation=90)

plt.tight_layout()
plt.legend()

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quebra do valor diário por tipo de transação

# COMMAND ----------

transacoes_do_mes = faturamento.select("total_boleto", "total_payout", "total_payin").collect()

volume_boleto = [v["total_boleto"] for v in transacoes_do_mes]
volume_payout = [v["total_payout"] for v in transacoes_do_mes]
volume_payin = [v["total_payin"] for v in transacoes_do_mes]

bar_width = 0.5
index = range(len(volume_boleto))

plt.figure(figsize=(20, 6))
plt.bar(index, volume_boleto, bar_width, label="Boleto")
plt.bar(index, volume_payout, bar_width, bottom=volume_boleto, label="Payout")
plt.bar(
    index,
    volume_payin,
    bar_width,
    bottom=[volume_boleto[i] + volume_payout[i] for i in range(len(volume_boleto))],
    label="Payin",
)
plt.legend()
plt.xticks(range(0, len(data_labels), 5), data_labels[::5], rotation=90)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Identificação dos Dias de Utilização do Produto

# COMMAND ----------

transacoes_por_dia_df = (
    fact_transacoes_dia_df.join(
        dim_tempo_df,
        fact_transacoes_dia_df["data_movimentacao"] == dim_tempo_df["data"],
    )
    .select(
        "id_cliente",
        "dim_tempo.dia_do_ano",
        "dim_tempo.data",
        "total_movimentacao",
        "quantidade_movimentacoes",
        "dim_tempo.dia",
        "dim_tempo.mes",
        "dim_tempo.ano",
    )
    .filter(f"dim_tempo.ano = {ano_filtro}")
    .orderBy("id_cliente", "dim_tempo.data")
)


transacoes_por_dia_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualização de usos
# MAGIC Dados dos últimos 3 meses. Todos os dias sem uso são considerados como 0.

# COMMAND ----------

plt.figure(figsize=(12, 8))

data_inicial = date.today() - relativedelta(months=3)
data_final = date.today()


transacoes = (
    transacoes_por_dia_df.orderBy("data")
    .filter((col("id_cliente") == id_cliente_filtro) & (col("data") >= data_inicial))
    .collect()
)


dias_validos = (data_final - data_inicial).days
labels_validas = [
    (data_inicial + timedelta(days=i)) for i in range((data_final - data_inicial).days)
]

transacoes_por_dia_values = np.zeros(dias_validos)
movimentado_por_dia_values = np.zeros(dias_validos)

for t in transacoes:
    transacoes_por_dia_values[(t["data"] - data_inicial).days] = t[
        "quantidade_movimentacoes"
    ]
    movimentado_por_dia_values[(t["data"] - data_inicial).days] = t[
        "total_movimentacao"
    ]

fig, ax1 = plt.subplots(figsize=(20, 8))

ax1.bar(range(dias_validos), transacoes_por_dia_values, color="blue")
ax1.set_ylabel("Uso por dia ", color="blue")

ax1.set_xticks(range(dias_validos))
ax1.set_xticklabels(labels_validas, rotation=90)

ax2 = ax1.twinx()

ax2.plot(movimentado_por_dia_values, color="red")
ax2.set_ylabel("Valor movido", color="red")

plt.title("Uso por dia e valor movido")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualização de usos (1 mês)
# MAGIC Similar ao anterior mas apenas para 1 mês

# COMMAND ----------

transacoes = (
    transacoes_por_dia_df.select(
        "total_movimentacao", "quantidade_movimentacoes", "data", "dia"
    )
    .filter(
        f"dim_tempo.ano = {ano_filtro} and dim_tempo.mes = {mes_filtro} and id_cliente = {id_cliente_filtro}"
    )
    .collect()
)

datas = (
    dim_tempo_df.select("data")
    .filter(f"dim_tempo.ano = {ano_filtro} and dim_tempo.mes = {mes_filtro}")
    .orderBy("data")
    .collect()
)

data_labels = [data["data"] for data in datas]

transacoes_por_dia_values = np.zeros(len(datas))
movimentado_por_dia_values = np.zeros(len(datas))


for t in transacoes:
    transacoes_por_dia_values[t["dia"] - 1] = t["quantidade_movimentacoes"]
    movimentado_por_dia_values[t["dia"] - 1] = t["total_movimentacao"]


fig, ax1 = plt.subplots(figsize=(20, 8))

ax1.bar(range(len(data_labels)), transacoes_por_dia_values, color="blue")
ax1.set_ylabel("Uso por dia ", color="blue")

ax1.set_xticks(range(len(data_labels)))
ax1.set_xticklabels(data_labels, rotation=90)

ax2 = ax1.twinx()

ax2.plot(movimentado_por_dia_values, color="red")
ax2.set_ylabel("Valor movido", color="red")

plt.title("Uso por dia e valor movido (2023/12)")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Faturamento Diário dos Clientes

# COMMAND ----------

transacoes_df = (
    faturamento.join(
        dim_contratos_df, dim_contratos_df["id_contrato"] == faturamento["id_contrato"]
    )
    .select(
        "taxa_boleto",
        "taxa_payout",
        "taxa_payin",
        "total_boleto",
        "total_payin",
        "total_payout",
        "data",
        "dia",
    )
    .filter(f"fact_transacoes_dia.id_cliente = {id_cliente_filtro}")
    .orderBy("data")
)
transacoes = transacoes_df.collect()


data_labels = [data["data"] for data in transacoes]

faturamento_boleto = np.zeros(len(data_labels))
faturamento_payin = np.zeros(len(data_labels))
faturamento_payout = np.zeros(len(data_labels))
faturamento_total = np.zeros(len(data_labels))

for idx, t in enumerate(transacoes):
    faturamento_boleto[idx] = t["taxa_boleto"] * t["total_boleto"]
    faturamento_payin[idx] = t["taxa_payin"] * t["total_payin"]
    faturamento_payout[idx] = t["taxa_payout"] * t["total_payout"]
    faturamento_total[idx] = (
        faturamento_payout[idx] + faturamento_payin[idx] + faturamento_boleto[idx]
    )

categories = [
    "Faturamento Boleto",
    "Faturamento Payin",
    "Faturamento Payout",
    "Faturamento Total",
]

x = np.arange(len(data_labels))
bar_width = 0.2

plt.figure(figsize=(20, 6))
plt.plot(faturamento_boleto, label="Faturamento Boleto", color="red")
plt.plot(faturamento_payin, label="Faturamento Payin", color="green")
plt.plot(faturamento_payout, label="Faturamento Payout", color="blue")


plt.xlabel("Data")
plt.ylabel("Faturamento")
plt.title("Faturamento por Dia")
plt.xticks(range(0, len(data_labels), 5), data_labels[::5], rotation=90)
plt.legend()

plt.tight_layout()
plt.show()

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from datetime import date, timedelta
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import unix_timestamp

def plot_linha_tendencia(valores, labels, titulo, targetVariable = "valor", dias= None):
    treinamento_df = spark.createDataFrame(valores)

    treinamento_df = treinamento_df.withColumn("data_movimentacao", unix_timestamp(treinamento_df["data_movimentacao"]))

    assembler = VectorAssembler(inputCols=["data_movimentacao"], outputCol="features")
    treinamento_features = assembler.transform(treinamento_df)

    lr = LinearRegression(featuresCol="features", labelCol=targetVariable)
    lr_model = lr.fit(treinamento_features)

    predictions = lr_model.transform(treinamento_features)

    predictions.select("data_movimentacao", "prediction").show()
    
    predictions_df = predictions.select("data_movimentacao", "prediction").toPandas()

    predictions_df["data_movimentacao"] = pd.to_datetime(
        predictions_df["data_movimentacao"], unit="s"
    )
    plt.figure(figsize=(20, 6))
    plt.plot(
        predictions_df["data_movimentacao"],
        predictions_df["prediction"],
        label="Predictions",
        color="red",
    )
    plt.scatter(
        labels,
        valores[targetVariable].tolist(),
        color="blue",
        label="Valores",
    )

    if dias:
        ultimo_dia = max(predictions_df["data_movimentacao"])
        print(ultimo_dia)
        future_dates = [ultimo_dia + timedelta(days=d) for d in range(dias)]
        
        future_df = spark.createDataFrame(pd.DataFrame({"data_movimentacao": future_dates}))
        future_df = future_df.withColumn("data_movimentacao", unix_timestamp(future_df["data_movimentacao"]))
        future_features = assembler.transform(future_df)
        future_predictions = lr_model.transform(future_features).toPandas()

        future_predictions["data_movimentacao"] = pd.to_datetime(
            future_predictions["data_movimentacao"], unit="s"
        )
        plt.plot(
            future_predictions["data_movimentacao"],
            future_predictions["prediction"],
            label="Future Predictions",
            color="green",
            linestyle="--",
        )
    plt.xlabel("Data")
    plt.ylabel("Total Movimentacao")
    plt.title(titulo)
    plt.legend()
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.show()

    return predictions_df

# COMMAND ----------

rl_boleto = plot_linha_tendencia(pd.DataFrame({"data_movimentacao":data_labels, "valor":faturamento_boleto}), data_labels, "Regressão linear de faturamento por boleto")

rl_payin = plot_linha_tendencia(pd.DataFrame({"data_movimentacao":data_labels, "valor":faturamento_payin}), data_labels, "Regressão linear de faturamento por payin")

rl_payout = plot_linha_tendencia(pd.DataFrame({"data_movimentacao":data_labels, "valor":faturamento_payout}), data_labels, "Regressão linear de faturamento por payout")

rl_total = plot_linha_tendencia(pd.DataFrame({"data_movimentacao":data_labels, "valor":faturamento_total}), data_labels, "Regressão linear de faturamento total")

# COMMAND ----------

# MAGIC %md
# MAGIC # Explorando modelos
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparando dados
# MAGIC Para fins de comparação, todos os modelos devem usar os mesmos dados nos mesmos intervalos de tempo

# COMMAND ----------

from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.arima_process import ArmaProcess
import statsmodels.api as sm
import pandas as pd


valores_df = fact_transacoes_dia_df.orderBy("data_movimentacao").filter(f"id_cliente = {id_cliente_filtro}")

valores = valores_df.collect()

treinamento = valores[:len(valores) // 2+ 1]
testes = valores[len(valores) // 2:]

testes_valores = [v["total_movimentacao"] for v in testes]
treinamento_valores = [v["total_movimentacao"] for v in treinamento]
testes_datas = [v["data_movimentacao"] for v in testes]

# COMMAND ----------

# MAGIC %md
# MAGIC ## ARMA

# COMMAND ----------

from statsmodels.tsa.arima.model import ARIMA

ARMAmodel = ARIMA(treinamento_valores, order=(0, 0, 2))
ARMAmodel = ARMAmodel.fit()

arma_prev = ARMAmodel.get_forecast(len(testes))
arma_forecast_index = pd.to_datetime(testes_datas)
arma_prev_df = arma_prev.conf_int(alpha=0.05)
arma_prev_df = pd.DataFrame(arma_prev_df, columns=["lower", "upper"])

arma_prev_df["previsao"] = ARMAmodel.predict(
    start=arma_prev_df.index[0], end=arma_prev_df.index[-1]
)
arma_prev_df.index = testes
arma_prev_res = arma_prev_df["previsao"]


residuals = pd.DataFrame(ARMAmodel.resid)

print(residuals.describe())

yhat = arma_prev.predicted_mean
yhat_conf_int = arma_prev.conf_int(alpha=0.05)

plt.figure(figsize=(20, 6))
plt.xlabel("Data")
plt.plot(testes_datas, arma_prev_res, color="green", label="ARMA")
plt.fill_between(
    testes_datas,
    [v[0] for v in yhat_conf_int],
    [v[1] for v in yhat_conf_int],
    color="gray",
    alpha=0.3,
    label="Intervalo de confiança",
)
plt.plot(
    [v["data_movimentacao"] for v in treinamento],
    treinamento_valores,
    color="blue",
    label="Treinamento",
)
plt.plot(testes_datas, yhat, color="yellow", alpha=0.3, label="Media")

plt.plot(
    testes_datas, [v["total_movimentacao"] for v in testes], color="red", label="Testes"
)
plt.legend()
plt.grid(True)
plt.xticks(rotation=45)
residuals.plot(kind="kde", figsize=(20, 6))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ARIMA

# COMMAND ----------

ARIMAmodel = ARIMA(treinamento_valores, order=(0,1, 1))

ARIMAmodel = ARIMAmodel.fit()

arima_prev = ARIMAmodel.get_forecast(len(testes))
arima_prev_df = arima_prev.conf_int(alpha=0.05)
arima_prev_df = pd.DataFrame(arima_prev_df, columns=["lower", "upper"])

arima_prev_df["previsao"] = ARIMAmodel.predict(
    start=arima_prev_df.index[0], end=arima_prev_df.index[-1]
)
arima_prev_df.index = testes
arima_prev_res = arima_prev_df["previsao"]

yhat = arima_prev.predicted_mean
yhat_conf_int = arima_prev.conf_int(alpha=0.05)

plt.figure(figsize=(20, 6))
plt.xlabel("Data")
plt.plot(testes_datas, arima_prev_res, color="green", label="ARIMA")
plt.fill_between(
    testes_datas,
    [v[0] for v in yhat_conf_int],
    [v[1] for v in yhat_conf_int],
    color="gray",
    alpha=0.3,
    label="Intervalo de confiança",
)
plt.plot(testes_datas, yhat, color="yellow", alpha=0.3, label="Media")
plt.plot(
    [v["data_movimentacao"] for v in treinamento],
    treinamento_valores,
    color="blue",
    label="Treinamento",
)
plt.plot(
    testes_datas,
    [v["total_movimentacao"] for v in testes],
    color="red",
    label="Testes",
)
plt.legend()
plt.grid(True)
plt.xticks(rotation=45)

residuals = pd.DataFrame(ARIMAmodel.resid)
residuals.plot(kind="kde", figsize=(20, 6))

print(residuals.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## SARIMAX

# COMMAND ----------

SARIMAXmodel = SARIMAX(treinamento_valores, order = (2,1, 1), seasonal_order=(1,1,1,12))
SARIMAXmodel = SARIMAXmodel.fit()

sarima_prev = SARIMAXmodel.get_forecast(len(testes))
sarima_prev_df = sarima_prev.conf_int(alpha = 0.05) 
sarima_prev_df = pd.DataFrame(sarima_prev_df, columns=['lower', 'upper'])
sarima_prev_df["Predictions"] = SARIMAXmodel.predict(
    start = sarima_prev_df.index[0], 
    end = sarima_prev_df.index[-1]
)
sarima_prev_df.index = testes
sarima_prev_df_res = sarima_prev_df["Predictions"] 


yhat = sarima_prev.predicted_mean
yhat_conf_int = sarima_prev.conf_int(alpha=0.05)

plt.figure(figsize=(20, 6))
plt.xlabel('Data')
plt.fill_between(testes_datas, [v[0] for v in yhat_conf_int],  [v[1] for v in yhat_conf_int], color='gray', alpha=0.3, label='Intervalo de confiança')
plt.plot(testes_datas, yhat, color='yellow', alpha=0.3, label='Media')
plt.plot([v["data_movimentacao"] for v in treinamento], treinamento_valores, color = "blue", label="Treinamento")
plt.plot(testes_datas, sarima_prev_df_res, color='orange', label = 'SARIMAX')
plt.plot(testes_datas, [v["total_movimentacao"] for v in testes], color = "red", label="Testes")
plt.legend()
plt.xticks(rotation=45)


residuals = pd.DataFrame(SARIMAXmodel.resid)
residuals.plot(kind='kde', figsize=(20, 6))

print(residuals.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC # Novas análises
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Segmentos de clientes

# COMMAND ----------

transacao_cliente = fact_transacoes_dia_df.join(
    dim_clientes_df,
    dim_clientes_df["id_cliente"] == fact_transacoes_dia_df["id_cliente"],
).join(
    dim_contratos_df,
    dim_contratos_df["id_contrato"] == fact_transacoes_dia_df["id_contrato"]
)

clientes_por_segmento = (
    dim_clientes_df.groupBy("segmento")
    .agg(count("id_cliente").alias("Clientes"))
    .collect()
)

segmentos = [v["segmento"] for v in clientes_por_segmento]

plt.figure(figsize=(20, 6))
plt.bar(range(len(segmentos)),[v["Clientes"] for v in clientes_por_segmento], label="Segmentos")

plt.xticks(range(len(segmentos)), segmentos)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transacoes por segmento
# MAGIC
# MAGIC

# COMMAND ----------

transacoes_segmento = (
    transacao_cliente.groupBy("segmento", "data_movimentacao")
    .agg(
        sum("quantidade_movimentacoes").alias("quantidade_movimentacoes"),
        sum("total_movimentacao").alias("total_movimentacao")
    )
    .orderBy("segmento", "data_movimentacao")
)


data_inicio = transacoes_segmento.first()["data_movimentacao"]
data_fim = transacoes_segmento.orderBy(desc("data_movimentacao")).first()[
    "data_movimentacao"
]

dias = [
    v[0]
    for v in transacoes_segmento.select("data_movimentacao")
    .distinct()
    .orderBy("data_movimentacao")
    .collect()
]

plt.figure(figsize=(20, 6))

trends = []
for segmento in segmentos:
    dados = (
        transacoes_segmento.filter(f"segmento = '{segmento}'")
        .orderBy("data_movimentacao")
        .collect()
    )
    valores = []
    valores = np.zeros((data_fim - data_inicio).days + 1)

    for v in dados:
        valores[(v["data_movimentacao"] - data_inicio).days] = v[
            "quantidade_movimentacoes"
        ]
    plt.plot(dias, valores, label=segmento)
    trends.append((valores, segmento))
plt.legend()

for t in trends:
    plot_linha_tendencia(pd.DataFrame({"data_movimentacao":dias, "valor":t[0]}), dias, t[1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Valor movido por segmento
# MAGIC
# MAGIC

# COMMAND ----------

plt.figure(figsize=(20, 6))

t = []
for segmento in segmentos:
    dados = (
        transacoes_segmento.filter(f"segmento = '{segmento}'")
        .orderBy("data_movimentacao")
        .collect()
    )
    valores = []
    valores = np.zeros((data_fim - data_inicio).days + 1)

    for v in dados:
        valores[(v["data_movimentacao"] - data_inicio).days] = v[
            "total_movimentacao"
        ]
    plt.plot(dias, valores, label=segmento)
    trends.append((valores, segmento))

plt.legend()


for t in trends:
    plot_linha_tendencia(pd.DataFrame({"data_movimentacao":dias, "valor":t[0]}), dias, t[1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Faturamento por segmento
# MAGIC
# MAGIC

# COMMAND ----------

plt.figure(figsize=(20, 6))


trends=[]
for segmento in segmentos:
    dados = (
        transacao_cliente.filter(f"segmento = '{segmento}'")
        .orderBy("data_movimentacao")
        .collect()
    )
    
    valores = np.zeros((data_fim - data_inicio).days + 1)

    for v in dados:
        valores[(v["data_movimentacao"] - data_inicio).days] += v["total_boleto"] * v["taxa_boleto"]
        valores[(v["data_movimentacao"] - data_inicio).days] += v["total_payin"] * v["taxa_payin"]
        valores[(v["data_movimentacao"] - data_inicio).days] += v["total_payout"] * v["taxa_payout"]
        
    plt.plot(dias, valores, label=segmento)  
    trends.append((valores, segmento))
plt.legend()


for t in trends:
    plot_linha_tendencia(pd.DataFrame({"data_movimentacao":dias, "valor":t[0]}), dias, t[1])
