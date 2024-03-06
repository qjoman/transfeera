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
# MAGIC ## Dataframe com todos os dados de cliente.
# MAGIC Incluil labels com datas de todos os dias

# COMMAND ----------

faturamento = (
    fact_transacoes_dia_df.join(
        dim_tempo_df,
        fact_transacoes_dia_df["data_movimentacao"] == dim_tempo_df["data"],
    )
    .orderBy("dim_tempo.data", "id_cliente")
)


data_labels = [data["data"] for data in dim_tempo_df.collect()]
dias_da_semana = [data["dia_da_semana"] for data in dim_tempo_df.collect()]
fim_de_semana = [index for index, value in enumerate(dias_da_semana) if value == 6 or value == 7]
inicio_fim_da_semana = [index for index, value in enumerate(dias_da_semana) if value == 1 or value == 7]
inicio_semana = [index for index, value in enumerate(dias_da_semana) if value == 1]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers de datas

# COMMAND ----------


data_labels = [data["data"] for data in dim_tempo_df.collect()]
dias_da_semana = [data["dia_da_semana"] for data in dim_tempo_df.collect()]
fim_de_semana = [index for index, value in enumerate(dias_da_semana) if value == 5 or value == 6]
inicio_fim_da_semana = [index for index, value in enumerate(dias_da_semana) if value == 0 or value == 6]
inicio_semana = [index for index, value in enumerate(dias_da_semana) if value == 0]

def destacar_semana(data_labels = data_labels):

  inicios = [index for index, value in enumerate(data_labels) if value.weekday() == 6]
  for i in inicios:
      start_index = i
      plt.axvline(x=data_labels[i], color= "gray", alpha=0.1)
  

# COMMAND ----------

# MAGIC %md
# MAGIC #Analise de volume

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quebra do valor diário por tipo de transação
# MAGIC Contém média móvel dos últimos 7 dias.

# COMMAND ----------

def sma(data, window_size):
    sma_values = []
    for i in range(len(data) - window_size + 1):
        window = data[i:i+window_size]
        sma = np.mean(window)
        sma_values.append(sma)
    return sma_values

# COMMAND ----------

transacoes_do_mes = (
    faturamento.select(
        "id_cliente", "data", "quantidade_boleto", "quantidade_payout", "quantidade_payin"
    )
    .groupBy("data", "id_cliente")
    .agg(
        sum("quantidade_boleto").alias("quantidade_boleto"),
        sum("quantidade_payout").alias("quantidade_payout"),
        sum("quantidade_payin").alias("quantidade_payin"),
    )
)

transacoes_do_mes.display()

transacoes_do_mes = transacoes_do_mes.filter(f"id_cliente = {id_cliente_filtro}").collect()

volume_boleto = [v["quantidade_boleto"] for v in transacoes_do_mes]
volume_payout = [v["quantidade_payout"] for v in transacoes_do_mes]
volume_payin = [v["quantidade_payin"] for v in transacoes_do_mes]

volume_boleto_series = pd.Series(volume_boleto)
volume_payout_series = pd.Series(volume_payout)
volume_payin_series = pd.Series(volume_payin)

index = range(len(volume_boleto))

plt.figure(figsize=(20, 6))
plt.bar(data_labels, volume_boleto, label="Boleto")
plt.plot(
    data_labels,
    volume_boleto_series.rolling(window=5, min_periods=1).mean(),
    label="Média móvel",
    color="red",
)
destacar_semana()
plt.legend()
plt.tight_layout()
plt.show()


plt.figure(figsize=(20, 6))
plt.bar(data_labels, volume_payout, label="Payout")
plt.plot(
    data_labels,
    volume_payout_series.rolling(window=5, min_periods=1).mean(),
    label="Média móvel",
    color="red",
)
destacar_semana()
plt.legend()
plt.tight_layout()
plt.show()


plt.figure(figsize=(20, 6))
plt.bar(data_labels, volume_payin, label="Payin")
plt.plot(
    data_labels,
    volume_payin_series.rolling(window=5, min_periods=1).mean(),
    label="Média móvel",
    color="red",
)
destacar_semana()
plt.legend()

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Identificação dos Dias de Utilização do Produto

# COMMAND ----------

transacoes_por_dia_df = (
    fact_transacoes_df.join(
        dim_tempo_df,
        fact_transacoes_df["data_movimentacao"] == dim_tempo_df["data"],
    )
    .select(
        "id_cliente",
        "dim_tempo.dia_do_ano",
        "valor_transacao",
        "dim_tempo.data",
        "dim_tempo.dia",
        "dim_tempo.mes",
        "dim_tempo.ano",
        "tipo_transacao"
    )
    .filter(f"dim_tempo.ano = {ano_filtro}")
    .orderBy("id_cliente", "data")
    .groupBy("id_cliente", "data", "dia", "mes", "ano", "tipo_transacao")
    .agg(
        sum("valor_transacao").alias("total_movimentacao"),
        count("id_cliente").alias("quantidade_movimentacoes"),
    )
)


transacoes_por_dia_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualização de usos
# MAGIC Dados dos últimos 3 meses. Todos os dias sem uso são considerados como 0.

# COMMAND ----------

plt.figure(figsize=(12, 8))

data_inicial = date.today() - relativedelta(months=3)
data_final = date.today() - relativedelta(months=1)


transacoes = transacoes_por_dia_df.orderBy("data").filter(
    (col("data") >= data_inicial) & (col("data") <= data_final)
)

transacoes.display()


transacoes = transacoes.filter(f"id_cliente = {id_cliente_filtro}").collect()


dias_validos = (data_final - data_inicial).days
labels_validas = [
    (data_inicial + timedelta(days=i)) for i in range((data_final - data_inicial).days)
]

transacoes_por_dia_values = np.zeros(dias_validos)
movimentado_por_dia_values = np.zeros(dias_validos)

boletos_por_dia_values = np.zeros(dias_validos)
payins_por_dia_values = np.zeros(dias_validos)
payouts_por_dia_values = np.zeros(dias_validos)

for t in transacoes:
    transacoes_por_dia_values[(t["data"] - data_inicial).days] += t[
        "quantidade_movimentacoes"
    ]
    movimentado_por_dia_values[(t["data"] - data_inicial).days] += t[
        "total_movimentacao"
    ]

    if t["tipo_transacao"] == "boleto":
        boletos_por_dia_values[(t["data"] - data_inicial).days] += t[
            "quantidade_movimentacoes"
        ]
    elif t["tipo_transacao"] == "payin":
        payins_por_dia_values[(t["data"] - data_inicial).days] += t[
            "quantidade_movimentacoes"
        ]
    elif t["tipo_transacao"] == "payout":
        payouts_por_dia_values[(t["data"] - data_inicial).days] += t[
            "quantidade_movimentacoes"
        ]

fig, ax1 = plt.subplots(figsize=(20, 8))
destacar_semana(labels_validas)

ax1.bar(labels_validas, boletos_por_dia_values, label="Boletos")
ax1.bar(
    labels_validas, payins_por_dia_values, bottom=boletos_por_dia_values, label="Payins"
)
ax1.bar(
    labels_validas,
    payouts_por_dia_values,
    bottom=boletos_por_dia_values + payins_por_dia_values,
    label="Payouts",
)

ax1.legend()
ax1.set_ylabel("Uso por dia ")


ax2 = ax1.twinx()

ax2.plot(labels_validas, movimentado_por_dia_values, color="red")
ax2.set_ylabel("Valor movido", color="red")

plt.title("Uso por dia e valor movido (2 meses)")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Faturamento Diário dos Clientes

# COMMAND ----------

transacoes_df = (
    (
        fact_transacoes_dia_df.join(
            dim_contratos_df,
            dim_contratos_df["id_contrato"] == fact_transacoes_dia_df["id_contrato"],
        )
        .join(
            dim_tempo_df,
            fact_transacoes_dia_df["data_movimentacao"] == dim_tempo_df["data"],
        )
        .withColumn("faturamento_boleto", col("taxa_boleto") * col("total_boleto"))
        .withColumn("faturamento_payin", col("taxa_payin") * col("total_payin"))
        .withColumn("faturamento_payout", col("taxa_payout") * col("total_payout"))
    )
    .groupBy("data", "fact_transacoes_dia.id_cliente")
    .agg(
        sum("faturamento_boleto").alias("faturamento_boleto"),
        sum("faturamento_payin").alias("faturamento_payin"),
        sum("faturamento_payout").alias("faturamento_payout"),
        sum("total_boleto").alias("total_boleto"),
        sum("total_payin").alias("total_payin"),
        sum("total_payout").alias("total_payout"),
    )
    .orderBy("data")
)

transacoes_df.display()

transacoes = (
    transacoes_df.groupBy("data")
    .agg(
        sum("faturamento_boleto").alias("faturamento_boleto"),
        sum("faturamento_payin").alias("faturamento_payin"),
        sum("faturamento_payout").alias("faturamento_payout"),
        sum("total_boleto").alias("total_boleto"),
        sum("total_payin").alias("total_payin"),
        sum("total_payout").alias("total_payout"),
    )
    .orderBy("data")
    .collect()
)


data_labels = [data["data"] for data in transacoes]

boleto = np.zeros(len(data_labels))
payin = np.zeros(len(data_labels))
payout = np.zeros(len(data_labels))
total = np.zeros(len(data_labels))

faturamento_boleto = np.zeros(len(data_labels))
faturamento_payin = np.zeros(len(data_labels))
faturamento_payout = np.zeros(len(data_labels))
faturamento_total = np.zeros(len(data_labels))

for idx, t in enumerate(transacoes):
    boleto[idx] = t["total_boleto"]
    payin[idx] = t["total_payin"]
    payout[idx] = t["total_payout"]
    total[idx] = t["total_boleto"] + t["total_payin"] + t["total_payout"]

    faturamento_boleto[idx] = t["faturamento_boleto"]
    faturamento_payin[idx] = t["faturamento_payin"]
    faturamento_payout[idx] = t["faturamento_payout"]
    faturamento_total[idx] = (
        t["faturamento_boleto"] + t["faturamento_payin"] + t["faturamento_payout"]
    )

categories = [
    "Faturamento Boleto",
    "Faturamento Payin",
    "Faturamento Payout",
    "Faturamento Total",
]

x = np.arange(len(data_labels))

plt.figure(figsize=(20, 6))
plt.plot(data_labels, boleto, label="Valor Boleto", color="red")
plt.plot(data_labels, payin, label="Valor Payin", color="green")
plt.plot(data_labels, payout, label="Valor Payout", color="blue")


plt.xlabel("Data")
plt.ylabel("Valor")
plt.title("Valores total por Dia")
plt.xticks(rotation=90)
plt.legend()
destacar_semana(data_labels)

plt.tight_layout()
plt.show()

plt.figure(figsize=(20, 6))
plt.plot(data_labels, faturamento_boleto, label="Faturamento Boleto", color="red")
plt.plot(data_labels, faturamento_payin, label="Faturamento Payin", color="green")
plt.plot(data_labels, faturamento_payout, label="Faturamento Payout", color="blue")


plt.xlabel("Data")
plt.ylabel("Faturamento")
plt.title("Faturamento por Dia")
plt.xticks(rotation=90)
plt.legend()
destacar_semana(data_labels)

plt.tight_layout()
plt.show()

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
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
    
    evaluator = RegressionEvaluator(labelCol='valor', predictionCol='prediction', metricName='rmse')
    rmse = evaluator.evaluate(predictions)
    print(f'Root Mean Squared Error (RMSE): {rmse}')

    # print(f'Intercept: {lr_model.intercept}')
    # print(f'Coefficient: {lr_model.coefficients[0]}')

    plt.figure(figsize=(20, 6))
    plt.plot(
        predictions_df["data_movimentacao"],
        predictions_df["prediction"],
        label="Previsto",
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
    plt.ylabel("Valor")
    plt.title(titulo)
    plt.legend()
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.show()

    return predictions_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tendencia de valor de transações

# COMMAND ----------

rl_boleto = plot_linha_tendencia(pd.DataFrame({"data_movimentacao":data_labels, "valor":boleto}), data_labels, "Regressão linear por boleto")
rl_boleto.display()

rl_payin = plot_linha_tendencia(pd.DataFrame({"data_movimentacao":data_labels, "valor":payin}), data_labels, "Regressão linear por payin")
rl_payin.display()

rl_payout = plot_linha_tendencia(pd.DataFrame({"data_movimentacao":data_labels, "valor":payout}), data_labels, "Regressão linear por payout")
rl_payout.display()

rl_total = plot_linha_tendencia(pd.DataFrame({"data_movimentacao":data_labels, "valor":total}), data_labels, "Regressão linear total")
rl_total.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tendência de faturamento

# COMMAND ----------

rl_boleto_faturamento = plot_linha_tendencia(pd.DataFrame({"data_movimentacao":data_labels, "valor":faturamento_boleto}), data_labels, "Regressão linear de faturamento por boleto")
rl_boleto_faturamento.display() 

rl_payin_faturamento = plot_linha_tendencia(pd.DataFrame({"data_movimentacao":data_labels, "valor":faturamento_payin}), data_labels, "Regressão linear de faturamento por payin")
rl_payin_faturamento.display() 

rl_payout_faturamento = plot_linha_tendencia(pd.DataFrame({"data_movimentacao":data_labels, "valor":faturamento_payout}), data_labels, "Regressão linear de faturamento por payout")
rl_payout_faturamento.display() 

rl_total_faturamento = plot_linha_tendencia(pd.DataFrame({"data_movimentacao":data_labels, "valor":faturamento_total}), data_labels, "Regressão linear de faturamento total")
rl_total_faturamento.display() 

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
        sum("total_movimentacao").alias("total_movimentacao"),
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

destacar_semana(data_labels)
plt.legend()

for t in trends:
    plot_linha_tendencia(
        pd.DataFrame({"data_movimentacao": dias, "valor": t[0]}), dias, t[1]
    ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Valor movido por segmento
# MAGIC
# MAGIC

# COMMAND ----------

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
            "total_movimentacao"
        ]
    plt.plot(dias, valores, label=segmento)
    trends.append((valores, segmento))

plt.legend()
destacar_semana(data_labels)


for t in trends:
    plot_linha_tendencia(pd.DataFrame({"data_movimentacao":dias, "valor":t[0]}), dias, t[1]).display()

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

destacar_semana(data_labels)

for t in trends:
    plot_linha_tendencia(pd.DataFrame({"data_movimentacao":dias, "valor":t[0]}), dias, t[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Explorando modelos de previsão de série temporal
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparando dados
# MAGIC Para fins de comparação, todos os modelos devem usar os mesmos dados nos mesmos intervalos de tempo. Valores analizados são do total movimentado por dia.

# COMMAND ----------

from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.arima_process import ArmaProcess
import statsmodels.api as sm
import pandas as pd


valores_df = (
    fact_transacoes_dia_df.orderBy("data_movimentacao")
    .groupBy("data_movimentacao")
    .agg(sum("total_movimentacao").alias("total_movimentacao"))
)

valores = valores_df.collect()

treinamento = valores[: len(valores) // 2 + 1]
testes = valores[len(valores) // 2 :]

testes_valores = [v["total_movimentacao"] for v in testes]
treinamento_valores = [v["total_movimentacao"] for v in treinamento]
testes_datas = [v["data_movimentacao"] for v in testes]

# COMMAND ----------

# MAGIC %md
# MAGIC ## ACF/PACF

# COMMAND ----------


acf, acf_conf = sm.tsa.acf(treinamento_valores, nlags=30, alpha=0.05)

plt.figure(figsize=(10, 5))
plt.stem(range(len(acf)), acf)
plt.fill_between(
    range(len(acf)),
    [v[0] for v in acf_conf],
    [v[1] for v in acf_conf],
    color="gray",
    alpha=0.3,
)
plt.xlabel('Lag')
plt.ylabel('Autocorrelation')
plt.title('Autocorrelation Function (ACF)')
plt.grid()

significant_lags = np.where((acf > acf_conf[:,1]) | (acf < acf_conf[:,0]))[0]

# Select the lag corresponding to the first significant spike as the best candidate for p
best_p = significant_lags[0] if len(significant_lags) > 0 else 0
print("Best candidate for p:", best_p)

# Highlight the first significant spike
if len(significant_lags) > 0:
    plt.scatter(significant_lags[0], acf[significant_lags[0]], color='red', marker='o', label=f'p = {significant_lags[0]}')

 
plt.show() 

pacf,pacf_conf = sm.tsa.pacf(treinamento_valores, nlags=30, alpha=0.05)

plt.figure(figsize=(10, 5))
plt.stem(range(len(pacf)), pacf)
plt.xlabel('Lag')
plt.ylabel('Partial Autocorrelation')
plt.title('Partial Autocorrelation Function (PACF)')

# Identify significant spikes (lags with partial autocorrelation values outside the confidence intervals)
significant_lags = np.where((pacf > 2/np.sqrt(len(treinamento_valores))) | (pacf < -2/np.sqrt(len(treinamento_valores))))[0]
print("Significant spikes at lags:", significant_lags)

# Select the lag corresponding to the first significant spike as the best candidate for q
best_q = significant_lags[0] if len(significant_lags) > 0 else 0
print("Best candidate for q:", best_q)

# Highlight the first significant spike
if len(significant_lags) > 0:
    plt.scatter(significant_lags[0], pacf[significant_lags[0]], color='red', marker='o', label=f'q = {significant_lags[0]}')

plt.grid()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ARMA

# COMMAND ----------

from statsmodels.tsa.arima.model import ARIMA

ARMAmodel = ARIMA(treinamento_valores, order=(1, 0, 1))
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

ARIMAmodel = ARIMA(treinamento_valores, order=(1,1, 1))

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

SARIMAXmodel = SARIMAX(treinamento_valores, order = (1,1, 1), seasonal_order=(1,1,1,12))
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
plt.grid(True)
plt.xticks(rotation=45)


residuals = pd.DataFrame(SARIMAXmodel.resid)
residuals.plot(kind='kde', figsize=(20, 6))

print(residuals.describe())
