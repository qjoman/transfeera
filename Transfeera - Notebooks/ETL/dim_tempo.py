# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import desc
from datetime import datetime, timedelta

t_start = df_transacoes.orderBy("data_movimentacao").take(1)[0]["data_movimentacao"]
t_end = df_transacoes.orderBy(desc("data_movimentacao")).take(1)[0]["data_movimentacao"]


start_date = datetime.combine(t_start, datetime.min.time())
end_date = datetime.now()

dates = []

current_date = start_date
while current_date <= end_date:
    dates.append((current_date.date(), current_date.year, current_date.month, current_date.day, current_date.weekday() + 1, current_date.timetuple().tm_yday, (current_date.month - 1) // 3 + 1))
    current_date += timedelta(days=1)

df = spark.createDataFrame(dates, schema=["data", "ano", "mes", "dia", "dia_da_semana", "dia_do_ano", "trimestre"])

df.createOrReplaceTempView("new_data")
spark.sql("""
    MERGE INTO dw.dim_tempo AS target
    USING new_data AS source
    ON target.data = source.data
    WHEN NOT MATCHED THEN INSERT *
""")
