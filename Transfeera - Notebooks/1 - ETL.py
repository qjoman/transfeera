# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão de dados

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando area de staging
# MAGIC Primeiro passo é a criação de um espaço de stagging onde os dados inalterados são copiados para realização do trabalho. Estas tabelas possuem a mesma estrutura dos dados da origem. Por segurança, estas tabelas são recriadas sempre que o processo for realizado.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Criar o schema de staging
# MAGIC DROP SCHEMA IF EXISTS staging CASCADE;
# MAGIC CREATE SCHEMA staging;
# MAGIC
# MAGIC -- Clientes
# MAGIC DROP TABLE IF EXISTS staging.clientes;
# MAGIC CREATE TABLE staging.clientes (
# MAGIC     id BIGINT,
# MAGIC     razao_social STRING,
# MAGIC     data_inicio_relacionamento DATE,
# MAGIC     status STRING,
# MAGIC     data_fim_relacionamento DATE,
# MAGIC     segmento STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- Contratos
# MAGIC DROP TABLE IF EXISTS staging.contratos;
# MAGIC CREATE TABLE staging.contratos (
# MAGIC     id_contrato BIGINT,
# MAGIC     id_cliente BIGINT,
# MAGIC     taxa_payout DOUBLE,
# MAGIC     taxa_payin DOUBLE,
# MAGIC     taxa_boleto DOUBLE,
# MAGIC     data_vencimento_contrato DATE
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- Transacoes
# MAGIC DROP TABLE IF EXISTS staging.transacoes;
# MAGIC CREATE TABLE staging.transacoes (
# MAGIC     id_cliente BIGINT,
# MAGIC     id_transacao BIGINT,
# MAGIC     tipo_transacao STRING,
# MAGIC     valor_transacao DOUBLE,
# MAGIC     data_movimentacao DATE,
# MAGIC     hora_movimentacao TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Warehouse / modelo final
# MAGIC Deve ser executado somente uma vez. Onde dados devem ser armazenados depois de transformados e onde consultas devem ocorrer.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Criar schema se não existir
# MAGIC DROP SCHEMA IF EXISTS dw CASCADE;
# MAGIC CREATE SCHEMA IF NOT EXISTS dw;
# MAGIC
# MAGIC -- Time Dimension Table
# MAGIC DROP TABLE IF EXISTS dw.dim_tempo;
# MAGIC CREATE TABLE IF NOT EXISTS dw.dim_tempo (
# MAGIC     data DATE,
# MAGIC     ano INT,
# MAGIC     mes INT,
# MAGIC     dia INT,
# MAGIC     dia_da_semana INT,
# MAGIC     dia_do_ano INT,
# MAGIC     trimestre INT
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- Clientes Dimension Table
# MAGIC -- DROP TABLE IF EXISTS dw.dim_clientes;
# MAGIC CREATE TABLE IF NOT EXISTS dw.dim_clientes (
# MAGIC     id_cliente BIGINT,
# MAGIC     razao_social STRING,
# MAGIC     data_inicio_relacionamento DATE,
# MAGIC     status STRING,
# MAGIC     data_fim_relacionamento DATE,
# MAGIC     segmento STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- Contratos Dimension Table
# MAGIC -- DROP TABLE IF EXISTS dw.dim_contratos;
# MAGIC CREATE TABLE IF NOT EXISTS dw.dim_contratos (
# MAGIC     id_contrato BIGINT,
# MAGIC     id_cliente BIGINT,
# MAGIC     taxa_payout DOUBLE,
# MAGIC     taxa_payin DOUBLE,
# MAGIC     taxa_boleto DOUBLE,
# MAGIC     data_vencimento_contrato DATE
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- Transacoes Fact Table
# MAGIC -- DROP TABLE IF EXISTS dw.fact_transacoes;
# MAGIC CREATE TABLE IF NOT EXISTS dw.fact_transacoes (
# MAGIC     id_transacao BIGINT,
# MAGIC     id_cliente BIGINT,
# MAGIC     id_contrato BIGINT,
# MAGIC     tipo_transacao STRING,
# MAGIC     valor_transacao DOUBLE,
# MAGIC     data_movimentacao DATE,
# MAGIC     hora_movimentacao TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- Transacoes Dia Fact Table
# MAGIC -- DROP TABLE IF EXISTS dw.fact_transacoes_dia;
# MAGIC CREATE TABLE IF NOT EXISTS dw.fact_transacoes_dia (
# MAGIC     id_cliente BIGINT,
# MAGIC     id_contrato BIGINT,
# MAGIC     data_movimentacao DATE,
# MAGIC     total_movimentacao DOUBLE,
# MAGIC     total_boleto DOUBLE,
# MAGIC     total_payin DOUBLE,
# MAGIC     total_payout DOUBLE,
# MAGIC     quantidade_movimentacoes INT,
# MAGIC     quantidade_boleto INT,
# MAGIC     quantidade_payout INT,
# MAGIC     quantidade_payin INT
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- DROP TABLE IF EXISTS dw.dim_tempo;
# MAGIC CREATE TABLE IF NOT EXISTS dw.dim_tempo (
# MAGIC     data DATE,
# MAGIC     ano INT,
# MAGIC     mes INT,
# MAGIC     dia INT,
# MAGIC     dia_da_semana INT,
# MAGIC     dia_do_ano INT,
# MAGIC     trimestre INT
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestão de dados
# MAGIC Leitura dos arquivos CSV. Dados serão copiados para as tabelas de staging.
# MAGIC Trocar a leitura de dados no futuro para a conexão necessária com bancos externos.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configurar conexão com o banco
# MAGIC Exemplo de conexão com PostgreSQL. Ignorando detalhes de segurança, apenas utilizando conexão básica com usuário e senha.

# COMMAND ----------

# jdbc_url = "jdbc:postgresql://postgres_server_origin:5432/database_origin"
# jdbcUsername = dbutils.secrets.get(scope = "jdbc", key = "username")
# jdbcPassword = dbutils.secrets.get(scope = "jdbc", key = "password")
# connection_properties = {
#    "user": jdbcUsername,
#    "password": jdbcPassword,
#    "driver": "org.postgresql.Driver"
# }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestão do CSV (Delta table)
# MAGIC Exemplos de como a ingestão ocorreria com PostgreSQL  estão presentes nos comentários

# COMMAND ----------

# Leitura do CSV de clientes
df_clientes = spark.read \
    .format("delta") \
    .option("header", "true") \
    .load("dbfs:/user/hive/warehouse/clientes")

# Dados de clientes do servidor PostgreSQL
# df_postgres_cliente = spark.read.jdbc(url=jdbc_url, table="cliente", properties=connection_properties)
# df_postgres_cliente.write.format("delta").mode("overwrite").saveAsTable("staging.clientes")

# Inserindo dados na tabela de staging
df_clientes.write.format("delta") \
        .mode("overwrite") \
        .save("dbfs:/user/hive/warehouse/staging.db/clientes")

# ---------------------

# Leitura do CSV de transações
df_contratos = spark.read \
    .format("delta") \
    .option("header", "true") \
    .load("dbfs:/user/hive/warehouse/contratos")

# Dados de transações do servidor PostgreSQL
# df_postgres_contratos = spark.read.jdbc(url=jdbc_url, table="contratos", properties=connection_properties)
# df_postgres_contratos.write.format("delta").mode("overwrite").saveAsTable("staging.contratos")

# Inserindo dados na tabela de staging
df_contratos.write.format("delta") \
        .mode("overwrite") \
        .save("dbfs:/user/hive/warehouse/staging.db/contratos")

# ---------------------

# Leitura do CSV de transações
df_transacoes = spark.read \
    .format("delta") \
    .option("header", "true") \
    .load("dbfs:/user/hive/warehouse/transacoes")

# Dados de transações do servidor PostgreSQL
# df_postgres_transacoes = spark.read.jdbc(url=jdbc_url, table="transacoes", properties=connection_properties)
# df_postgres_transacoes.write.format("delta").mode("overwrite").saveAsTable("staging.transacoes")

# Inserindo dados na tabela de staging
df_transacoes.write \
        .format("delta") \
        .mode("overwrite") \
        .save("dbfs:/user/hive/warehouse/staging.db/transacoes")
        

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformando modelo e populando tabelas

# COMMAND ----------

# MAGIC %md
# MAGIC ### Populando dim_tempo
# MAGIC Puramente pra facilitar a escrita de queries. Identifica a primeira data de todas as transações e a última data.

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import desc
from datetime import datetime, timedelta

t_start = df_transacoes.orderBy("data_movimentacao").take(1)[0]["data_movimentacao"]
t_end = df_transacoes.orderBy(desc("data_movimentacao")).take(1)[0]["data_movimentacao"]


start_date = datetime.combine(t_start, datetime.min.time())
end_date = datetime.combine(t_end, datetime.max.time())

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Populando dim_clientes
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dw.dim_clientes as dim_c USING staging.clientes as stg_c ON dim_c.id_cliente = stg_c.id
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   (
# MAGIC     id_cliente,
# MAGIC     razao_social,
# MAGIC     data_inicio_relacionamento,
# MAGIC     status,
# MAGIC     data_fim_relacionamento,
# MAGIC     segmento
# MAGIC   )
# MAGIC VALUES(
# MAGIC     stg_c.id,
# MAGIC     stg_c.razao_social,
# MAGIC     stg_c.data_inicio_relacionamento,
# MAGIC     stg_c.status,
# MAGIC     stg_c.data_fim_relacionamento,
# MAGIC     stg_c.segmento
# MAGIC   );
# MAGIC -- WHEN MATCHED THEN
# MAGIC   -- TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ### Populando dim_contratos

# COMMAND ----------

# MAGIC %sql
# MAGIC   MERGE INTO dw.dim_contratos as dim_c USING staging.contratos as stg_c ON dim_c.id_contrato = stg_c.id_contrato
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   *;
# MAGIC -- WHEN MATCHED THEN
# MAGIC   -- TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ### Populando fact_transacoes

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dw.fact_transacoes AS fact_t USING (
# MAGIC   SELECT
# MAGIC     st.id_transacao,
# MAGIC     st.id_cliente,
# MAGIC     sc.id_contrato,
# MAGIC     st.tipo_transacao,
# MAGIC     st.valor_transacao,
# MAGIC     st.data_movimentacao,
# MAGIC     st.hora_movimentacao
# MAGIC   FROM
# MAGIC     staging.transacoes st
# MAGIC     JOIN staging.contratos sc ON sc.id_cliente = st.id_cliente
# MAGIC   WHERE
# MAGIC     st.data_movimentacao <= sc.data_vencimento_contrato
# MAGIC     AND sc.data_vencimento_contrato = (
# MAGIC       SELECT
# MAGIC         MAX(data_vencimento_contrato)
# MAGIC       FROM
# MAGIC         dw.dim_contratos
# MAGIC       WHERE
# MAGIC         id_cliente = st.id_cliente
# MAGIC         AND data_vencimento_contrato >= st.data_movimentacao
# MAGIC     )
# MAGIC ) AS stg_t ON fact_t.id_transacao = stg_t.id_transacao
# MAGIC AND fact_t.id_cliente = stg_t.id_cliente
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   *;
# MAGIC -- WHEN MATCHED THEN
# MAGIC   -- TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ### Populando fact_transacoes_dia
# MAGIC Tabela para computar sumário dados diários, não deve ser utilizado pra query em dados recentes/tempo real. Atualizado por jobs diários com infos do dia anterior

# COMMAND ----------

# MAGIC %sql
# MAGIC   MERGE INTO dw.fact_transacoes_dia AS fact_t_d USING (
# MAGIC     SELECT
# MAGIC       st.id_cliente,
# MAGIC       data_movimentacao,
# MAGIC       id_contrato,
# MAGIC       COUNT(st.id_cliente) as quantidade_movimentacoes,
# MAGIC       SUM(
# MAGIC         CASE
# MAGIC           WHEN tipo_transacao = 'payin' THEN 1
# MAGIC           ELSE 0
# MAGIC         END
# MAGIC       ) AS quantidade_payin,
# MAGIC       SUM(
# MAGIC         CASE
# MAGIC           WHEN tipo_transacao = 'payout' THEN 1
# MAGIC           ELSE 0
# MAGIC         END
# MAGIC       ) AS quantidade_payout,
# MAGIC       SUM(
# MAGIC         CASE
# MAGIC           WHEN tipo_transacao = 'boleto' THEN 1
# MAGIC           ELSE 0
# MAGIC         END
# MAGIC       ) AS quantidade_boleto,
# MAGIC       SUM(valor_transacao) as total_movimentacao,
# MAGIC       SUM(
# MAGIC         CASE
# MAGIC           WHEN tipo_transacao = 'payin' THEN valor_transacao
# MAGIC           ELSE 0
# MAGIC         END
# MAGIC       ) AS total_payin,
# MAGIC       SUM(
# MAGIC         CASE
# MAGIC           WHEN tipo_transacao = 'payout' THEN valor_transacao
# MAGIC           ELSE 0
# MAGIC         END
# MAGIC       ) AS total_payout,
# MAGIC       SUM(
# MAGIC         CASE
# MAGIC           WHEN tipo_transacao = 'boleto' THEN valor_transacao
# MAGIC           ELSE 0
# MAGIC         END
# MAGIC       ) AS total_boleto
# MAGIC     FROM
# MAGIC       staging.transacoes st
# MAGIC       JOIN staging.contratos sc ON sc.id_cliente = st.id_cliente
# MAGIC     WHERE
# MAGIC       st.data_movimentacao <= sc.data_vencimento_contrato
# MAGIC       AND sc.data_vencimento_contrato = (
# MAGIC         SELECT
# MAGIC           MAX(data_vencimento_contrato)
# MAGIC         FROM
# MAGIC           dw.dim_contratos
# MAGIC         WHERE
# MAGIC           id_cliente = st.id_cliente
# MAGIC           AND data_vencimento_contrato >= st.data_movimentacao
# MAGIC       )
# MAGIC     GROUP BY
# MAGIC       data_movimentacao,
# MAGIC       st.id_cliente,
# MAGIC       sc.id_contrato
# MAGIC   ) AS stg_t_d ON fact_t_d.id_cliente = stg_t_d.id_cliente
# MAGIC   AND fact_t_d.data_movimentacao = stg_t_d.data_movimentacao
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   *;

# COMMAND ----------

# MAGIC %md
# MAGIC #Limpando stagging

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS staging CASCADE;
