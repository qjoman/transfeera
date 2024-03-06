# Databricks notebook source
# jdbc_url = "jdbc:postgresql://postgres_server_origin:5432/database_origin"
# jdbcUsername = dbutils.secrets.get(scope = "jdbc", key = "username")
# jdbcPassword = dbutils.secrets.get(scope = "jdbc", key = "password")
# connection_properties = {
#    "user": jdbcUsername,
#    "password": jdbcPassword,
#    "driver": "org.postgresql.Driver"
# }


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
        
