# Databricks notebook source
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
