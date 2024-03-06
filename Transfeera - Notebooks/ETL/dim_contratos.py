# Databricks notebook source
# MAGIC %sql
# MAGIC   MERGE INTO dw.dim_contratos as dim_c USING staging.contratos as stg_c ON dim_c.id_contrato = stg_c.id_contrato
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   *;
# MAGIC -- WHEN MATCHED THEN
# MAGIC   -- TODO
