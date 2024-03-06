# Databricks notebook source
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
