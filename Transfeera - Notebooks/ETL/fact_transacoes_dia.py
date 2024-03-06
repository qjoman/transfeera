# Databricks notebook source
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
# MAGIC       dw.transacoes st
# MAGIC       JOIN dw.contratos sc ON sc.id_cliente = st.id_cliente
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
# MAGIC   
