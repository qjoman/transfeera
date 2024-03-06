# Databricks notebook source
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
