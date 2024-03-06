# Databricks notebook source
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
