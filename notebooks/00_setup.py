# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup do ambiente
# MAGIC **MVP PSP 8 — Integração de eólica e solar no SIN: carga líquida, rampas e custo marginal (ONS, 2021–2025)**
# MAGIC
# MAGIC | Objeto | Uso |
# MAGIC |---|---|
# MAGIC | `workspace.mvp_energia` | schema com as tabelas Delta (bronze, silver, gold) |
# MAGIC | `/Volumes/workspace/mvp_energia/raw` | arquivos brutos baixados do ONS |

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.mvp_energia COMMENT 'MVP PSP8 - transicao energetica no SIN (ONS 2021-2025)'")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.mvp_energia.raw COMMENT 'Arquivos brutos (camada de pouso)'")
spark.sql("USE workspace.mvp_energia")
display(spark.sql("SHOW VOLUMES"))
