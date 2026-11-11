# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · ETL — bronze → silver → gold (Delta Lake)
# MAGIC
# MAGIC | Camada | Tabela | Conteúdo |
# MAGIC |---|---|---|
# MAGIC | bronze | `bronze_balanco`, `bronze_cmo` | arquivos do ONS como chegaram (texto) + arquivo de origem |
# MAGIC | silver | `silver_balanco` | balanço horário tipado, sem a linha agregada SIN, com marcações de qualidade |
# MAGIC | silver | `silver_cmo_horario` | CMO semi-horário agregado para a hora |
# MAGIC | silver | `silver_dia_solar_ausente` | dias com falha na série solar (R5) |
# MAGIC | gold | `fato_balanco_horario`, `dim_subsistema`, `dim_tempo` | modelo estrela |
# MAGIC
# MAGIC Regras de negócio R1–R7: README, seção 6.

# COMMAND ----------

spark.sql("USE workspace.mvp_energia")
RAW = "/Volumes/workspace/mvp_energia/raw"

# COMMAND ----------

# MAGIC %md ## Bronze

# COMMAND ----------

from pyspark.sql import functions as F

for nome, padrao in [("bronze_balanco", "balanco/BALANCO_ENERGIA_SUBSISTEMA_*.csv"),
                     ("bronze_cmo", "cmo/CMO_SEMIHORARIO_*.csv")]:
    (spark.read.option("header", True).option("sep", ";").csv(f"{RAW}/{padrao}")
     .withColumn("arquivo_origem", F.regexp_extract(F.col("_metadata.file_name"), r"^[A-Z_]+_\d{4}", 0))
     .withColumn("dt_carga", F.current_timestamp())
     .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(nome))
    print(nome, spark.table(nome).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver
# MAGIC * **R1** tipagem (TIMESTAMP, DOUBLE) e deduplicação por (subsistema, instante)
# MAGIC * **R2** remove a linha `SIN`, que é a soma dos 4 subsistemas (identidade verificada em `03_qualidade`); o SIN é recalculado por soma
# MAGIC * **R3** `flag_solar_noturno`: solar > 100 MW entre 0 h e 4 h
# MAGIC * **R4** `periodo_com_mmgd`: a partir de 29/04/2023 a série solar (e a carga) passa a incluir a micro e minigeração distribuída
# MAGIC * **R5** dia com solar ausente: soma solar SIN 10 h–14 h < 10% da mediana do mês
# MAGIC * **R6** CMO semi-horário → horário pela média das duas meias-horas; `flag_cmo_negativo` se alguma meia-hora < 0

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver_balanco AS
# MAGIC WITH t AS (
# MAGIC   SELECT upper(trim(id_subsistema)) AS id_subsistema,
# MAGIC          CAST(din_instante AS TIMESTAMP) AS instante,
# MAGIC          CAST(val_gerhidraulica AS DOUBLE) AS ger_hidraulica_mw,
# MAGIC          CAST(val_gertermica    AS DOUBLE) AS ger_termica_mw,
# MAGIC          CAST(val_gereolica     AS DOUBLE) AS ger_eolica_mw,
# MAGIC          CAST(val_gersolar      AS DOUBLE) AS ger_solar_mw,
# MAGIC          CAST(val_carga         AS DOUBLE) AS carga_mw,
# MAGIC          CAST(val_intercambio   AS DOUBLE) AS intercambio_mw,
# MAGIC          arquivo_origem
# MAGIC   FROM bronze_balanco
# MAGIC   WHERE upper(trim(id_subsistema)) <> 'SIN'
# MAGIC   QUALIFY row_number() OVER (PARTITION BY id_subsistema, din_instante ORDER BY arquivo_origem DESC) = 1
# MAGIC )
# MAGIC SELECT *,
# MAGIC        (hour(instante) BETWEEN 0 AND 4 AND ger_solar_mw > 100) AS flag_solar_noturno,
# MAGIC        instante >= TIMESTAMP'2023-04-29' AS periodo_com_mmgd
# MAGIC FROM t;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE silver_dia_solar_ausente AS
# MAGIC WITH d AS (SELECT CAST(instante AS DATE) AS data, sum(ger_solar_mw) AS solar_meio_dia
# MAGIC            FROM silver_balanco WHERE hour(instante) BETWEEN 10 AND 14 GROUP BY 1),
# MAGIC m AS (SELECT *, percentile(solar_meio_dia, 0.5) OVER (PARTITION BY date_trunc('MONTH', data)) AS mediana_mes FROM d)
# MAGIC SELECT data, solar_meio_dia, mediana_mes FROM m WHERE solar_meio_dia < 0.1 * mediana_mes;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE silver_cmo_horario AS
# MAGIC SELECT upper(trim(id_subsistema)) AS id_subsistema,
# MAGIC        date_trunc('HOUR', CAST(din_instante AS TIMESTAMP)) AS instante,
# MAGIC        avg(CAST(val_cmo AS DOUBLE)) AS cmo_rs_mwh,
# MAGIC        count(*) AS n_meias_horas,
# MAGIC        min(CAST(val_cmo AS DOUBLE)) < 0 AS flag_cmo_negativo
# MAGIC FROM bronze_cmo GROUP BY ALL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — modelo estrela
# MAGIC * **R7** junção balanço × CMO por (subsistema, hora) com `LEFT JOIN`: horas sem CMO ficam nulas (não são imputadas)
# MAGIC * Medidas derivadas: `ger_total_mw`, `ger_variavel_mw` (eólica + solar), `carga_liquida_mw` (carga − eólica − solar), `residuo_balanco_mw`
# MAGIC * `flag_excluir_analise` = solar noturno ou dia com solar ausente

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_subsistema COMMENT 'Subsistemas do SIN' AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('SE', 'Sudeste/Centro-Oeste', 'ES, MG, RJ, SP, GO, DF, MT, MS, AC, RO'),
# MAGIC   ('S',  'Sul',                  'PR, SC, RS'),
# MAGIC   ('NE', 'Nordeste',             'AL, BA, CE, PB, PE, PI, RN, SE'),
# MAGIC   ('N',  'Norte',                'AM, AP, MA, PA, TO, RR')
# MAGIC AS t(id_subsistema, nome_subsistema, ufs_principais);
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dim_tempo COMMENT 'Dimensão horária 2021-2025' AS
# MAGIC SELECT i AS instante, CAST(i AS DATE) AS data, hour(i) AS hora, year(i) AS ano, month(i) AS mes,
# MAGIC        date_format(i, 'yyyy-MM') AS ano_mes, extract(DAYOFWEEK_ISO FROM i) AS dia_semana_num,
# MAGIC        CASE WHEN extract(DAYOFWEEK_ISO FROM i) = 7 THEN 'domingo'
# MAGIC             WHEN extract(DAYOFWEEK_ISO FROM i) = 6 THEN 'sábado' ELSE 'dia útil' END AS tipo_dia,
# MAGIC        CASE WHEN month(i) IN (11, 12, 1, 2, 3, 4) THEN 'úmido' ELSE 'seco' END AS periodo_hidrologico,
# MAGIC        CASE WHEN hour(i) BETWEEN 18 AND 20 THEN 'ponta' ELSE 'fora de ponta' END AS posto_tarifario,
# MAGIC        i >= TIMESTAMP'2023-04-29' AS periodo_com_mmgd
# MAGIC FROM (SELECT explode(sequence(TIMESTAMP'2021-01-01 00:00:00', TIMESTAMP'2025-12-31 23:00:00', INTERVAL 1 HOUR)) AS i);
# MAGIC
# MAGIC CREATE OR REPLACE TABLE fato_balanco_horario COMMENT 'Fato: balanço de energia por subsistema e hora + CMO' AS
# MAGIC SELECT b.id_subsistema, b.instante,
# MAGIC        b.ger_hidraulica_mw, b.ger_termica_mw, b.ger_eolica_mw, b.ger_solar_mw,
# MAGIC        b.ger_hidraulica_mw + b.ger_termica_mw + b.ger_eolica_mw + b.ger_solar_mw AS ger_total_mw,
# MAGIC        b.ger_eolica_mw + b.ger_solar_mw AS ger_variavel_mw,
# MAGIC        b.carga_mw,
# MAGIC        b.carga_mw - b.ger_eolica_mw - b.ger_solar_mw AS carga_liquida_mw,
# MAGIC        b.intercambio_mw,
# MAGIC        b.ger_hidraulica_mw + b.ger_termica_mw + b.ger_eolica_mw + b.ger_solar_mw - b.carga_mw - b.intercambio_mw AS residuo_balanco_mw,
# MAGIC        c.cmo_rs_mwh,
# MAGIC        b.flag_solar_noturno,
# MAGIC        a.data IS NOT NULL AS flag_solar_ausente,
# MAGIC        coalesce(c.flag_cmo_negativo, false) AS flag_cmo_negativo,
# MAGIC        b.flag_solar_noturno OR a.data IS NOT NULL AS flag_excluir_analise
# MAGIC FROM silver_balanco b
# MAGIC LEFT JOIN silver_cmo_horario c USING (id_subsistema, instante)
# MAGIC LEFT JOIN silver_dia_solar_ausente a ON a.data = CAST(b.instante AS DATE);

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE dim_subsistema ALTER COLUMN id_subsistema SET NOT NULL;
# MAGIC ALTER TABLE dim_tempo      ALTER COLUMN instante      SET NOT NULL;
# MAGIC ALTER TABLE dim_subsistema ADD CONSTRAINT pk_subsistema PRIMARY KEY (id_subsistema);
# MAGIC ALTER TABLE dim_tempo      ADD CONSTRAINT pk_tempo      PRIMARY KEY (instante);
# MAGIC ALTER TABLE fato_balanco_horario ADD CONSTRAINT fk_subsistema FOREIGN KEY (id_subsistema) REFERENCES dim_subsistema;
# MAGIC ALTER TABLE fato_balanco_horario ADD CONSTRAINT fk_tempo      FOREIGN KEY (instante)      REFERENCES dim_tempo;

# COMMAND ----------

# MAGIC %md ## Contagem por camada

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bronze_balanco' AS tabela, count(*) AS linhas FROM bronze_balanco
# MAGIC UNION ALL SELECT 'bronze_cmo', count(*) FROM bronze_cmo
# MAGIC UNION ALL SELECT 'silver_balanco', count(*) FROM silver_balanco
# MAGIC UNION ALL SELECT 'silver_cmo_horario', count(*) FROM silver_cmo_horario
# MAGIC UNION ALL SELECT 'silver_dia_solar_ausente', count(*) FROM silver_dia_solar_ausente
# MAGIC UNION ALL SELECT 'fato_balanco_horario', count(*) FROM fato_balanco_horario
# MAGIC UNION ALL SELECT 'dim_subsistema', count(*) FROM dim_subsistema
# MAGIC UNION ALL SELECT 'dim_tempo', count(*) FROM dim_tempo;
