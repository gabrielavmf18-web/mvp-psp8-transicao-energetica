# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Qualidade dos dados
# MAGIC Completude, unicidade, consistência (identidade do balanço e agregado SIN), conformidade, acurácia e
# MAGIC consistência temporal (quebra metodológica). Resultados esperados em `resultados/qualidade_*.csv`.

# COMMAND ----------

spark.sql("USE workspace.mvp_energia")

# COMMAND ----------

# MAGIC %md ## Perfil por atributo (bronze)

# COMMAND ----------

from pyspark.sql import functions as F

linhas = []
for tabela, cols in [("bronze_balanco", ["id_subsistema", "nom_subsistema", "din_instante", "val_gerhidraulica",
                                          "val_gertermica", "val_gereolica", "val_gersolar", "val_carga", "val_intercambio"]),
                     ("bronze_cmo", ["id_subsistema", "nom_subsistema", "din_instante", "val_cmo"])]:
    df = spark.table(tabela)
    total = df.count()
    for c in cols:
        num = c.startswith("val_")
        expr = F.expr(f"try_cast({c} AS DOUBLE)") if num else F.col(c)
        r = df.agg(F.count(c).alias("nn"), F.countDistinct(c).alias("dist"),
                   F.min(expr).cast("string").alias("mn"), F.max(expr).cast("string").alias("mx"),
                   (F.count(c) - F.count(expr)).alias("inv")).first()
        linhas.append((tabela, c, total, total - r.nn, r.dist, r.mn, r.mx, r.inv if num else None))
display(spark.createDataFrame(linhas, "tabela string, atributo string, registros long, nulos long, distintos long, "
                                      "minimo string, maximo string, nao_numericos long"))

# COMMAND ----------

# MAGIC %md ## Regras de qualidade

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Unicidade: (subsistema, instante) duplicado no balanço' AS regra,
# MAGIC        (SELECT count(*) - count(DISTINCT id_subsistema, din_instante) FROM bronze_balanco) AS ocorrencias
# MAGIC UNION ALL SELECT 'Unicidade: (subsistema, instante) duplicado no CMO',
# MAGIC        (SELECT count(*) - count(DISTINCT id_subsistema, din_instante) FROM bronze_cmo)
# MAGIC UNION ALL SELECT 'Completude: horas faltantes no balanço (4 × 43.824 esperadas)', (SELECT 4 * 43824 - count(*) FROM silver_balanco)
# MAGIC UNION ALL SELECT 'Completude: horas sem CMO após a junção', (SELECT count_if(cmo_rs_mwh IS NULL) FROM fato_balanco_horario)
# MAGIC UNION ALL SELECT 'Completude: horas de CMO com menos de 2 meias-horas', (SELECT count_if(n_meias_horas < 2) FROM silver_cmo_horario)
# MAGIC UNION ALL SELECT 'Consistência: SIN ≠ soma dos subsistemas (carga, |dif| > 1 MW)',
# MAGIC        (SELECT count(*) FROM bronze_balanco s
# MAGIC         JOIN (SELECT din_instante, sum(CAST(val_carga AS DOUBLE)) AS c FROM bronze_balanco WHERE id_subsistema <> 'SIN' GROUP BY 1) x
# MAGIC           ON x.din_instante = s.din_instante
# MAGIC         WHERE s.id_subsistema = 'SIN' AND abs(CAST(s.val_carga AS DOUBLE) - x.c) > 1)
# MAGIC UNION ALL SELECT 'Consistência: |geração − carga − intercâmbio| > 500 MW', (SELECT count_if(abs(residuo_balanco_mw) > 500) FROM fato_balanco_horario)
# MAGIC UNION ALL SELECT 'Conformidade: geração ou carga negativas',
# MAGIC        (SELECT count_if(least(ger_hidraulica_mw, ger_termica_mw, ger_eolica_mw, ger_solar_mw, carga_mw) < 0) FROM fato_balanco_horario)
# MAGIC UNION ALL SELECT 'Acurácia: solar > 100 MW entre 0h e 4h (R3)', (SELECT count_if(flag_solar_noturno) FROM fato_balanco_horario)
# MAGIC UNION ALL SELECT 'Acurácia: dias com solar ausente ao meio-dia (R5)', (SELECT count(*) FROM silver_dia_solar_ausente)
# MAGIC UNION ALL SELECT 'Acurácia: horas com CMO negativo', (SELECT count_if(flag_cmo_negativo) FROM fato_balanco_horario)
# MAGIC UNION ALL SELECT 'Total excluído das análises (flag_excluir_analise)', (SELECT count_if(flag_excluir_analise) FROM fato_balanco_horario);

# COMMAND ----------

# MAGIC %md ## Domínios observados por subsistema

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id_subsistema, count(*) AS linhas,
# MAGIC        round(min(ger_hidraulica_mw)) AS hid_min, round(max(ger_hidraulica_mw)) AS hid_max,
# MAGIC        round(min(ger_termica_mw)) AS ter_min, round(max(ger_termica_mw)) AS ter_max,
# MAGIC        round(min(ger_eolica_mw)) AS eol_min, round(max(ger_eolica_mw)) AS eol_max,
# MAGIC        round(min(ger_solar_mw)) AS sol_min, round(max(ger_solar_mw)) AS sol_max,
# MAGIC        round(min(carga_mw)) AS carga_min, round(max(carga_mw)) AS carga_max,
# MAGIC        round(min(intercambio_mw)) AS int_min, round(max(intercambio_mw)) AS int_max,
# MAGIC        round(min(cmo_rs_mwh), 2) AS cmo_min, round(max(cmo_rs_mwh), 2) AS cmo_max
# MAGIC FROM fato_balanco_horario GROUP BY 1 ORDER BY 1;

# COMMAND ----------

# MAGIC %md ## Quebra metodológica de 29/04/2023 (inclusão da MMGD) — 14 dias antes × depois

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH d AS (SELECT CAST(instante AS DATE) AS data, sum(ger_solar_mw)/24 AS solar_med, sum(carga_mw)/24 AS carga_med,
# MAGIC                   sum(carga_liquida_mw)/24 AS cl_med
# MAGIC            FROM fato_balanco_horario GROUP BY 1)
# MAGIC SELECT CASE WHEN data < DATE'2023-04-29' THEN 'antes (14 dias)' ELSE 'depois (14 dias)' END AS janela,
# MAGIC        round(avg(solar_med)) AS solar_medio_mw, round(avg(carga_med)) AS carga_media_mw, round(avg(cl_med)) AS carga_liquida_media_mw
# MAGIC FROM d WHERE data BETWEEN DATE'2023-04-15' AND DATE'2023-05-12' GROUP BY 1 ORDER BY 1;
