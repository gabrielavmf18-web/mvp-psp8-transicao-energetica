# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Análise — respostas às perguntas de negócio
# MAGIC `v_sin` soma os 4 subsistemas por hora; `v_sub` é a fato com as dimensões. Horas com `flag_excluir_analise`
# MAGIC são retiradas dos perfis horários.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE workspace.mvp_energia;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW v_sin AS
# MAGIC SELECT t.*, f.hid, f.ter, f.eol, f.sol, f.ger, f.carga, f.cl, f.excluir
# MAGIC FROM (SELECT instante, sum(ger_hidraulica_mw) AS hid, sum(ger_termica_mw) AS ter, sum(ger_eolica_mw) AS eol,
# MAGIC              sum(ger_solar_mw) AS sol, sum(ger_total_mw) AS ger, sum(carga_mw) AS carga,
# MAGIC              sum(carga_liquida_mw) AS cl, bool_or(flag_excluir_analise) AS excluir
# MAGIC       FROM fato_balanco_horario GROUP BY 1) f
# MAGIC JOIN dim_tempo t USING (instante);
# MAGIC
# MAGIC CREATE OR REPLACE VIEW v_sub AS
# MAGIC SELECT f.*, t.ano, t.mes, t.hora, t.data, t.periodo_hidrologico, s.nome_subsistema
# MAGIC FROM fato_balanco_horario f JOIN dim_tempo t USING (instante) JOIN dim_subsistema s USING (id_subsistema);

# COMMAND ----------

# MAGIC %md ## P1 — Participação por fonte (SIN e subsistemas)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ano, round(avg(ger)/1000,1) AS ger_media_gw,
# MAGIC        round(100*sum(hid)/sum(ger),1) AS pct_hidraulica, round(100*sum(ter)/sum(ger),1) AS pct_termica,
# MAGIC        round(100*sum(eol)/sum(ger),1) AS pct_eolica, round(100*sum(sol)/sum(ger),1) AS pct_solar,
# MAGIC        round(100*(sum(eol)+sum(sol))/sum(ger),1) AS pct_eolica_solar
# MAGIC FROM v_sin GROUP BY 1 ORDER BY 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ano, id_subsistema, nome_subsistema,
# MAGIC        round(100*sum(ger_hidraulica_mw)/sum(ger_total_mw),1) AS pct_hidraulica,
# MAGIC        round(100*sum(ger_termica_mw)/sum(ger_total_mw),1) AS pct_termica,
# MAGIC        round(100*sum(ger_eolica_mw)/sum(ger_total_mw),1) AS pct_eolica,
# MAGIC        round(100*sum(ger_solar_mw)/sum(ger_total_mw),1) AS pct_solar,
# MAGIC        round(100*sum(ger_variavel_mw)/sum(ger_total_mw),1) AS pct_eolica_solar
# MAGIC FROM v_sub GROUP BY ALL ORDER BY ano, id_subsistema;

# COMMAND ----------

# MAGIC %md ## P2 — Curva de carga líquida ("curva do pato")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- visualização sugerida: linhas, x = hora, y = carga_liquida_gw, série = ano
# MAGIC SELECT ano, hora, round(avg(carga)/1000,2) AS carga_gw, round(avg(cl)/1000,2) AS carga_liquida_gw,
# MAGIC        round(avg(eol+sol)/1000,2) AS eolica_solar_gw
# MAGIC FROM v_sin WHERE NOT excluir GROUP BY ALL ORDER BY ALL;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH c AS (SELECT ano, hora, avg(cl) AS cl, avg(carga) AS carga FROM v_sin WHERE NOT excluir GROUP BY ALL)
# MAGIC SELECT ano, min_by(hora, cl) AS hora_vale_cl, round(min(cl)/1000,1) AS vale_cl_gw,
# MAGIC        max_by(hora, cl) AS hora_pico_cl, round(max(cl)/1000,1) AS pico_cl_gw,
# MAGIC        round((max(cl)-min(cl))/1000,1) AS amplitude_cl_gw, round(100*min(cl)/max(cl),1) AS vale_sobre_pico_pct,
# MAGIC        min_by(hora, carga) AS hora_vale_carga, max_by(hora, carga) AS hora_pico_carga
# MAGIC FROM c GROUP BY 1 ORDER BY 1;

# COMMAND ----------

# MAGIC %md ## P3 — Rampas da carga líquida

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW rampa AS
# MAGIC SELECT instante, ano, hora, data, cl - lag(cl) OVER (ORDER BY instante) AS rampa_mw,
# MAGIC        excluir OR lag(excluir) OVER (ORDER BY instante) AS excl
# MAGIC FROM v_sin;
# MAGIC
# MAGIC WITH d AS (SELECT data, ano, max(rampa_mw) AS rampa_max, max_by(hora, rampa_mw) AS hora_rampa_max
# MAGIC            FROM rampa WHERE NOT excl AND rampa_mw IS NOT NULL GROUP BY ALL)
# MAGIC SELECT ano, round(avg(rampa_max)) AS media_rampa_max_diaria_mw, round(percentile(rampa_max, 0.95)) AS p95_rampa_max_diaria_mw,
# MAGIC        round(max(rampa_max)) AS maior_rampa_ano_mw, mode(hora_rampa_max) AS hora_mais_frequente_rampa_max,
# MAGIC        round(100*avg(int(hora_rampa_max BETWEEN 16 AND 19)),1) AS pct_dias_rampa_max_16h_19h
# MAGIC FROM d GROUP BY 1 ORDER BY 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ano, hora, round(avg(rampa_mw)) AS rampa_media_mw FROM rampa
# MAGIC WHERE NOT excl AND rampa_mw IS NOT NULL GROUP BY ALL ORDER BY ALL;

# COMMAND ----------

# MAGIC %md ## P4 — Nordeste: autossuficiência renovável e exportação

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ano, count(*) AS horas,
# MAGIC        round(100*avg(int(ger_variavel_mw >= carga_mw)),1) AS pct_horas_eol_sol_supera_carga,
# MAGIC        round(100*avg(int(intercambio_mw > 0)),1) AS pct_horas_exportando,
# MAGIC        round(avg(intercambio_mw)) AS intercambio_medio_mw, round(max(intercambio_mw)) AS exportacao_maxima_mw,
# MAGIC        round(100*sum(ger_variavel_mw)/sum(carga_mw),1) AS eol_sol_sobre_carga_pct
# MAGIC FROM v_sub WHERE id_subsistema = 'NE' AND NOT flag_excluir_analise GROUP BY 1 ORDER BY 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ano, hora, round(avg(intercambio_mw)) AS intercambio_medio_mw
# MAGIC FROM v_sub WHERE id_subsistema = 'NE' AND NOT flag_excluir_analise GROUP BY ALL ORDER BY ALL;

# COMMAND ----------

# MAGIC %md ## P5 — Quem atende a rampa do fim de tarde (16 h–19 h)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH d AS (SELECT instante, ano, hora,
# MAGIC                   cl  - lag(cl)  OVER w AS d_cl,  hid - lag(hid) OVER w AS d_hid,
# MAGIC                   ter - lag(ter) OVER w AS d_ter, excluir OR lag(excluir) OVER w AS excl
# MAGIC            FROM v_sin WINDOW w AS (ORDER BY instante))
# MAGIC SELECT ano, round(avg(d_cl)) AS rampa_media_cl_mw,
# MAGIC        round(100*sum(d_hid)/sum(d_cl),1) AS pct_atendida_hidraulica,
# MAGIC        round(100*sum(d_ter)/sum(d_cl),1) AS pct_atendida_termica,
# MAGIC        round(100 - 100*(sum(d_hid)+sum(d_ter))/sum(d_cl),1) AS pct_outros_intercambio_intl,
# MAGIC        round(corr(d_hid, d_cl),3) AS corr_hidraulica, round(corr(d_ter, d_cl),3) AS corr_termica
# MAGIC FROM d WHERE NOT excl AND hora BETWEEN 16 AND 19 AND d_cl > 0 GROUP BY 1 ORDER BY 1;

# COMMAND ----------

# MAGIC %md ## P6 — CMO × carga líquida

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH b AS (SELECT s.ano, s.cl, f.cmo_rs_mwh AS cmo
# MAGIC            FROM v_sin s JOIN fato_balanco_horario f ON f.instante = s.instante AND f.id_subsistema = 'SE'
# MAGIC            WHERE NOT s.excluir AND f.cmo_rs_mwh IS NOT NULL),
# MAGIC q AS (SELECT *, ntile(5) OVER (PARTITION BY ano ORDER BY cl) AS quintil FROM b)
# MAGIC SELECT ano, quintil, round(min(cl)/1000,1) AS cl_min_gw, round(max(cl)/1000,1) AS cl_max_gw,
# MAGIC        round(avg(cmo),1) AS cmo_medio_se, round(median(cmo),1) AS cmo_mediano_se
# MAGIC FROM q GROUP BY ALL ORDER BY ALL;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH b AS (SELECT s.ano, s.cl, f.cmo_rs_mwh AS cmo,
# MAGIC                   rank() OVER (PARTITION BY s.ano ORDER BY s.cl) AS rc,
# MAGIC                   rank() OVER (PARTITION BY s.ano ORDER BY f.cmo_rs_mwh) AS rm
# MAGIC            FROM v_sin s JOIN fato_balanco_horario f ON f.instante = s.instante AND f.id_subsistema = 'SE'
# MAGIC            WHERE NOT s.excluir AND f.cmo_rs_mwh IS NOT NULL)
# MAGIC SELECT ano, round(corr(cl, cmo),3) AS pearson, round(corr(rc, rm),3) AS spearman, round(avg(cmo),1) AS cmo_medio_se
# MAGIC FROM b GROUP BY 1 ORDER BY 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ano, hora, round(avg(cmo_rs_mwh),1) AS cmo_medio_se FROM v_sub
# MAGIC WHERE id_subsistema = 'SE' AND NOT flag_excluir_analise AND cmo_rs_mwh IS NOT NULL GROUP BY ALL ORDER BY ALL;
