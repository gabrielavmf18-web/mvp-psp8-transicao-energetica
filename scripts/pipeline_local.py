"""
Reprodução local do pipeline do MVP (mesma lógica dos notebooks Databricks).

Uso:
    python scripts/pipeline_local.py            # coleta (se necessário) + ETL + qualidade + análise
    python scripts/pipeline_local.py --sem-coleta

Gera:
    dados/bronze/...                 arquivos brutos (NÃO versionados, ver .gitignore)
    dados/gold/*.parquet             modelo estrela
    resultados/*.csv                 tabelas de qualidade e respostas às perguntas
    evidencias/graficos/*.png        gráficos usados no README

O motor local é o DuckDB; no Databricks as mesmas regras são escritas em Spark SQL.
"""
import argparse
import datetime as dt
import json
import pathlib
import urllib.request

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RAIZ = pathlib.Path(__file__).resolve().parents[1]
BRONZE = RAIZ / "dados" / "bronze"
GOLD = RAIZ / "dados" / "gold"
RES = RAIZ / "resultados"
GRAF = RAIZ / "evidencias" / "graficos"

ANOS = range(2021, 2026)
S3 = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset"
FONTES = {
    "balanco": S3 + "/balanco_energia_subsistema_ho/BALANCO_ENERGIA_SUBSISTEMA_{ano}.csv",
    "cmo": S3 + "/cmo_tm/CMO_SEMIHORARIO_{ano}.csv",
}
DATA_MMGD = "2023-04-29"  # quebra metodológica detectada na série solar (ver README, seção 7)

CORES = {2021: "#9dc3e6", 2022: "#6fa8dc", 2023: "#3d85c6", 2024: "#1f4e79", 2025: "#c55a11"}


# --------------------------------------------------------------------------- coleta
def coletar() -> None:
    log = []
    for nome, url in FONTES.items():
        for ano in ANOS:
            u = url.format(ano=ano)
            destino = BRONZE / nome / u.rsplit("/", 1)[1]
            destino.parent.mkdir(parents=True, exist_ok=True)
            if not destino.exists():
                urllib.request.urlretrieve(u, destino)
            log.append({"arquivo": destino.name, "url": u, "bytes": destino.stat().st_size,
                        "coletado_em": dt.datetime.fromtimestamp(destino.stat().st_mtime).isoformat(timespec="seconds")})
    RES.mkdir(exist_ok=True)
    (RES / "log_coleta.json").write_text(json.dumps(log, indent=2, ensure_ascii=False), encoding="utf-8")


# --------------------------------------------------------------------------- ETL
SQL_BRONZE = """
CREATE OR REPLACE TABLE bronze_balanco AS
SELECT * EXCLUDE (filename), regexp_extract(filename, 'BALANCO_ENERGIA_SUBSISTEMA_[0-9]+') AS arquivo_origem
FROM read_csv('{b}/balanco/*.csv', delim=';', header=true, filename=true, all_varchar=true);

CREATE OR REPLACE TABLE bronze_cmo AS
SELECT * EXCLUDE (filename), regexp_extract(filename, 'CMO_SEMIHORARIO_[0-9]+') AS arquivo_origem
FROM read_csv('{b}/cmo/*.csv', delim=';', header=true, filename=true, all_varchar=true);
"""

SQL_SILVER = f"""
-- R1 tipagem; R2 remove o agregado 'SIN' (é a soma dos subsistemas; validado em qualidade)
CREATE OR REPLACE TABLE silver_balanco AS
WITH t AS (
    SELECT upper(trim(id_subsistema)) AS id_subsistema,
           CAST(din_instante AS TIMESTAMP) AS instante,
           CAST(val_gerhidraulica AS DOUBLE) AS ger_hidraulica_mw,
           CAST(val_gertermica    AS DOUBLE) AS ger_termica_mw,
           CAST(val_gereolica     AS DOUBLE) AS ger_eolica_mw,
           CAST(val_gersolar      AS DOUBLE) AS ger_solar_mw,
           CAST(val_carga         AS DOUBLE) AS carga_mw,
           CAST(val_intercambio   AS DOUBLE) AS intercambio_mw,
           arquivo_origem
    FROM bronze_balanco
    WHERE upper(trim(id_subsistema)) <> 'SIN'
    QUALIFY row_number() OVER (PARTITION BY id_subsistema, din_instante ORDER BY arquivo_origem DESC) = 1
)
SELECT *,
       -- R3 solar noturno (0h–4h) acima de 100 MW é fisicamente implausível
       (hour(instante) BETWEEN 0 AND 4 AND ger_solar_mw > 100) AS flag_solar_noturno,
       -- R4 marca o regime metodológico da série solar/carga (inclusão da MMGD)
       instante >= TIMESTAMP '{DATA_MMGD}' AS periodo_com_mmgd
FROM t;

-- R5 dias com geração solar ausente: soma solar das 10h–14h do SIN < 10% da mediana do mês
CREATE OR REPLACE TABLE silver_dia_solar_ausente AS
WITH d AS (SELECT CAST(instante AS DATE) AS data, sum(ger_solar_mw) AS solar_meio_dia
           FROM silver_balanco WHERE hour(instante) BETWEEN 10 AND 14 GROUP BY 1),
m AS (SELECT *, median(solar_meio_dia) OVER (PARTITION BY date_trunc('month', data)) AS mediana_mes FROM d)
SELECT data, solar_meio_dia, mediana_mes FROM m WHERE solar_meio_dia < 0.1 * mediana_mes;

-- R6 CMO semi-horário → horário (média das duas meias-horas), por subsistema
CREATE OR REPLACE TABLE silver_cmo_horario AS
SELECT upper(trim(id_subsistema)) AS id_subsistema,
       date_trunc('hour', CAST(din_instante AS TIMESTAMP)) AS instante,
       avg(CAST(val_cmo AS DOUBLE)) AS cmo_rs_mwh,
       count(*) AS n_meias_horas,
       min(CAST(val_cmo AS DOUBLE)) < 0 AS flag_cmo_negativo
FROM bronze_cmo GROUP BY ALL;
"""

SQL_GOLD = """
CREATE OR REPLACE TABLE dim_subsistema AS
SELECT * FROM (VALUES
    ('SE', 'Sudeste/Centro-Oeste', 'ES, MG, RJ, SP, GO, DF, MT, MS, AC, RO'),
    ('S',  'Sul',                  'PR, SC, RS'),
    ('NE', 'Nordeste',             'AL, BA, CE, PB, PE, PI, RN, SE'),
    ('N',  'Norte',                'AM, AP, MA, PA, TO, RR')
) t(id_subsistema, nome_subsistema, ufs_principais);

CREATE OR REPLACE TABLE dim_tempo AS
SELECT i AS instante,
       CAST(i AS DATE) AS data,
       hour(i) AS hora,
       year(i) AS ano,
       month(i) AS mes,
       strftime(i, '%Y-%m') AS ano_mes,
       isodow(i) AS dia_semana_num,
       CASE WHEN isodow(i) = 7 THEN 'domingo' WHEN isodow(i) = 6 THEN 'sábado' ELSE 'dia útil' END AS tipo_dia,
       CASE WHEN month(i) IN (11, 12, 1, 2, 3, 4) THEN 'úmido' ELSE 'seco' END AS periodo_hidrologico,
       CASE WHEN hour(i) BETWEEN 18 AND 20 THEN 'ponta' ELSE 'fora de ponta' END AS posto_tarifario,
       i >= TIMESTAMP '{mmgd}' AS periodo_com_mmgd
FROM range(TIMESTAMP '2021-01-01', TIMESTAMP '2026-01-01', INTERVAL 1 HOUR) r(i);

-- fato: grão = subsistema × hora
CREATE OR REPLACE TABLE fato_balanco_horario AS
SELECT b.id_subsistema, b.instante,
       b.ger_hidraulica_mw, b.ger_termica_mw, b.ger_eolica_mw, b.ger_solar_mw,
       b.ger_hidraulica_mw + b.ger_termica_mw + b.ger_eolica_mw + b.ger_solar_mw AS ger_total_mw,
       b.ger_eolica_mw + b.ger_solar_mw AS ger_variavel_mw,
       b.carga_mw,
       b.carga_mw - b.ger_eolica_mw - b.ger_solar_mw AS carga_liquida_mw,
       b.intercambio_mw,
       b.ger_hidraulica_mw + b.ger_termica_mw + b.ger_eolica_mw + b.ger_solar_mw - b.carga_mw - b.intercambio_mw AS residuo_balanco_mw,
       c.cmo_rs_mwh,
       b.flag_solar_noturno,
       CAST(b.instante AS DATE) IN (SELECT data FROM silver_dia_solar_ausente) AS flag_solar_ausente,
       coalesce(c.flag_cmo_negativo, false) AS flag_cmo_negativo,
       b.flag_solar_noturno OR CAST(b.instante AS DATE) IN (SELECT data FROM silver_dia_solar_ausente) AS flag_excluir_analise
FROM silver_balanco b
LEFT JOIN silver_cmo_horario c USING (id_subsistema, instante);
"""


def etl(con) -> None:
    con.execute(SQL_BRONZE.format(b=BRONZE.as_posix()))
    con.execute(SQL_SILVER)
    con.execute(SQL_GOLD.format(mmgd=DATA_MMGD))
    GOLD.mkdir(parents=True, exist_ok=True)
    for t in ["fato_balanco_horario", "dim_subsistema", "dim_tempo"]:
        con.execute(f"COPY {t} TO '{(GOLD / (t + '.parquet')).as_posix()}' (FORMAT parquet)")
    df = con.execute("""
        SELECT 'bronze_balanco' t, count(*) FROM bronze_balanco UNION ALL
        SELECT 'bronze_cmo', count(*) FROM bronze_cmo UNION ALL
        SELECT 'silver_balanco (sem SIN)', count(*) FROM silver_balanco UNION ALL
        SELECT 'silver_cmo_horario', count(*) FROM silver_cmo_horario UNION ALL
        SELECT 'silver_dia_solar_ausente', count(*) FROM silver_dia_solar_ausente UNION ALL
        SELECT 'fato_balanco_horario', count(*) FROM fato_balanco_horario UNION ALL
        SELECT 'dim_subsistema', count(*) FROM dim_subsistema UNION ALL
        SELECT 'dim_tempo', count(*) FROM dim_tempo""").df()
    df.columns = ["tabela", "linhas"]
    df.to_csv(RES / "contagem_camadas.csv", index=False)


# --------------------------------------------------------------------------- qualidade
def qualidade(con) -> None:
    import pandas as pd
    linhas = []
    for tabela, cols in [("bronze_balanco", ["id_subsistema", "nom_subsistema", "din_instante", "val_gerhidraulica",
                                              "val_gertermica", "val_gereolica", "val_gersolar", "val_carga", "val_intercambio"]),
                         ("bronze_cmo", ["id_subsistema", "nom_subsistema", "din_instante", "val_cmo"])]:
        total = con.execute(f"SELECT count(*) FROM {tabela}").fetchone()[0]
        for c in cols:
            numerico = c.startswith("val_")
            expr = f"TRY_CAST({c} AS DOUBLE)" if numerico else c
            nulos, dist, mn, mx, invalidos = con.execute(
                f"SELECT count(*) - count({c}), count(DISTINCT {c}), min({expr}), max({expr}), "
                f"count({c}) - count({expr}) FROM {tabela}").fetchone()
            linhas.append({"tabela": tabela, "atributo": c, "registros": total, "nulos": nulos, "distintos": dist,
                           "minimo": round(mn, 2) if numerico else mn, "maximo": round(mx, 2) if numerico else mx,
                           "nao_numericos": invalidos if numerico else None})
    pd.DataFrame(linhas).to_csv(RES / "qualidade_perfil_bronze.csv", index=False)

    con.execute("""
    CREATE OR REPLACE TABLE qualidade_regras AS
    SELECT 'Unicidade: (subsistema, instante) duplicado no balanço' AS regra,
           (SELECT count(*) - count(DISTINCT (id_subsistema, din_instante)) FROM bronze_balanco) AS ocorrencias
    UNION ALL SELECT 'Unicidade: (subsistema, instante) duplicado no CMO',
           (SELECT count(*) - count(DISTINCT (id_subsistema, din_instante)) FROM bronze_cmo)
    UNION ALL SELECT 'Completude: horas faltantes no balanço (esperado 4 subsistemas × 43.824 h)',
           (SELECT 4 * 43824 - count(*) FROM silver_balanco)
    UNION ALL SELECT 'Completude: horas sem CMO após a junção',
           (SELECT count(*) FILTER (WHERE cmo_rs_mwh IS NULL) FROM fato_balanco_horario)
    UNION ALL SELECT 'Completude: horas de CMO com menos de 2 meias-horas',
           (SELECT count(*) FILTER (WHERE n_meias_horas < 2) FROM silver_cmo_horario)
    UNION ALL SELECT 'Consistência: SIN ≠ soma dos subsistemas (carga, |dif| > 1 MW)',
           (SELECT count(*) FROM bronze_balanco s JOIN (
                SELECT din_instante, sum(CAST(val_carga AS DOUBLE)) c FROM bronze_balanco WHERE id_subsistema <> 'SIN' GROUP BY 1) x
              USING (din_instante)
            WHERE s.id_subsistema = 'SIN' AND abs(CAST(s.val_carga AS DOUBLE) - x.c) > 1)
    UNION ALL SELECT 'Consistência: |geração − carga − intercâmbio| > 500 MW',
           (SELECT count(*) FILTER (WHERE abs(residuo_balanco_mw) > 500) FROM fato_balanco_horario)
    UNION ALL SELECT 'Conformidade: valores negativos de geração ou carga',
           (SELECT count(*) FILTER (WHERE least(ger_hidraulica_mw, ger_termica_mw, ger_eolica_mw, ger_solar_mw, carga_mw) < 0) FROM fato_balanco_horario)
    UNION ALL SELECT 'Acurácia: solar > 100 MW entre 0h e 4h (R3)',
           (SELECT count(*) FILTER (WHERE flag_solar_noturno) FROM fato_balanco_horario)
    UNION ALL SELECT 'Acurácia: dias com solar ausente ao meio-dia (R5)',
           (SELECT count(*) FROM silver_dia_solar_ausente)
    UNION ALL SELECT 'Acurácia: horas com CMO negativo',
           (SELECT count(*) FILTER (WHERE flag_cmo_negativo) FROM fato_balanco_horario)
    UNION ALL SELECT 'Consistência temporal: quebra de nível da série solar em ' || '{mmgd}' || ' (R4)', 1
    UNION ALL SELECT 'Total de linhas excluídas das análises (flag_excluir_analise)',
           (SELECT count(*) FILTER (WHERE flag_excluir_analise) FROM fato_balanco_horario)
    """.replace("{mmgd}", DATA_MMGD))
    con.execute("SELECT * FROM qualidade_regras").df().to_csv(RES / "qualidade_regras.csv", index=False)
    con.execute("SELECT * FROM silver_dia_solar_ausente ORDER BY data").df().to_csv(RES / "qualidade_dias_solar_ausente.csv", index=False)

    con.execute("""
    SELECT id_subsistema, count(*) linhas,
           round(min(ger_hidraulica_mw)) hid_min, round(max(ger_hidraulica_mw)) hid_max,
           round(min(ger_termica_mw)) ter_min, round(max(ger_termica_mw)) ter_max,
           round(min(ger_eolica_mw)) eol_min, round(max(ger_eolica_mw)) eol_max,
           round(min(ger_solar_mw)) sol_min, round(max(ger_solar_mw)) sol_max,
           round(min(carga_mw)) carga_min, round(max(carga_mw)) carga_max,
           round(min(intercambio_mw)) int_min, round(max(intercambio_mw)) int_max,
           round(min(cmo_rs_mwh), 2) cmo_min, round(max(cmo_rs_mwh), 2) cmo_max
    FROM fato_balanco_horario GROUP BY 1 ORDER BY 1""").df().to_csv(RES / "qualidade_dominios_subsistema.csv", index=False)

    # quebra de nível: média diária SIN de solar e carga 14 dias antes/depois
    con.execute(f"""
    WITH d AS (SELECT CAST(instante AS DATE) data, sum(ger_solar_mw)/24 solar_med, sum(carga_mw)/24 carga_med,
                      sum(carga_liquida_mw)/24 cl_med
               FROM fato_balanco_horario GROUP BY 1)
    SELECT CASE WHEN data < DATE '{DATA_MMGD}' THEN 'antes (14 dias)' ELSE 'depois (14 dias)' END janela,
           round(avg(solar_med)) solar_medio_mw, round(avg(carga_med)) carga_media_mw, round(avg(cl_med)) carga_liquida_media_mw
    FROM d WHERE data BETWEEN DATE '{DATA_MMGD}' - 14 AND DATE '{DATA_MMGD}' + 13 GROUP BY 1 ORDER BY 1""").df() \
        .to_csv(RES / "qualidade_quebra_mmgd.csv", index=False)


# --------------------------------------------------------------------------- análise
BASE = """
-- visão SIN: soma dos 4 subsistemas por hora, sem horas marcadas para exclusão
CREATE OR REPLACE VIEW v_sin AS
SELECT t.*, f.*
FROM (SELECT instante,
             sum(ger_hidraulica_mw) hid, sum(ger_termica_mw) ter, sum(ger_eolica_mw) eol, sum(ger_solar_mw) sol,
             sum(ger_total_mw) ger, sum(carga_mw) carga, sum(carga_liquida_mw) cl,
             bool_or(flag_excluir_analise) excluir
      FROM fato_balanco_horario GROUP BY 1) f
JOIN dim_tempo t USING (instante);

CREATE OR REPLACE VIEW v_sub AS
SELECT f.*, t.ano, t.mes, t.hora, t.data, t.periodo_hidrologico, s.nome_subsistema
FROM fato_balanco_horario f JOIN dim_tempo t USING (instante) JOIN dim_subsistema s USING (id_subsistema);
"""


def salvar(df, nome):
    df.to_csv(RES / f"{nome}.csv", index=False)
    return df


def estilo(ax, titulo, xl=None, yl=None):
    ax.set_title(titulo, fontsize=12, loc="left", fontweight="bold")
    if xl:
        ax.set_xlabel(xl)
    if yl:
        ax.set_ylabel(yl)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", alpha=.3)
    ax.set_axisbelow(True)


def analise(con) -> None:
    con.execute(BASE)
    GRAF.mkdir(parents=True, exist_ok=True)

    # P1 — participação por fonte, subsistema e ano
    p1 = salvar(con.execute("""
        SELECT ano, id_subsistema, nome_subsistema,
               round(sum(ger_total_mw)/1e3/count(DISTINCT data)*24/24, 0) ger_media_gw_x1000,
               round(100*sum(ger_hidraulica_mw)/sum(ger_total_mw),1) pct_hidraulica,
               round(100*sum(ger_termica_mw)/sum(ger_total_mw),1) pct_termica,
               round(100*sum(ger_eolica_mw)/sum(ger_total_mw),1) pct_eolica,
               round(100*sum(ger_solar_mw)/sum(ger_total_mw),1) pct_solar,
               round(100*sum(ger_variavel_mw)/sum(ger_total_mw),1) pct_eolica_solar
        FROM v_sub GROUP BY ALL ORDER BY ano, id_subsistema""").df().drop(columns="ger_media_gw_x1000"), "p1_participacao_subsistema")
    p1s = salvar(con.execute("""
        SELECT ano, round(avg(ger)/1000,1) ger_media_gw,
               round(100*sum(hid)/sum(ger),1) pct_hidraulica, round(100*sum(ter)/sum(ger),1) pct_termica,
               round(100*sum(eol)/sum(ger),1) pct_eolica, round(100*sum(sol)/sum(ger),1) pct_solar,
               round(100*(sum(eol)+sum(sol))/sum(ger),1) pct_eolica_solar
        FROM v_sin GROUP BY 1 ORDER BY 1""").df(), "p1_participacao_sin")
    fig, ax = plt.subplots(figsize=(9, 4.5))
    base = [0] * len(p1s)
    for col, cor, rot in [("pct_hidraulica", "#5b9bd5", "hidráulica"), ("pct_termica", "#7f7f7f", "térmica"),
                          ("pct_eolica", "#70ad47", "eólica"), ("pct_solar", "#ffc000", "solar")]:
        ax.bar(p1s.ano.astype(str), p1s[col], bottom=base, color=cor, label=rot)
        for i, (b, v) in enumerate(zip(base, p1s[col])):
            if v >= 4:
                ax.text(i, b + v / 2, f"{v:.0f}%", ha="center", va="center", fontsize=9, color="white" if col != "pct_solar" else "black")
        base = [b + v for b, v in zip(base, p1s[col])]
    estilo(ax, "P1 — Matriz de geração do SIN por ano (% da energia gerada)", None, "% da geração")
    ax.axvline(1.5 + .5, color="black", ls=":", lw=1)
    ax.text(2.05, 104, "solar passa a incluir MMGD (29/04/2023)", fontsize=8)
    ax.legend(frameon=False, ncol=4, loc="upper center", bbox_to_anchor=(.5, -.08))
    ax.set_ylim(0, 110)
    fig.tight_layout(); fig.savefig(GRAF / "p1_matriz_sin.png", dpi=150); plt.close(fig)

    # P2 — curva de carga líquida
    p2 = salvar(con.execute("""
        SELECT ano, hora, round(avg(carga)/1000,2) carga_gw, round(avg(cl)/1000,2) carga_liquida_gw,
               round(avg(eol+sol)/1000,2) eolica_solar_gw
        FROM v_sin WHERE NOT excluir GROUP BY ALL ORDER BY ALL""").df(), "p2_curva_carga_liquida")
    p2r = salvar(con.execute("""
        WITH c AS (SELECT ano, hora, avg(cl) cl, avg(carga) carga FROM v_sin WHERE NOT excluir GROUP BY ALL)
        SELECT ano, arg_min(hora, cl) hora_vale_cl, round(min(cl)/1000,1) vale_cl_gw,
               arg_max(hora, cl) hora_pico_cl, round(max(cl)/1000,1) pico_cl_gw,
               round((max(cl)-min(cl))/1000,1) amplitude_cl_gw, round(100*min(cl)/max(cl),1) vale_sobre_pico_pct,
               arg_min(hora, carga) hora_vale_carga, arg_max(hora, carga) hora_pico_carga
        FROM c GROUP BY 1 ORDER BY 1""").df(), "p2_resumo")
    fig, ax = plt.subplots(figsize=(10, 4.8))
    for ano in ANOS:
        s = p2[p2.ano == ano]
        ax.plot(s.hora, s.carga_liquida_gw, color=CORES[ano], lw=2.2 if ano in (2021, 2025) else 1.4, label=str(ano), marker="o", ms=3)
    estilo(ax, "P2 — Carga líquida média do SIN por hora (carga − eólica − solar)", "hora do dia", "GW médios")
    ax.set_xticks(range(24)); ax.legend(frameon=False, ncol=5)
    fig.tight_layout(); fig.savefig(GRAF / "p2_curva_pato.png", dpi=150); plt.close(fig)

    # P3 — rampas
    con.execute("""
        CREATE OR REPLACE TABLE rampa AS
        SELECT instante, ano, hora, data, cl - lag(cl) OVER (ORDER BY instante) AS rampa_mw,
               excluir OR lag(excluir) OVER (ORDER BY instante) AS excl
        FROM v_sin""")
    p3 = salvar(con.execute("""
        WITH d AS (SELECT data, ano, max(rampa_mw) rampa_max, arg_max(hora, rampa_mw) hora_rampa_max
                   FROM rampa WHERE NOT excl AND rampa_mw IS NOT NULL GROUP BY ALL)
        SELECT ano, round(avg(rampa_max)) media_rampa_max_diaria_mw, round(quantile_cont(rampa_max, .95)) p95_rampa_max_diaria_mw,
               round(max(rampa_max)) maior_rampa_ano_mw, mode(hora_rampa_max) hora_mais_frequente_rampa_max,
               round(100*avg((hora_rampa_max BETWEEN 16 AND 19)::INT),1) pct_dias_rampa_max_16h_19h
        FROM d GROUP BY 1 ORDER BY 1""").df(), "p3_rampas")
    p3h = salvar(con.execute("""
        SELECT ano, hora, round(avg(rampa_mw)) rampa_media_mw FROM rampa WHERE NOT excl AND rampa_mw IS NOT NULL
        GROUP BY ALL ORDER BY ALL""").df(), "p3_rampa_por_hora")
    fig, ax = plt.subplots(figsize=(10, 4.5))
    for ano in ANOS:
        s = p3h[p3h.ano == ano]
        ax.plot(s.hora, s.rampa_media_mw / 1000, color=CORES[ano], lw=2.2 if ano in (2021, 2025) else 1.4, label=str(ano))
    ax.axhline(0, color="black", lw=.8)
    estilo(ax, "P3 — Rampa horária média da carga líquida do SIN (variação em relação à hora anterior)", "hora do dia", "GW/h")
    ax.set_xticks(range(24)); ax.legend(frameon=False, ncol=5)
    fig.tight_layout(); fig.savefig(GRAF / "p3_rampas.png", dpi=150); plt.close(fig)

    # P4 — Nordeste
    p4 = salvar(con.execute("""
        SELECT ano, count(*) horas,
               round(100*avg((ger_variavel_mw >= carga_mw)::INT),1) pct_horas_eol_sol_supera_carga,
               round(100*avg((intercambio_mw > 0)::INT),1) pct_horas_exportando,
               round(avg(intercambio_mw)) intercambio_medio_mw,
               round(max(intercambio_mw)) exportacao_maxima_mw,
               round(100*sum(ger_variavel_mw)/sum(carga_mw),1) eol_sol_sobre_carga_pct
        FROM v_sub WHERE id_subsistema = 'NE' AND NOT flag_excluir_analise GROUP BY 1 ORDER BY 1""").df(), "p4_nordeste")
    p4h = salvar(con.execute("""
        SELECT ano, hora, round(avg(intercambio_mw)) intercambio_medio_mw
        FROM v_sub WHERE id_subsistema = 'NE' AND NOT flag_excluir_analise GROUP BY ALL ORDER BY ALL""").df(), "p4_nordeste_por_hora")
    fig, axs = plt.subplots(1, 2, figsize=(12, 4.3))
    axs[0].bar(p4.ano.astype(str), p4.pct_horas_eol_sol_supera_carga, color="#70ad47")
    for i, v in enumerate(p4.pct_horas_eol_sol_supera_carga):
        axs[0].text(i, v + .8, f"{v:.0f}%", ha="center", fontsize=9)
    estilo(axs[0], "P4a — % das horas em que eólica+solar\ndo NE ≥ carga do NE", None, "% das horas")
    for ano in ANOS:
        s = p4h[p4h.ano == ano]
        axs[1].plot(s.hora, s.intercambio_medio_mw / 1000, color=CORES[ano], label=str(ano), lw=2.2 if ano in (2021, 2025) else 1.4)
    axs[1].axhline(0, color="black", lw=.8)
    estilo(axs[1], "P4b — Intercâmbio médio do NE por hora\n(+ = exportação)", "hora do dia", "GW")
    axs[1].legend(frameon=False, ncol=3, fontsize=8); axs[1].set_xticks(range(0, 24, 2))
    fig.tight_layout(); fig.savefig(GRAF / "p4_nordeste.png", dpi=150); plt.close(fig)

    # P5 — quem atende a rampa noturna (16h–19h)
    salvar(con.execute("""
        WITH d AS (SELECT instante, ano, hora,
                          cl  - lag(cl)  OVER w AS d_cl,  hid - lag(hid) OVER w AS d_hid,
                          ter - lag(ter) OVER w AS d_ter, excluir OR lag(excluir) OVER w AS excl
                   FROM v_sin WINDOW w AS (ORDER BY instante))
        SELECT ano, round(avg(d_cl)) rampa_media_cl_mw,
               round(100*sum(d_hid)/sum(d_cl),1) pct_atendida_hidraulica,
               round(100*sum(d_ter)/sum(d_cl),1) pct_atendida_termica,
               round(100 - 100*(sum(d_hid)+sum(d_ter))/sum(d_cl),1) pct_outros_intercambio_intl,
               round(corr(d_hid, d_cl),3) corr_hidraulica, round(corr(d_ter, d_cl),3) corr_termica
        FROM d WHERE NOT excl AND hora BETWEEN 16 AND 19 AND d_cl > 0 GROUP BY 1 ORDER BY 1""").df(), "p5_atendimento_rampa")
    p5p = salvar(con.execute("""
        SELECT ano, hora, round(avg(hid)/1000,2) hidraulica_gw, round(avg(ter)/1000,2) termica_gw
        FROM v_sin WHERE NOT excluir AND ano IN (2021, 2025) GROUP BY ALL ORDER BY ALL""").df(), "p5_perfil_despachaveis")
    fig, ax = plt.subplots(figsize=(10, 4.5))
    for ano, ls in [(2021, "--"), (2025, "-")]:
        s = p5p[p5p.ano == ano]
        ax.plot(s.hora, s.hidraulica_gw, color="#5b9bd5", ls=ls, lw=2, label=f"hidráulica {ano}")
        ax.plot(s.hora, s.termica_gw, color="#7f7f7f", ls=ls, lw=2, label=f"térmica {ano}")
    estilo(ax, "P5 — Perfil horário médio da geração despachável no SIN (2021 × 2025)", "hora do dia", "GW médios")
    ax.set_xticks(range(24)); ax.legend(frameon=False, ncol=2)
    fig.tight_layout(); fig.savefig(GRAF / "p5_despachaveis.png", dpi=150); plt.close(fig)

    # P6 — CMO × carga líquida
    salvar(con.execute("""
        WITH b AS (SELECT s.instante, s.ano, s.hora, s.cl, f.cmo_rs_mwh cmo
                   FROM v_sin s JOIN fato_balanco_horario f ON f.instante = s.instante AND f.id_subsistema = 'SE'
                   WHERE NOT s.excluir AND f.cmo_rs_mwh IS NOT NULL),
        q AS (SELECT *, ntile(5) OVER (PARTITION BY ano ORDER BY cl) quintil FROM b)
        SELECT ano, quintil, round(min(cl)/1000,1) cl_min_gw, round(max(cl)/1000,1) cl_max_gw,
               round(avg(cmo),1) cmo_medio_se, round(median(cmo),1) cmo_mediano_se
        FROM q GROUP BY ALL ORDER BY ALL""").df(), "p6_cmo_por_quintil")
    salvar(con.execute("""
        WITH b AS (SELECT s.ano, s.cl, f.cmo_rs_mwh cmo,
                          rank() OVER (PARTITION BY s.ano ORDER BY s.cl) rc, rank() OVER (PARTITION BY s.ano ORDER BY f.cmo_rs_mwh) rm
                   FROM v_sin s JOIN fato_balanco_horario f ON f.instante = s.instante AND f.id_subsistema = 'SE'
                   WHERE NOT s.excluir AND f.cmo_rs_mwh IS NOT NULL)
        SELECT ano, round(corr(cl, cmo),3) pearson, round(corr(rc, rm),3) spearman, round(avg(cmo),1) cmo_medio_se FROM b GROUP BY 1 ORDER BY 1""").df(),
          "p6_correlacao")
    p6h = salvar(con.execute("""
        SELECT ano, hora, round(avg(cmo_rs_mwh),1) cmo_medio_se FROM v_sub
        WHERE id_subsistema='SE' AND NOT flag_excluir_analise AND cmo_rs_mwh IS NOT NULL GROUP BY ALL ORDER BY ALL""").df(), "p6_cmo_por_hora")
    salvar(con.execute("""
        SELECT ano, id_subsistema, round(avg(cmo_rs_mwh),1) cmo_medio, round(median(cmo_rs_mwh),1) cmo_mediano
        FROM v_sub WHERE cmo_rs_mwh IS NOT NULL GROUP BY ALL ORDER BY ALL""").df(), "p6_cmo_subsistema")
    fig, ax = plt.subplots(figsize=(10, 4.5))
    for ano in ANOS:
        s = p6h[p6h.ano == ano]
        ax.plot(s.hora, s.cmo_medio_se, color=CORES[ano], label=str(ano), lw=2.2 if ano in (2021, 2025) else 1.4)
    estilo(ax, "P6 — CMO médio do Sudeste/Centro-Oeste por hora do dia", "hora do dia", "R$/MWh")
    ax.set_xticks(range(24)); ax.legend(frameon=False, ncol=5)
    fig.tight_layout(); fig.savefig(GRAF / "p6_cmo_hora.png", dpi=150); plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sem-coleta", action="store_true")
    args = ap.parse_args()
    RES.mkdir(exist_ok=True)
    if not args.sem_coleta:
        coletar()
    con = duckdb.connect()
    etl(con)
    qualidade(con)
    analise(con)
    print("Pipeline concluído. Resultados em", RES)


if __name__ == "__main__":
    main()
