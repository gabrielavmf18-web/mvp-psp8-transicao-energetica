# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Coleta
# MAGIC Baixa do bucket público de Dados Abertos do ONS (AWS S3) para o volume `/Volumes/workspace/mvp_energia/raw`:
# MAGIC
# MAGIC | Conjunto | Arquivos | Granularidade |
# MAGIC |---|---|---|
# MAGIC | Balanço de Energia nos Subsistemas | `BALANCO_ENERGIA_SUBSISTEMA_2021…2025.csv` | horária, por subsistema + SIN |
# MAGIC | Custo Marginal de Operação (CMO) semi-horário | `CMO_SEMIHORARIO_2021…2025.csv` | 30 min, por subsistema |
# MAGIC
# MAGIC Nenhuma transformação é feita na coleta: os arquivos são gravados byte a byte.
# MAGIC
# MAGIC > Se o cluster não tiver saída para a internet, rode `scripts/coletar.py` localmente e envie os arquivos por
# MAGIC > **Catalog › workspace › mvp_energia › raw › Upload to this volume** (pastas `balanco/` e `cmo/`).

# COMMAND ----------

import datetime as dt
import json
import os
import urllib.request

RAW = "/Volumes/workspace/mvp_energia/raw"
S3 = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset"
FONTES = {
    "balanco": S3 + "/balanco_energia_subsistema_ho/BALANCO_ENERGIA_SUBSISTEMA_{ano}.csv",
    "cmo": S3 + "/cmo_tm/CMO_SEMIHORARIO_{ano}.csv",
}

log = []
for pasta, modelo in FONTES.items():
    os.makedirs(f"{RAW}/{pasta}", exist_ok=True)
    for ano in range(2021, 2026):
        url = modelo.format(ano=ano)
        destino = f"{RAW}/{pasta}/{url.rsplit('/', 1)[1]}"
        with urllib.request.urlopen(url, timeout=120) as r:
            conteudo = r.read()
            modificado = r.headers.get("Last-Modified")
        with open(destino, "wb") as f:
            f.write(conteudo)
        log.append({"url": url, "destino": destino, "bytes": len(conteudo), "fonte_modificada_em": modificado,
                    "coletado_em": dt.datetime.now().isoformat(timespec="seconds")})

with open(f"{RAW}/log_coleta.json", "w", encoding="utf-8") as f:
    json.dump(log, f, ensure_ascii=False, indent=2)
display(spark.createDataFrame(log))

# COMMAND ----------

# MAGIC %md ### Evidência de persistência: arquivos gravados no volume

# COMMAND ----------

display(dbutils.fs.ls(f"{RAW}/balanco"))
display(dbutils.fs.ls(f"{RAW}/cmo"))
