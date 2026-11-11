"""
Coleta fora do Databricks (plano B, caso o cluster não tenha saída para a internet).

Baixa do bucket público do ONS os arquivos do Balanço de Energia nos Subsistemas e do CMO semi-horário
(2021–2025) para dados/para_upload/{balanco,cmo}/, prontos para envio ao volume
/Volumes/workspace/mvp_energia/raw pela interface do Databricks.

    python scripts/coletar.py
"""
import pathlib
import urllib.request

RAIZ = pathlib.Path(__file__).resolve().parents[1]
DESTINO = RAIZ / "dados" / "para_upload"
S3 = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset"
FONTES = {
    "balanco": S3 + "/balanco_energia_subsistema_ho/BALANCO_ENERGIA_SUBSISTEMA_{ano}.csv",
    "cmo": S3 + "/cmo_tm/CMO_SEMIHORARIO_{ano}.csv",
}


def main():
    for pasta, modelo in FONTES.items():
        (DESTINO / pasta).mkdir(parents=True, exist_ok=True)
        for ano in range(2021, 2026):
            url = modelo.format(ano=ano)
            destino = DESTINO / pasta / url.rsplit("/", 1)[1]
            urllib.request.urlretrieve(url, destino)
            print(destino.name, destino.stat().st_size, "bytes")
    print("Arquivos prontos em", DESTINO)


if __name__ == "__main__":
    main()
