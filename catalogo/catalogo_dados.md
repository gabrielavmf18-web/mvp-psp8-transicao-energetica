# Catálogo de Dados — MVP PSP 8 (Transição energética no SIN, ONS 2021–2025)

Schema no Databricks: `workspace.mvp_energia` · Formato: Delta Lake · Modelo: **esquema estrela** (ver [modelo_estrela.md](modelo_estrela.md)).

Os domínios numéricos foram observados na carga de 2021–2025 e servem como **faixa esperada** para cargas futuras. Unidades: potência média horária em **MW** (equivale a MWh na hora); CMO em **R$/MWh**.

## Linhagem geral

```
ONS · Balanço de Energia nos Subsistemas (horário, 5 CSV anuais) ─► Volume raw/balanco ─► bronze_balanco ─► silver_balanco ─┐
ONS · CMO semi-horário (5 CSV anuais)                           ─► Volume raw/cmo     ─► bronze_cmo     ─► silver_cmo_horario ─┼─► fato_balanco_horario
                                                                                               silver_dia_solar_ausente ─┘     + dim_subsistema + dim_tempo
```

| Fonte | URL | Coleta | Volume | Formato |
|---|---|---|---|---|
| ONS — Balanço de Energia nos Subsistemas | https://dados.ons.org.br/dataset/balanco-energia-subsistema (arquivos em `ons-aws-prod-opendata.s3.amazonaws.com/dataset/balanco_energia_subsistema_ho/`) | 21/10/2026 | 5 arquivos · 23,4 MB · 219.120 linhas | CSV `;`, UTF-8, ponto decimal |
| ONS — CMO semi-horário | https://dados.ons.org.br/dataset/cmo-semi-horario (`…/dataset/cmo_tm/`) | 21/10/2026 | 5 arquivos · 12,7 MB · 343.488 linhas | CSV `;`, UTF-8 |

Licença de ambos: **Creative Commons Atribuição (CC-BY 4.0)**, conforme metadados do portal de dados abertos do ONS.

---

## `fato_balanco_horario` — grão: subsistema × hora (175.296 linhas = 4 × 43.824 h)

| Atributo | Tipo | Descrição | Domínio esperado (observado) | Obrigatório / nulo | Linhagem |
|---|---|---|---|---|---|
| `id_subsistema` | categórico (FK) | Subsistema do SIN | SE, S, NE, N | Sim | `id_subsistema` (linha `SIN` removida — R2) |
| `instante` | data-hora (FK) | Início da hora de referência | 2021-01-01 00:00 a 2025-12-31 23:00 | Sim | `din_instante` |
| `ger_hidraulica_mw` | numérico | Geração hidráulica verificada | ≥ 0 (SE 12.291–48.893; S 603–15.835; NE 1.187–8.904; N 275–19.533) | Sim | `val_gerhidraulica` |
| `ger_termica_mw` | numérico | Geração térmica (inclui nuclear) | ≥ 0 (SE 2.194–12.525; S 19–2.520; NE 135–5.264; N 156–3.589) | Sim | `val_gertermica` |
| `ger_eolica_mw` | numérico | Geração eólica | ≥ 0 (NE 395–22.681; S 14–2.065; N 0–424; SE 0–265) | Sim | `val_gereolica` |
| `ger_solar_mw` | numérico | Geração solar; **a partir de 29/04/2023 inclui a estimativa de MMGD** | ≥ 0; noturno ≈ 0 (SE 0–21.934; NE 0–13.835; S 0–6.951; N 0–2.439) | Sim | `val_gersolar` |
| `ger_total_mw` | numérico | Soma das quatro fontes | > 0 | Sim | Derivado |
| `ger_variavel_mw` | numérico | Eólica + solar (renovável variável) | ≥ 0 | Sim | Derivado |
| `carga_mw` | numérico | Carga verificada do subsistema (com MMGD a partir de 29/04/2023) | > 0 (SE 26.260–62.150; S 5.756–22.737; NE 7.262–18.157; N 1.364–10.239) | Sim | `val_carga` |
| `carga_liquida_mw` | numérico | Carga − eólica − solar: o que precisa ser atendido por fontes despacháveis ou intercâmbio | pode ser negativo no NE | Sim | Derivado |
| `intercambio_mw` | numérico | Saldo de intercâmbio; **positivo = exportação** do subsistema | −16.172 a 14.147 | Sim | `val_intercambio` |
| `residuo_balanco_mw` | numérico | Geração − carga − intercâmbio (deve ser ≈ 0) | \|x\| ≤ 500 (24 h fora, S, 14/09/2022) | Sim | Derivado (verificação) |
| `cmo_rs_mwh` | numérico | Custo Marginal de Operação médio na hora | −39 a 5.793 (picos raros; 2023 ≈ 0) | **Nulo = dia sem CMO publicado** (37 dias, 3.552 linhas) | Média das 2 meias-horas do CMO semi-horário (R6, R7) |
| `flag_solar_noturno` | booleano | Solar > 100 MW entre 0 h e 4 h (R3) | true em 252 linhas (52 dias) | Sim | Derivado |
| `flag_solar_ausente` | booleano | Dia com solar ausente ao meio-dia (R5) | nenhum caso em 2021–2025 | Sim | Derivado |
| `flag_cmo_negativo` | booleano | Alguma meia-hora com CMO < 0 | true em 292 linhas | Sim | Derivado |
| `flag_excluir_analise` | booleano | Solar noturno ou solar ausente; linha fora dos perfis horários | true em 252 linhas | Sim | Derivado |

## `dim_subsistema` (4 linhas)

| Atributo | Tipo | Descrição | Domínio | Obrigatório | Linhagem |
|---|---|---|---|---|---|
| `id_subsistema` | categórico (PK) | Código ONS | SE, S, NE, N | Sim | ONS |
| `nome_subsistema` | texto | Nome | Sudeste/Centro-Oeste, Sul, Nordeste, Norte | Sim | ONS (`nom_subsistema`, normalizado) |
| `ufs_principais` | texto | UFs atendidas majoritariamente | lista de UFs | Sim | Mapa de subsistemas do ONS (simplificado) |

## `dim_tempo` (43.824 linhas)

| Atributo | Tipo | Descrição | Domínio | Obrigatório | Linhagem |
|---|---|---|---|---|---|
| `instante` | data-hora (PK) | Hora | 2021-01-01 00:00 a 2025-12-31 23:00 | Sim | Gerada (sequence) |
| `data` | data | Dia | 2021-01-01 a 2025-12-31 | Sim | Derivado |
| `hora` | numérico | Hora do dia | 0–23 | Sim | Derivado |
| `ano` / `mes` | numérico | Ano / mês | 2021–2025 / 1–12 | Sim | Derivado |
| `ano_mes` | texto | AAAA-MM | — | Sim | Derivado |
| `dia_semana_num` | numérico | Dia ISO | 1 (seg) – 7 (dom) | Sim | Derivado |
| `tipo_dia` | categórico | Tipo de dia | dia útil, sábado, domingo | Sim | Derivado |
| `periodo_hidrologico` | categórico | Estação hidrológica do SIN | úmido (nov–abr), seco (mai–out) | Sim | Convenção do setor |
| `posto_tarifario` | categórico | Posto horário de referência | ponta (18 h–20 h), fora de ponta | Sim | Convenção simplificada |
| `periodo_com_mmgd` | booleano | Instante ≥ 29/04/2023 (série solar e carga com MMGD) | true/false | Sim | Quebra detectada (R4) |

---

## Camadas intermediárias

| Tabela | Conteúdo | Linhas |
|---|---|---|
| `bronze_balanco` | Balanço bruto (5 subsistemas, inclusive a linha SIN), texto | 219.120 |
| `bronze_cmo` | CMO bruto semi-horário, texto | 343.488 |
| `silver_balanco` | Balanço tipado, sem SIN, com marcações R3/R4 | 175.296 |
| `silver_cmo_horario` | CMO médio horário por subsistema | 171.744 |
| `silver_dia_solar_ausente` | Dias com solar ausente (R5) | 0 |

### Atributos do bruto

| Arquivo | Coluna | Tipo | Descrição |
|---|---|---|---|
| Balanço | `id_subsistema` | texto | N, NE, S, SE, SIN |
| Balanço | `nom_subsistema` | texto | NORTE, NORDESTE, SUL, SUDESTE/CENTRO-OESTE, SISTEMA INTERLIGADO NACIONAL |
| Balanço | `din_instante` | data-hora | AAAA-MM-DD HH:MM:SS |
| Balanço | `val_gerhidraulica`, `val_gertermica`, `val_gereolica`, `val_gersolar`, `val_carga`, `val_intercambio` | decimal | MW médios na hora |
| CMO | `id_subsistema`, `nom_subsistema` | texto | N, NE, S, SE |
| CMO | `din_instante` | data-hora | início da meia-hora |
| CMO | `val_cmo` | decimal | R$/MWh |
