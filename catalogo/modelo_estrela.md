# Modelo de dados — esquema estrela

```mermaid
erDiagram
    dim_tempo      ||--o{ fato_balanco_horario : instante
    dim_subsistema ||--o{ fato_balanco_horario : id_subsistema

    fato_balanco_horario {
        string    id_subsistema PK, FK
        timestamp instante PK, FK
        double    ger_hidraulica_mw
        double    ger_termica_mw
        double    ger_eolica_mw
        double    ger_solar_mw
        double    ger_total_mw
        double    ger_variavel_mw
        double    carga_mw
        double    carga_liquida_mw
        double    intercambio_mw
        double    residuo_balanco_mw
        double    cmo_rs_mwh
        boolean   flag_solar_noturno
        boolean   flag_solar_ausente
        boolean   flag_cmo_negativo
        boolean   flag_excluir_analise
    }
    dim_subsistema {
        string id_subsistema PK
        string nome_subsistema
        string ufs_principais
    }
    dim_tempo {
        timestamp instante PK
        date      data
        int       hora
        int       ano
        int       mes
        string    ano_mes
        int       dia_semana_num
        string    tipo_dia
        string    periodo_hidrologico
        string    posto_tarifario
        boolean   periodo_com_mmgd
    }
```

**Grão.** Uma linha por subsistema e hora. A chave é composta por (`id_subsistema`, `instante`).

**Conciliação de duas fontes.** O balanço é horário e o CMO é semi-horário. O CMO foi agregado para a hora pela média das duas meias-horas (`silver_cmo_horario`) antes da junção, para que ambos fiquem no mesmo grão. A junção é `LEFT`: horas sem CMO publicado continuam na fato com `cmo_rs_mwh` nulo.

**Por que a linha "SIN" foi removida.** No arquivo do ONS, o SIN é a soma dos quatro subsistemas; mantê-lo na fato duplicaria toda a energia em qualquer agregação. O SIN é recalculado por soma na view `v_sin`, e a identidade SIN = Σ subsistemas é verificada na etapa de qualidade.

**Por que fato larga, e não uma linha por fonte.** As perguntas comparam fontes na mesma hora (ex.: Δhidráulica × Δcarga líquida). Com as fontes em colunas, essas comparações não precisam de *pivot*. Uma `dim_fonte` com a fato em formato longo seria a evolução natural se entrassem mais fontes (nuclear separada, biomassa, importação).
