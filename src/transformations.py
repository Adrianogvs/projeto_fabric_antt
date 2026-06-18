import logging
from typing import Dict, List

import duckdb
import pandas as pd

log = logging.getLogger(__name__)

CONCESSAO_MAP: Dict[str, str] = {
    "aco":                  "RODOVIA DO AÇO",
    "af":                   "AUTOPISTA FLUMINENSE",
    "afd":                  "AUTOPISTA FERNÃO DIAS",
    "als":                  "AUTOPISTA LITORAL SUL",
    "aps":                  "AUTOPISTA PLANALTO SUL",
    "arb":                  "AUTOPISTA REGIS BITTENCOURT",
    "concebra":             "CONCEBRA",
    "concer":               "CONCER",
    "cro":                  "CRO",
    "eco050":               "ECO050",
    "eco101":               "ECO101",
    "ecoponte":             "ECOPONTE",
    "ecoriominas":          "ECORIOMINAS",
    "ecosul":               "ECOSUL",
    "ecoviasaraguaia":      "ECOVIAS DO ARAGUAIA",
    "ecoviascapixaba":      "ECOVIAS CAPIXABA",
    "ecoviasdocerrado":     "ECOVIAS DO CERRADO",
    "elovias":              "ELOVIAS",
    "epr_iguacu":           "EPR IGUACU",
    "epr_litoral_pioneiro": "EPR LITORAL PIONEIRO",
    "nova_381":             "NOVA 381",
    "pantanal":             "PANTANAL",
    "prvias":               "PRVIAS",
    "riosp":                "RIOSP",
    "rota-verde-goias":     "ROTA VERDE GOIÁS",
    "trans":                "TRANSBRASILIANA",
    "via040":               "VIA040",
    "viaaraucaria":         "VIA ARAUCARIA",
    "viabahia":             "VIABAHIA",
    "viabrasil":            "VIABRASIL",
    "viacosteira":          "VIACOSTEIRA",
    "viacristais":          "VIACRISTAIS",
    "viamineira":           "VIA MINEIRA",
    "viasul":               "VIASUL",
    "way_262":              "WAY 262",
}

COLS_VEICULOS: List[str] = [
    "automovel", "bicicleta", "caminhao", "moto", "onibus",
    "outros", "tracao_animal", "transporte_de_cargas_especiais",
    "trator_maquinas", "utilitarios",
]

COLS_VITIMAS: List[str] = [
    "ilesos", "levemente_feridos", "moderadamente_feridos",
    "gravemente_feridos", "mortos",
]


def mapear_concessionaria(df: pd.DataFrame, mapa: Dict[str, str]) -> pd.DataFrame:
    result = df.copy()
    result["_file_key"] = result["_filename"].str.extract(
        r"demostrativo_acidentes_([^/.]+)\.csv", expand=False
    )
    result["concessionaria"] = result["_file_key"].map(mapa).fillna("DESCONHECIDA")

    desconhecidas = (result["concessionaria"] == "DESCONHECIDA").sum()
    if desconhecidas > 0:
        log.warning("%d registros com concessionaria DESCONHECIDA", desconhecidas)

    return result.drop(columns=["_filename", "_file_key"])


def transformar(df: pd.DataFrame) -> pd.DataFrame:
    soma_veiculos = " + ".join(
        [f"COALESCE(TRY_CAST({c} AS INTEGER), 0)" for c in COLS_VEICULOS]
    )
    soma_vitimas = " + ".join(
        [f"COALESCE(TRY_CAST({c} AS INTEGER), 0)" for c in COLS_VITIMAS]
    )

    con = duckdb.connect()
    con.register("df", df)

    return con.execute(f"""
        SELECT
            TRY_STRPTIME(data, '%d/%m/%Y')::DATE                                        AS data,
            TRY_CAST(split_part(horario, ':', 1) AS INTEGER)                            AS hora,
            TRY_CAST(replace(km, ',', '.') AS DOUBLE)                                   AS km,
            CASE
                WHEN upper(trim(sentido)) IN ('NORTE', 'N', 'PISTA NORTE', 'CRESCENTE') THEN 'Norte'
                WHEN upper(trim(sentido)) IN ('SUL', 'S', 'PISTA SUL', 'DECRESCENTE')   THEN 'Sul'
                ELSE 'Indefinido'
            END                                                                          AS sentido,
            CASE
                WHEN regexp_matches(lower(tipo_de_ocorrencia), 'sem|s.vitima|ac01|ac02')   THEN 'Sem Vítima'
                WHEN regexp_matches(lower(tipo_de_ocorrencia), 'com|c.vitima|atropel|ac03') THEN 'Com Vítima'
                ELSE 'Indefinido'
            END                                                                          AS tipo_de_ocorrencia,
            trim(split_part(trecho, '/', 1))                                            AS rodovia,
            trim(split_part(trecho, '/', 2))                                            AS uf,
            {soma_veiculos}                                                              AS total_veiculos,
            {soma_vitimas}                                                               AS total_vitimas,
            CASE
                WHEN TRY_CAST(mortos AS INTEGER) > 0                 THEN 'Fatal'
                WHEN TRY_CAST(gravemente_feridos AS INTEGER) > 0     THEN 'Grave'
                WHEN TRY_CAST(moderadamente_feridos AS INTEGER) > 0  THEN 'Moderado'
                WHEN TRY_CAST(levemente_feridos AS INTEGER) > 0      THEN 'Leve'
                ELSE 'Sem Vítimas'
            END                                                                          AS severidade,
            YEAR(TRY_STRPTIME(data, '%d/%m/%Y')::DATE)                                  AS ano,
            MONTH(TRY_STRPTIME(data, '%d/%m/%Y')::DATE)                                 AS mes
        FROM df
    """).df()
