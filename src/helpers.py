import logging
from functools import reduce
from typing import Dict, List

from pyspark.sql import Column, DataFrame, functions as F
from pyspark.sql.functions import when

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


def _expr_concessionaria(mapa: Dict[str, str]) -> Column:
    keys = list(mapa.keys())
    expr = when(F.col("_file_key") == keys[0], mapa[keys[0]])
    for k in keys[1:]:
        expr = expr.when(F.col("_file_key") == k, mapa[k])
    return expr.otherwise("DESCONHECIDA")


def mapear_concessionaria(df: DataFrame, mapa: Dict[str, str]) -> DataFrame:
    df = df.withColumn(
        "_file_key",
        F.regexp_extract(F.col("_filename"), r"demostrativo_acidentes_([^/\\.]+)\.csv", 1),
    ).withColumn("concessionaria", _expr_concessionaria(mapa))

    desconhecidas = df.filter(F.col("concessionaria") == "DESCONHECIDA").count()
    if desconhecidas > 0:
        log.warning("%d registros com concessionaria DESCONHECIDA", desconhecidas)

    return df.drop("_filename", "_file_key")


def transformar(df: DataFrame) -> DataFrame:
    df = (
        df
        .withColumn("data", F.to_date("data", "dd/MM/yyyy"))
        .withColumn("hora", F.split("horario", ":")[0].cast("int"))
    )

    df = df.withColumn("km", F.regexp_replace("km", ",", ".").cast("double"))

    for col in COLS_VEICULOS + COLS_VITIMAS:
        df = df.withColumn(col, F.col(col).cast("int"))

    df = df.withColumn(
        "sentido",
        when(F.upper(F.trim("sentido")).isin("NORTE", "N", "PISTA NORTE", "CRESCENTE"), "Norte")
        .when(F.upper(F.trim("sentido")).isin("SUL", "S", "PISTA SUL", "DECRESCENTE"), "Sul")
        .otherwise("Indefinido"),
    )

    df = df.withColumn(
        "tipo_de_ocorrencia",
        when(F.lower("tipo_de_ocorrencia").rlike(r"sem|s.vitima|ac01|ac02"), "Sem Vítima")
        .when(F.lower("tipo_de_ocorrencia").rlike(r"com|c.vitima|atropel|ac03"), "Com Vítima")
        .otherwise("Indefinido"),
    )

    df = (
        df
        .withColumn("rodovia", F.trim(F.split("trecho", "/")[0]))
        .withColumn("uf",      F.trim(F.split("trecho", "/")[1]))
    )

    soma_veiculos = reduce(lambda a, b: a + b, [F.coalesce(F.col(c), F.lit(0)) for c in COLS_VEICULOS])
    soma_vitimas  = reduce(lambda a, b: a + b, [F.coalesce(F.col(c), F.lit(0)) for c in COLS_VITIMAS])

    df = (
        df
        .withColumn("total_veiculos", soma_veiculos)
        .withColumn("total_vitimas",  soma_vitimas)
        .withColumn(
            "severidade",
            when(F.col("mortos") > 0,                 "Fatal")
            .when(F.col("gravemente_feridos") > 0,    "Grave")
            .when(F.col("moderadamente_feridos") > 0, "Moderado")
            .when(F.col("levemente_feridos") > 0,     "Leve")
            .otherwise("Sem Vítimas"),
        )
    )

    df = df.withColumn("ano", F.year("data")).withColumn("mes", F.month("data"))

    log.info("Transformações aplicadas.")
    return df
