import pytest
from pyspark.sql import Row

from src.helpers import CONCESSAO_MAP, mapear_concessionaria


def _df_com_filename(spark, filename: str):
    return spark.createDataFrame([Row(_filename=filename)])


class TestMapearConcessionaria:
    def test_chave_conhecida_af(self, spark):
        df = _df_com_filename(spark, "Files/bronze/acidentes/demostrativo_acidentes_af.csv")
        result = mapear_concessionaria(df, CONCESSAO_MAP).collect()[0]
        assert result["concessionaria"] == "AUTOPISTA FLUMINENSE"

    def test_chave_conhecida_epr_litoral_pioneiro(self, spark):
        df = _df_com_filename(spark, "Files/bronze/acidentes/demostrativo_acidentes_epr_litoral_pioneiro.csv")
        result = mapear_concessionaria(df, CONCESSAO_MAP).collect()[0]
        assert result["concessionaria"] == "EPR LITORAL PIONEIRO"

    def test_chave_conhecida_rota_verde_goias(self, spark):
        df = _df_com_filename(spark, "Files/bronze/acidentes/demostrativo_acidentes_rota-verde-goias.csv")
        result = mapear_concessionaria(df, CONCESSAO_MAP).collect()[0]
        assert result["concessionaria"] == "ROTA VERDE GOIÁS"

    def test_chave_desconhecida(self, spark):
        df = _df_com_filename(spark, "Files/bronze/acidentes/demostrativo_acidentes_novanova.csv")
        result = mapear_concessionaria(df, CONCESSAO_MAP).collect()[0]
        assert result["concessionaria"] == "DESCONHECIDA"

    def test_remove_colunas_internas(self, spark):
        df = _df_com_filename(spark, "Files/bronze/acidentes/demostrativo_acidentes_af.csv")
        result = mapear_concessionaria(df, CONCESSAO_MAP)
        assert "_filename" not in result.columns
        assert "_file_key" not in result.columns

    def test_todas_as_35_chaves_mapeadas(self, spark):
        rows = [
            Row(_filename=f"Files/bronze/acidentes/demostrativo_acidentes_{k}.csv")
            for k in CONCESSAO_MAP
        ]
        df = spark.createDataFrame(rows)
        result = mapear_concessionaria(df, CONCESSAO_MAP)
        desconhecidas = result.filter(result["concessionaria"] == "DESCONHECIDA").count()
        assert desconhecidas == 0
