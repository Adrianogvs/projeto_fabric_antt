from datetime import date

import pytest
from pyspark.sql import SparkSession

from src.helpers import COLS_VEICULOS, COLS_VITIMAS, transformar


def _bronze_row(**overrides) -> dict:
    defaults = {
        "data": "15/03/2022",
        "horario": "14:30:00",
        "km": "123,5",
        "sentido": "Norte",
        "tipo_de_ocorrencia": "Sem Vitima",
        "trecho": "BR-116/SP",
        # veículos
        "automovel": "2",
        "bicicleta": "0",
        "caminhao": "1",
        "moto": "0",
        "onibus": "0",
        "outros": "0",
        "tracao_animal": "0",
        "transporte_de_cargas_especiais": "0",
        "trator_maquinas": "0",
        "utilitarios": "0",
        # vítimas
        "ilesos": "2",
        "levemente_feridos": "0",
        "moderadamente_feridos": "0",
        "gravemente_feridos": "0",
        "mortos": "0",
    }
    defaults.update(overrides)
    return defaults


def _transformar_row(spark: SparkSession, **overrides):
    row = _bronze_row(**overrides)
    df = spark.createDataFrame([row])
    return transformar(df).collect()[0]


class TestData:
    def test_data_parsing(self, spark):
        row = _transformar_row(spark, data="15/03/2022")
        assert row["data"] == date(2022, 3, 15)

    def test_ano_e_mes_derivados(self, spark):
        row = _transformar_row(spark, data="07/11/2019")
        assert row["ano"] == 2019
        assert row["mes"] == 11

    def test_data_invalida_vira_null(self, spark):
        row = _transformar_row(spark, data="99/99/9999")
        assert row["data"] is None


class TestHora:
    def test_hora_extraida_do_horario(self, spark):
        row = _transformar_row(spark, horario="14:30:00")
        assert row["hora"] == 14

    def test_hora_meia_noite(self, spark):
        row = _transformar_row(spark, horario="00:00:00")
        assert row["hora"] == 0


class TestKm:
    def test_virgula_para_ponto(self, spark):
        row = _transformar_row(spark, km="123,5")
        assert row["km"] == pytest.approx(123.5)

    def test_inteiro_sem_virgula(self, spark):
        row = _transformar_row(spark, km="456")
        assert row["km"] == pytest.approx(456.0)

    def test_zero(self, spark):
        row = _transformar_row(spark, km="0,0")
        assert row["km"] == pytest.approx(0.0)


class TestSentido:
    @pytest.mark.parametrize("valor", ["NORTE", "N", "PISTA NORTE", "CRESCENTE"])
    def test_variantes_norte(self, spark, valor):
        row = _transformar_row(spark, sentido=valor)
        assert row["sentido"] == "Norte"

    @pytest.mark.parametrize("valor", ["SUL", "S", "PISTA SUL", "DECRESCENTE"])
    def test_variantes_sul(self, spark, valor):
        row = _transformar_row(spark, sentido=valor)
        assert row["sentido"] == "Sul"

    def test_valor_desconhecido_vira_indefinido(self, spark):
        row = _transformar_row(spark, sentido="LESTE")
        assert row["sentido"] == "Indefinido"

    def test_trim_de_espacos(self, spark):
        row = _transformar_row(spark, sentido="  norte  ")
        assert row["sentido"] == "Norte"


class TestTipoDeOcorrencia:
    @pytest.mark.parametrize("valor", ["sem vitima", "sem vitimas", "s/vitima", "ac01", "ac02"])
    def test_sem_vitima(self, spark, valor):
        row = _transformar_row(spark, tipo_de_ocorrencia=valor)
        assert row["tipo_de_ocorrencia"] == "Sem Vítima"

    @pytest.mark.parametrize("valor", ["com vitima", "c/vitima", "atropelamento", "ac03"])
    def test_com_vitima(self, spark, valor):
        row = _transformar_row(spark, tipo_de_ocorrencia=valor)
        assert row["tipo_de_ocorrencia"] == "Com Vítima"

    def test_valor_desconhecido_vira_indefinido(self, spark):
        row = _transformar_row(spark, tipo_de_ocorrencia="colisao frontal")
        assert row["tipo_de_ocorrencia"] == "Indefinido"


class TestTrechoSplit:
    def test_rodovia_e_uf(self, spark):
        row = _transformar_row(spark, trecho="BR-116/SP")
        assert row["rodovia"] == "BR-116"
        assert row["uf"] == "SP"

    def test_outro_trecho(self, spark):
        row = _transformar_row(spark, trecho="BR-040/MG")
        assert row["rodovia"] == "BR-040"
        assert row["uf"] == "MG"

    def test_trim_de_espacos(self, spark):
        row = _transformar_row(spark, trecho=" BR-101 / RJ ")
        assert row["rodovia"] == "BR-101"
        assert row["uf"] == "RJ"


class TestSeveridade:
    def test_fatal(self, spark):
        row = _transformar_row(spark, mortos="1")
        assert row["severidade"] == "Fatal"

    def test_grave(self, spark):
        row = _transformar_row(spark, mortos="0", gravemente_feridos="1")
        assert row["severidade"] == "Grave"

    def test_moderado(self, spark):
        row = _transformar_row(spark, mortos="0", gravemente_feridos="0", moderadamente_feridos="1")
        assert row["severidade"] == "Moderado"

    def test_leve(self, spark):
        row = _transformar_row(spark, mortos="0", gravemente_feridos="0",
                               moderadamente_feridos="0", levemente_feridos="1")
        assert row["severidade"] == "Leve"

    def test_sem_vitimas(self, spark):
        row = _transformar_row(spark, mortos="0", gravemente_feridos="0",
                               moderadamente_feridos="0", levemente_feridos="0")
        assert row["severidade"] == "Sem Vítimas"

    def test_fatal_tem_prioridade_sobre_feridos(self, spark):
        row = _transformar_row(spark, mortos="1", gravemente_feridos="2", levemente_feridos="3")
        assert row["severidade"] == "Fatal"


class TestTotais:
    def test_total_veiculos(self, spark):
        row = _transformar_row(spark, automovel="3", caminhao="2", moto="1")
        assert row["total_veiculos"] == 6

    def test_total_vitimas(self, spark):
        row = _transformar_row(spark, ilesos="5", levemente_feridos="2", mortos="1")
        assert row["total_vitimas"] == 8

    def test_null_tratado_como_zero_em_veiculos(self, spark):
        overrides = {c: None for c in COLS_VEICULOS}
        row = _transformar_row(spark, **overrides)
        assert row["total_veiculos"] == 0

    def test_null_tratado_como_zero_em_vitimas(self, spark):
        overrides = {c: None for c in COLS_VITIMAS}
        row = _transformar_row(spark, **overrides)
        assert row["total_vitimas"] == 0
