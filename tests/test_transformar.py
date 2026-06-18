from datetime import date

import pandas as pd
import pytest

from src.transformations import COLS_VEICULOS, COLS_VITIMAS, transformar


def _bronze_row(**overrides) -> dict:
    defaults = {
        "data": "15/03/2022",
        "horario": "14:30:00",
        "km": "123,5",
        "sentido": "Norte",
        "tipo_de_ocorrencia": "Sem Vitima",
        "trecho": "BR-116/SP",
        "automovel": "2",  "bicicleta": "0", "caminhao": "1",  "moto": "0",
        "onibus": "0",     "outros": "0",    "tracao_animal": "0",
        "transporte_de_cargas_especiais": "0", "trator_maquinas": "0", "utilitarios": "0",
        "ilesos": "2",     "levemente_feridos": "0", "moderadamente_feridos": "0",
        "gravemente_feridos": "0", "mortos": "0",
    }
    defaults.update(overrides)
    return defaults


def _transformar_row(**overrides):
    df = pd.DataFrame([_bronze_row(**overrides)])
    return transformar(df).iloc[0]


class TestData:
    def test_data_parsing(self):
        row = _transformar_row(data="15/03/2022")
        assert row["data"].date() == date(2022, 3, 15)

    def test_ano_e_mes_derivados(self):
        row = _transformar_row(data="07/11/2019")
        assert row["ano"] == 2019
        assert row["mes"] == 11

    def test_data_invalida_vira_null(self):
        row = _transformar_row(data="99/99/9999")
        assert pd.isna(row["data"])


class TestHora:
    def test_hora_extraida_do_horario(self):
        assert _transformar_row(horario="14:30:00")["hora"] == 14

    def test_hora_meia_noite(self):
        assert _transformar_row(horario="00:00:00")["hora"] == 0


class TestKm:
    def test_virgula_para_ponto(self):
        assert _transformar_row(km="123,5")["km"] == pytest.approx(123.5)

    def test_inteiro_sem_virgula(self):
        assert _transformar_row(km="456")["km"] == pytest.approx(456.0)

    def test_zero(self):
        assert _transformar_row(km="0,0")["km"] == pytest.approx(0.0)


class TestSentido:
    @pytest.mark.parametrize("valor", ["NORTE", "N", "PISTA NORTE", "CRESCENTE"])
    def test_variantes_norte(self, valor):
        assert _transformar_row(sentido=valor)["sentido"] == "Norte"

    @pytest.mark.parametrize("valor", ["SUL", "S", "PISTA SUL", "DECRESCENTE"])
    def test_variantes_sul(self, valor):
        assert _transformar_row(sentido=valor)["sentido"] == "Sul"

    def test_valor_desconhecido_vira_indefinido(self):
        assert _transformar_row(sentido="LESTE")["sentido"] == "Indefinido"

    def test_trim_de_espacos(self):
        assert _transformar_row(sentido="  norte  ")["sentido"] == "Norte"


class TestTipoDeOcorrencia:
    @pytest.mark.parametrize("valor", ["sem vitima", "sem vitimas", "s/vitima", "ac01", "ac02"])
    def test_sem_vitima(self, valor):
        assert _transformar_row(tipo_de_ocorrencia=valor)["tipo_de_ocorrencia"] == "Sem Vítima"

    @pytest.mark.parametrize("valor", ["com vitima", "c/vitima", "atropelamento", "ac03"])
    def test_com_vitima(self, valor):
        assert _transformar_row(tipo_de_ocorrencia=valor)["tipo_de_ocorrencia"] == "Com Vítima"

    def test_valor_desconhecido_vira_indefinido(self):
        assert _transformar_row(tipo_de_ocorrencia="colisao frontal")["tipo_de_ocorrencia"] == "Indefinido"


class TestTrechoSplit:
    def test_rodovia_e_uf(self):
        row = _transformar_row(trecho="BR-116/SP")
        assert row["rodovia"] == "BR-116"
        assert row["uf"] == "SP"

    def test_outro_trecho(self):
        row = _transformar_row(trecho="BR-040/MG")
        assert row["rodovia"] == "BR-040"
        assert row["uf"] == "MG"

    def test_trim_de_espacos(self):
        row = _transformar_row(trecho=" BR-101 / RJ ")
        assert row["rodovia"] == "BR-101"
        assert row["uf"] == "RJ"


class TestSeveridade:
    def test_fatal(self):
        assert _transformar_row(mortos="1")["severidade"] == "Fatal"

    def test_grave(self):
        assert _transformar_row(mortos="0", gravemente_feridos="1")["severidade"] == "Grave"

    def test_moderado(self):
        assert _transformar_row(mortos="0", gravemente_feridos="0", moderadamente_feridos="1")["severidade"] == "Moderado"

    def test_leve(self):
        assert _transformar_row(mortos="0", gravemente_feridos="0",
                                moderadamente_feridos="0", levemente_feridos="1")["severidade"] == "Leve"

    def test_sem_vitimas(self):
        assert _transformar_row(mortos="0", gravemente_feridos="0",
                                moderadamente_feridos="0", levemente_feridos="0")["severidade"] == "Sem Vítimas"

    def test_fatal_tem_prioridade_sobre_feridos(self):
        assert _transformar_row(mortos="1", gravemente_feridos="2", levemente_feridos="3")["severidade"] == "Fatal"


class TestTotais:
    def test_total_veiculos(self):
        assert _transformar_row(automovel="3", caminhao="2", moto="1")["total_veiculos"] == 6

    def test_total_vitimas(self):
        assert _transformar_row(ilesos="5", levemente_feridos="2", mortos="1")["total_vitimas"] == 8

    def test_null_tratado_como_zero_em_veiculos(self):
        row = _transformar_row(**{c: None for c in COLS_VEICULOS})
        assert row["total_veiculos"] == 0

    def test_null_tratado_como_zero_em_vitimas(self):
        row = _transformar_row(**{c: None for c in COLS_VITIMAS})
        assert row["total_vitimas"] == 0
