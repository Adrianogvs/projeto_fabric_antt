import pandas as pd

from src.transformations import CONCESSAO_MAP, mapear_concessionaria


def _df(filename: str) -> pd.DataFrame:
    return pd.DataFrame([{"_filename": filename}])


class TestMapearConcessionaria:
    def test_chave_conhecida_af(self):
        result = mapear_concessionaria(_df("Files/bronze/acidentes/demostrativo_acidentes_af.csv"), CONCESSAO_MAP)
        assert result.iloc[0]["concessionaria"] == "AUTOPISTA FLUMINENSE"

    def test_chave_conhecida_epr_litoral_pioneiro(self):
        result = mapear_concessionaria(_df("Files/bronze/acidentes/demostrativo_acidentes_epr_litoral_pioneiro.csv"), CONCESSAO_MAP)
        assert result.iloc[0]["concessionaria"] == "EPR LITORAL PIONEIRO"

    def test_chave_conhecida_rota_verde_goias(self):
        result = mapear_concessionaria(_df("Files/bronze/acidentes/demostrativo_acidentes_rota-verde-goias.csv"), CONCESSAO_MAP)
        assert result.iloc[0]["concessionaria"] == "ROTA VERDE GOIÁS"

    def test_chave_desconhecida(self):
        result = mapear_concessionaria(_df("Files/bronze/acidentes/demostrativo_acidentes_novanova.csv"), CONCESSAO_MAP)
        assert result.iloc[0]["concessionaria"] == "DESCONHECIDA"

    def test_remove_colunas_internas(self):
        result = mapear_concessionaria(_df("Files/bronze/acidentes/demostrativo_acidentes_af.csv"), CONCESSAO_MAP)
        assert "_filename" not in result.columns
        assert "_file_key" not in result.columns

    def test_todas_as_35_chaves_mapeadas(self):
        rows = [
            {"_filename": f"Files/bronze/acidentes/demostrativo_acidentes_{k}.csv"}
            for k in CONCESSAO_MAP
        ]
        df = pd.DataFrame(rows)
        result = mapear_concessionaria(df, CONCESSAO_MAP)
        assert (result["concessionaria"] == "DESCONHECIDA").sum() == 0
