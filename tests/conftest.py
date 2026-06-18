# Testes usam DuckDB + pandas — sem SparkSession, sem Java.
# Esta fixture está disponível para testes de integração futuros.
import duckdb
import pytest


@pytest.fixture(scope="session")
def con():
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()
