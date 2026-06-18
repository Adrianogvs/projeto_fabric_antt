"""
Pipeline local — Bronze → Silver → Gold (DuckDB)

Replica a lógica dos notebooks Fabric localmente, sem Spark nem Java.
Os dados são lidos de lakehouse_antt/Files/bronze/ e salvos como Parquet
em lakehouse_antt/Tables/dbo/, espelhando a estrutura do OneLake.

Uso:
    py -m src.pipeline_local
"""
import logging
import time
from pathlib import Path

import duckdb
import pandas as pd

from src.transformations import CONCESSAO_MAP, COLS_VEICULOS, COLS_VITIMAS

log = logging.getLogger(__name__)

ROOT        = Path(__file__).resolve().parent.parent
BRONZE_PATH = ROOT / "lakehouse_antt" / "Files"  / "bronze" / "acidentes"
DBO_PATH    = ROOT / "lakehouse_antt" / "Tables" / "dbo"


# ── Utilitários ───────────────────────────────────────────────────────────────

def _parquet(dest: Path) -> str:
    return str(dest / "data.parquet").replace("\\", "/")


def _salvar(con: duckdb.DuckDBPyConnection, view: str, dest: Path) -> int:
    """Materializa uma view como Parquet e retorna o total de registros."""
    dest.mkdir(parents=True, exist_ok=True)
    pq = _parquet(dest)
    con.execute(f"COPY (SELECT * FROM {view}) TO '{pq}' (FORMAT PARQUET)")
    total = con.execute(f"SELECT COUNT(*) FROM '{pq}'").fetchone()[0]
    log.info("  %-40s %d registros", dest.name, total)
    return total


def _view_parquet(con: duckdb.DuckDBPyConnection, view: str, dest: Path) -> None:
    """Reaponta uma view para ler do Parquet já salvo (evita reprocessar)."""
    con.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM '{_parquet(dest)}'")


# ── Etapa 1: Bronze → Silver ──────────────────────────────────────────────────

def _bronze(con: duckdb.DuckDBPyConnection) -> None:
    """Lê todos os CSVs Bronze com pandas (cp1252) e registra no DuckDB."""
    frames = []
    for csv in sorted(BRONZE_PATH.glob("*.csv")):
        df = pd.read_csv(
            csv,
            sep=";",
            encoding="cp1252",
            dtype=str,
            quotechar='"',
            on_bad_lines="skip",
        )
        df["_filename"] = csv.name
        frames.append(df)

    bronze = pd.concat(frames, ignore_index=True)
    con.register("_bronze_df", bronze)
    con.execute("CREATE OR REPLACE VIEW bronze AS SELECT * FROM _bronze_df")
    log.info("  CSVs lidos   : %d registros brutos (%d arquivos)", len(bronze), len(frames))


def _silver(con: duckdb.DuckDBPyConnection) -> None:
    """Aplica todas as transformações Bronze → Silver numa view."""
    lookup = pd.DataFrame(list(CONCESSAO_MAP.items()), columns=["_key", "concessionaria"])
    con.register("_lookup_concessao", lookup)

    veiculos_cols = ",\n        ".join(
        [f"COALESCE(TRY_CAST({c} AS INTEGER), 0) AS {c}" for c in COLS_VEICULOS]
    )
    vitimas_cols = ",\n        ".join(
        [f"COALESCE(TRY_CAST({c} AS INTEGER), 0) AS {c}" for c in COLS_VITIMAS]
    )
    soma_veiculos = " + ".join(
        [f"COALESCE(TRY_CAST({c} AS INTEGER), 0)" for c in COLS_VEICULOS]
    )
    soma_vitimas = " + ".join(
        [f"COALESCE(TRY_CAST({c} AS INTEGER), 0)" for c in COLS_VITIMAS]
    )

    con.execute(f"""
        CREATE OR REPLACE VIEW silver AS
        WITH base AS (
            SELECT *,
                regexp_extract(_filename, 'demostrativo_acidentes_([^/.]+)\\.csv', 1) AS _file_key
            FROM bronze
        ),
        mapped AS (
            SELECT base.*,
                COALESCE(l.concessionaria, 'DESCONHECIDA') AS concessionaria
            FROM base
            LEFT JOIN _lookup_concessao l ON base._file_key = l._key
        )
        SELECT
            TRY_STRPTIME(data, '%d/%m/%Y')::DATE                                        AS data,
            TRY_CAST(split_part(horario, ':', 1) AS INTEGER)                            AS hora,
            TRY_CAST(replace(km, ',', '.') AS DOUBLE)                                   AS km,
            concessionaria,
            tipo_de_acidente,
            CASE
                WHEN upper(trim(sentido)) IN ('NORTE','N','PISTA NORTE','CRESCENTE') THEN 'Norte'
                WHEN upper(trim(sentido)) IN ('SUL','S','PISTA SUL','DECRESCENTE')   THEN 'Sul'
                ELSE 'Indefinido'
            END                                                                          AS sentido,
            CASE
                WHEN regexp_matches(lower(tipo_de_ocorrencia),'sem|s.vitima|ac01|ac02')   THEN 'Sem Vítima'
                WHEN regexp_matches(lower(tipo_de_ocorrencia),'com|c.vitima|atropel|ac03') THEN 'Com Vítima'
                ELSE 'Indefinido'
            END                                                                          AS tipo_de_ocorrencia,
            trim(split_part(trecho, '/', 1))                                            AS rodovia,
            trim(split_part(trecho, '/', 2))                                            AS uf,
            {veiculos_cols},
            {vitimas_cols},
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
        FROM mapped
    """)


# ── Etapa 2: Silver → Dimensões ───────────────────────────────────────────────

def _dims(con: duckdb.DuckDBPyConnection) -> None:
    # dim_data — SK como inteiro yyyyMMdd (legível no Power BI)
    con.execute("""
        CREATE OR REPLACE VIEW gold_dim_data AS
        SELECT DISTINCT
            CAST(strftime(data, '%Y%m%d') AS INTEGER) AS id_data,
            data,
            YEAR(data)          AS ano,
            MONTH(data)         AS mes,
            DAY(data)           AS dia,
            QUARTER(data)       AS trimestre,
            DAYOFWEEK(data) + 1 AS dia_semana,
            strftime(data, '%B') AS nome_mes,
            DAYOFWEEK(data) IN (0, 6) AS fim_de_semana
        FROM silver
        WHERE data IS NOT NULL
        ORDER BY data
    """)
    _salvar(con, "gold_dim_data", DBO_PATH / "gold_dim_data")

    # dim_concessionaria
    con.execute("""
        CREATE OR REPLACE VIEW gold_dim_concessionaria AS
        SELECT ROW_NUMBER() OVER (ORDER BY concessionaria) AS id_concessionaria,
               concessionaria
        FROM (SELECT DISTINCT concessionaria FROM silver WHERE concessionaria IS NOT NULL)
    """)
    _salvar(con, "gold_dim_concessionaria", DBO_PATH / "gold_dim_concessionaria")

    # dim_rodovia
    con.execute("""
        CREATE OR REPLACE VIEW gold_dim_rodovia AS
        SELECT ROW_NUMBER() OVER (ORDER BY uf, rodovia) AS id_rodovia, rodovia, uf
        FROM (
            SELECT DISTINCT rodovia, uf
            FROM silver
            WHERE rodovia IS NOT NULL AND uf IS NOT NULL
        )
    """)
    _salvar(con, "gold_dim_rodovia", DBO_PATH / "gold_dim_rodovia")

    # dim_tipo_acidente
    con.execute("""
        CREATE OR REPLACE VIEW gold_dim_tipo_acidente AS
        SELECT ROW_NUMBER() OVER (ORDER BY tipo_de_acidente) AS id_tipo_acidente,
               tipo_de_acidente
        FROM (SELECT DISTINCT tipo_de_acidente FROM silver WHERE tipo_de_acidente IS NOT NULL)
    """)
    _salvar(con, "gold_dim_tipo_acidente", DBO_PATH / "gold_dim_tipo_acidente")

    # dim_veiculo — estática, não deriva do Silver
    veiculos = [(i + 1, v) for i, v in enumerate(sorted(COLS_VEICULOS))]
    con.execute("CREATE OR REPLACE TABLE _dim_veiculo_t (id_veiculo INTEGER, tipo_veiculo VARCHAR)")
    con.executemany("INSERT INTO _dim_veiculo_t VALUES (?, ?)", veiculos)
    con.execute("CREATE OR REPLACE VIEW gold_dim_veiculo AS SELECT * FROM _dim_veiculo_t ORDER BY id_veiculo")
    _salvar(con, "gold_dim_veiculo", DBO_PATH / "gold_dim_veiculo")

    # dim_tipo_vitima — estática
    vitimas = [(i + 1, v) for i, v in enumerate(sorted(COLS_VITIMAS))]
    con.execute("CREATE OR REPLACE TABLE _dim_tipo_vitima_t (id_tipo_vitima INTEGER, tipo_vitima VARCHAR)")
    con.executemany("INSERT INTO _dim_tipo_vitima_t VALUES (?, ?)", vitimas)
    con.execute("CREATE OR REPLACE VIEW gold_dim_tipo_vitima AS SELECT * FROM _dim_tipo_vitima_t ORDER BY id_tipo_vitima")
    _salvar(con, "gold_dim_tipo_vitima", DBO_PATH / "gold_dim_tipo_vitima")


# ── Etapa 3: Silver + Dims → Fatos ───────────────────────────────────────────

def _fatos(con: duckdb.DuckDBPyConnection) -> None:
    # Surrogate key determinístico — equivalente ao xxhash64 do Spark
    _ID_ACIDENTE = """hash(
        CAST(s.data AS VARCHAR) || '|' || COALESCE(s.concessionaria, '') || '|' ||
        CAST(COALESCE(s.km, 0) AS VARCHAR) || '|' ||
        CAST(COALESCE(s.hora, -1) AS VARCHAR) || '|' ||
        COALESCE(s.tipo_de_acidente, '')
    )"""

    veiculos_s = ", ".join([f"s.{c}" for c in COLS_VEICULOS])
    vitimas_s  = ", ".join([f"s.{c}" for c in COLS_VITIMAS])

    # View auxiliar com todas as FKs resolvidas — base para os 3 fatos
    con.execute(f"""
        CREATE OR REPLACE VIEW _silver_fk AS
        SELECT
            ({_ID_ACIDENTE})                         AS id_acidente,
            d.id_data,
            CAST(d.id_data / 10000 AS INTEGER)       AS ano,
            c.id_concessionaria,
            r.id_rodovia,
            t.id_tipo_acidente,
            s.km, s.hora, s.sentido, s.tipo_de_ocorrencia, s.severidade,
            s.total_veiculos, s.total_vitimas,
            s.mortos, s.gravemente_feridos, s.moderadamente_feridos,
            s.levemente_feridos, s.ilesos,
            {veiculos_s},
            {vitimas_s}
        FROM silver s
        LEFT JOIN gold_dim_data           d ON s.data             = d.data
        LEFT JOIN gold_dim_concessionaria c ON s.concessionaria   = c.concessionaria
        LEFT JOIN gold_dim_rodovia        r ON s.rodovia           = r.rodovia AND s.uf = r.uf
        LEFT JOIN gold_dim_tipo_acidente  t ON s.tipo_de_acidente = t.tipo_de_acidente
    """)

    # fato_acidente
    con.execute("""
        CREATE OR REPLACE VIEW gold_fato_acidente AS
        SELECT
            id_acidente, id_data, ano, id_concessionaria, id_rodovia, id_tipo_acidente,
            km, hora, sentido, tipo_de_ocorrencia, severidade,
            total_veiculos, total_vitimas,
            mortos, gravemente_feridos, moderadamente_feridos, levemente_feridos, ilesos
        FROM _silver_fk
    """)
    _salvar(con, "gold_fato_acidente", DBO_PATH / "gold_fato_acidente")

    # fato_veiculo_acidente — UNPIVOT 10 colunas de veículo (wide → long)
    veiculos_on = ", ".join(COLS_VEICULOS)
    con.execute(f"""
        CREATE OR REPLACE VIEW gold_fato_veiculo_acidente AS
        WITH unpivoted AS (
            UNPIVOT (
                SELECT id_acidente, id_data, ano, id_concessionaria, id_rodovia, {veiculos_on}
                FROM _silver_fk
            ) ON {veiculos_on} INTO NAME tipo_veiculo VALUE quantidade
        )
        SELECT u.id_acidente, u.id_data, u.ano, u.id_concessionaria, u.id_rodovia,
               v.id_veiculo, u.quantidade
        FROM unpivoted u
        LEFT JOIN gold_dim_veiculo v ON u.tipo_veiculo = v.tipo_veiculo
        WHERE u.quantidade > 0
    """)
    _salvar(con, "gold_fato_veiculo_acidente", DBO_PATH / "gold_fato_veiculo_acidente")

    # fato_vitima_acidente — UNPIVOT 5 colunas de vítima (wide → long)
    vitimas_on = ", ".join(COLS_VITIMAS)
    con.execute(f"""
        CREATE OR REPLACE VIEW gold_fato_vitima_acidente AS
        WITH unpivoted AS (
            UNPIVOT (
                SELECT id_acidente, id_data, ano, id_concessionaria, id_rodovia, {vitimas_on}
                FROM _silver_fk
            ) ON {vitimas_on} INTO NAME tipo_vitima VALUE quantidade
        )
        SELECT u.id_acidente, u.id_data, u.ano, u.id_concessionaria, u.id_rodovia,
               tv.id_tipo_vitima, u.quantidade
        FROM unpivoted u
        LEFT JOIN gold_dim_tipo_vitima tv ON u.tipo_vitima = tv.tipo_vitima
        WHERE u.quantidade > 0
    """)
    _salvar(con, "gold_fato_vitima_acidente", DBO_PATH / "gold_fato_vitima_acidente")


# ── Orquestração ──────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )
    t_total = time.monotonic()
    log.info("Pipeline local iniciado.")
    log.info("Bronze  : %s", BRONZE_PATH)
    log.info("Destino : %s", DBO_PATH)

    con = duckdb.connect()
    try:
        # ── Bronze → Silver
        t0 = time.monotonic()
        log.info("── BRONZE → SILVER ──────────────────────────")
        _bronze(con)
        _silver(con)
        _salvar(con, "silver", DBO_PATH / "silver_acidentes")
        # Reaponta 'silver' para o Parquet já salvo — Gold queries não releem os CSVs
        _view_parquet(con, "silver", DBO_PATH / "silver_acidentes")
        log.info("  Concluído em %.1fs", time.monotonic() - t0)

        # ── Silver → Dims
        t0 = time.monotonic()
        log.info("── SILVER → DIMS ─────────────────────────────")
        _dims(con)
        log.info("  Concluído em %.1fs", time.monotonic() - t0)

        # ── Dims + Silver → Fatos
        t0 = time.monotonic()
        log.info("── DIMS + SILVER → FATOS ─────────────────────")
        _fatos(con)
        log.info("  Concluído em %.1fs", time.monotonic() - t0)

    finally:
        con.close()

    log.info("Pipeline concluído em %.1fs ✓", time.monotonic() - t_total)


if __name__ == "__main__":
    main()
