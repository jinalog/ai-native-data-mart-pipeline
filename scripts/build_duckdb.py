"""
DuckDB 빌더/로더 (Stage 1)

역할:
- (옵션) raw/mart 스키마 및 raw 테이블 생성(SQL 실행)
- 특정 날짜의 CSV를 raw 테이블에 적재
- (옵션) mart.daily_campaign_kpi 재생성

주의:
- Raw의 event_date는 VARCHAR로 설계되어 있으므로, date_str('YYYY-MM-DD') 그대로 적재/삭제합니다.
- CSV에서 event_ts가 문자열로 읽혀도, INSERT 시 CAST로 TIMESTAMP 변환하여 타입 불일치 리스크를 줄입니다.
"""

import argparse
from pathlib import Path

import duckdb


DB_PATH = Path("data/portfolio.duckdb")
RAW_DIR = Path("data/raw")

SQL_CREATE_RAW = Path("sql/create_raw_tables.sql")
SQL_BUILD_MART = Path("sql/build_mart_daily_campaign_kpi.sql")


def to_yyyymmdd(date_str: str) -> str:
    return date_str.replace("-", "")


def exec_sql(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    con.execute(path.read_text(encoding="utf-8"))


def load_csv_for_date(con: duckdb.DuckDBPyConnection, date_str: str) -> None:
    """
    CSV -> raw 테이블 적재.
    - 같은 날짜 데이터가 이미 있으면 DELETE 후 재적재(재실행/백필 대응)
    """
    ds = to_yyyymmdd(date_str)
    ad_csv = (RAW_DIR / f"ad_events_{ds}.csv").resolve()
    pay_csv = (RAW_DIR / f"payment_events_{ds}.csv").resolve()

    if not ad_csv.exists() or not pay_csv.exists():
        raise FileNotFoundError(
            "raw csv not found. 먼저 scripts/generate_realistic_data.py 를 실행하세요."
        )

    # 재실행 안전장치: 동일 date 제거 후 적재
    con.execute("DELETE FROM raw.ad_events WHERE event_date = ?", [date_str])
    con.execute("DELETE FROM raw.payment_events WHERE event_date = ?", [date_str])

    # CSV -> INSERT (컬럼을 명시하여 스키마 변경/순서 이슈에 강하게)
    # event_ts는 CSV에서 문자열일 수 있으므로 CAST로 TIMESTAMP 변환
    con.execute(
        f"""
        INSERT INTO raw.ad_events
        SELECT
          CAST(event_date AS VARCHAR)    AS event_date,
          CAST(event_ts AS TIMESTAMP)    AS event_ts,
          CAST(event_type AS VARCHAR)    AS event_type,
          CAST(campaign_id AS VARCHAR)   AS campaign_id,
          CAST(ad_id AS VARCHAR)         AS ad_id,
          CAST(user_id AS VARCHAR)       AS user_id,
          CAST(device_os AS VARCHAR)     AS device_os,
          CAST(country AS VARCHAR)       AS country,
          CAST(cost AS DOUBLE)           AS cost,
          CAST(revenue AS DOUBLE)        AS revenue
        FROM read_csv_auto('{ad_csv.as_posix()}', header=true)
        """
    )

    con.execute(
        f"""
        INSERT INTO raw.payment_events
        SELECT
          CAST(event_date AS VARCHAR)    AS event_date,
          CAST(event_ts AS TIMESTAMP)    AS event_ts,
          CAST(order_id AS VARCHAR)      AS order_id,
          CAST(user_id AS VARCHAR)       AS user_id,
          CAST(campaign_id AS VARCHAR)   AS campaign_id,
          CAST(amount AS DOUBLE)         AS amount,
          CAST(currency AS VARCHAR)      AS currency,
          CAST(status AS VARCHAR)        AS status,
          CAST(fail_reason AS VARCHAR)   AS fail_reason
        FROM read_csv_auto('{pay_csv.as_posix()}', header=true)
        """
    )


def rebuild_mart(con: duckdb.DuckDBPyConnection) -> None:
    exec_sql(con, SQL_BUILD_MART)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--init", action="store_true", help="Create schemas/tables (run DDL)")
    p.add_argument("--rebuild-mart", action="store_true", help="Rebuild mart tables")
    args = p.parse_args()

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH))

    if args.init:
        exec_sql(con, SQL_CREATE_RAW)

    load_csv_for_date(con, args.date)

    if args.rebuild_mart:
        rebuild_mart(con)

    con.close()
    print("[OK] duckdb load/build done")


if __name__ == "__main__":
    main()
