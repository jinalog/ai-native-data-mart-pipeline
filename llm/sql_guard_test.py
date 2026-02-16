"""
SQL Guard Test Runner
- `llm/sql_guard.py`의 validate_sql이 제대로 막고/통과시키는지 빠르게 확인

실행:
  (venv) python -m llm.sql_guard_test
또는
  (venv) python llm/sql_guard_test.py
"""

from __future__ import annotations

from llm.sql_guard import validate_sql


def run_one(name: str, sql: str, should_pass: bool) -> None:
    print(f"\n=== {name} ===")
    print(sql)
    try:
        out = validate_sql(sql)
        print("[PASS] validated SQL =>", out)
        if not should_pass:
            raise AssertionError("원래 막혀야 하는데 통과했습니다.")
    except Exception as e:
        print("[BLOCKED]", repr(e))
        if should_pass:
            raise AssertionError("원래 통과해야 하는데 막혔습니다.") from e


def main() -> None:
    # ✅ 통과해야 하는 케이스
    run_one(
        "ok_select_basic",
        "SELECT * FROM mart.daily_campaign_kpi WHERE event_date='2026-02-19'",
        should_pass=True,
    )

    run_one(
        "ok_select_trailing_semicolon",
        "SELECT event_date, ad_revenue FROM mart.daily_campaign_kpi LIMIT 10;",
        should_pass=True,
    )

    # ✅ 막혀야 하는 케이스들
    run_one(
        "block_multi_statement",
        "SELECT 1; SELECT 2",
        should_pass=False,
    )

    run_one(
        "block_comment",
        "SELECT * FROM mart.daily_campaign_kpi -- comment",
        should_pass=False,
    )

    run_one(
        "block_join_keyword",
        "SELECT * FROM mart.daily_campaign_kpi JOIN mart_daily_insight ON 1=1",
        should_pass=False,
    )

    run_one(
        "block_implicit_join_comma",
        "SELECT * FROM mart.daily_campaign_kpi, mart_daily_insight",
        should_pass=False,
    )

    run_one(
        "block_dml_delete",
        "DELETE FROM mart_daily_insight",
        should_pass=False,
    )

    run_one(
        "block_unknown_table",
        "SELECT * FROM mart.some_other_table",
        should_pass=False,
    )

    print("\n✅ ALL TESTS OK")


if __name__ == "__main__":
    main()
