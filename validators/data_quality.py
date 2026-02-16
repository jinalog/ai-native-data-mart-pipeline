"""
Data Quality Validator (Stage 1)

목적:
- "운영 가능한 파이프라인" 증거로서, 최소한의 DQ 지표를 자동 생성합니다.
- 결과는 data/reports/dq_report_YYYYMMDD.md 로 저장합니다.

검증 항목(최소):
- raw rowcount 존재 여부(0이면 심각)
- mart rowcount 존재 여부(0이면 심각)
- mart의 (event_date, campaign_id) 중복 여부(PK 중복)
- 결제 실패율(운영 모니터링 지표 예시)
"""

from __future__ import annotations

from pathlib import Path
import duckdb


DB_PATH = Path("data/portfolio.duckdb")
REPORT_DIR = Path("data/reports")


def _md(lines: list[str]) -> str:
    return "\n".join(lines) + "\n"


def run_dq(date_str: str) -> Path:
    """
    date_str: 'YYYY-MM-DD' (raw에서는 VARCHAR로 저장되어 있으므로 그대로 조건 사용)
    """
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH))

    # raw rowcount
    raw_ad = con.execute(
        "SELECT COUNT(*) FROM raw.ad_events WHERE event_date = ?",
        [date_str],
    ).fetchone()[0]
    raw_pay = con.execute(
        "SELECT COUNT(*) FROM raw.payment_events WHERE event_date = ?",
        [date_str],
    ).fetchone()[0]

    # mart rowcount (mart는 DATE 타입이므로 CAST해서 비교)
    mart_cnt = con.execute(
        "SELECT COUNT(*) FROM mart.daily_campaign_kpi WHERE event_date = CAST(? AS DATE)",
        [date_str],
    ).fetchone()[0]

    # mart PK duplicates
    dup = con.execute(
        """
        SELECT COUNT(*) FROM (
          SELECT event_date, campaign_id, COUNT(*) c
          FROM mart.daily_campaign_kpi
          WHERE event_date = CAST(? AS DATE)
          GROUP BY 1,2
          HAVING c > 1
        )
        """,
        [date_str],
    ).fetchone()[0]

    # aggregated payment fail rate
    fail_rate = con.execute(
        """
        SELECT
          CASE WHEN SUM(payments_total)=0 THEN 0
               ELSE SUM(payments_failed)::DOUBLE / SUM(payments_total)
          END
        FROM mart.daily_campaign_kpi
        WHERE event_date = CAST(? AS DATE)
        """,
        [date_str],
    ).fetchone()[0]

    con.close()

    # 단순 스코어링(포트폴리오용)
    score = 100
    if raw_ad == 0:
        score -= 40
    if raw_pay == 0:
        score -= 40
    if mart_cnt == 0:
        score -= 30
    if dup > 0:
        score -= 30

    lines = [
        f"# Data Quality Report ({date_str})",
        "",
        "## Row Counts",
        f"- raw.ad_events rows: **{raw_ad}**",
        f"- raw.payment_events rows: **{raw_pay}**",
        f"- mart.daily_campaign_kpi rows: **{mart_cnt}**",
        "",
        "## Integrity",
        f"- mart PK duplicates (event_date, campaign_id): **{dup}**",
        "",
        "## Monitoring Signals",
        f"- payment fail rate (sum): **{fail_rate:.4f}**",
        "",
        "## DQ Score",
        f"**{max(score, 0)} / 100**",
        "",
        "## Notes",
        "- Stage 1 baseline checks (rowcount/duplicates/aggregated rates).",
        "- Raw keeps event_date as VARCHAR to avoid CSV type inference issues; Mart normalizes it to DATE.",
    ]

    out = REPORT_DIR / f"dq_report_{date_str.replace('-', '')}.md"
    out.write_text(_md(lines), encoding="utf-8")
    return out
