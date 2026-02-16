"""
LLM Insight Generator

역할:
- DuckDB의 KPI 마트(mart.daily_campaign_kpi)에서 "특정 날짜" KPI를 조회한다.
- 전일(yday), 최근 7일 평균(w7)과 비교한 컨텍스트를 프롬프트로 구성한다.
- OpenAI API로 "운영 관점 일일 인사이트" Markdown을 생성한다.
- 생성된 인사이트를
  1) data/reports/insight_YYYYMMDD.md 로 저장하고
  2) DuckDB 테이블 mart_daily_insight 에 upsert(DELETE → INSERT) 저장한다.
    -> 동일 날짜 재실행 시 중복 row가 누적되지 않도록 보장.

왜 이렇게 설계했나:
- 데이터 파이프라인 결과물을 "DB + 리포트" 2중 저장
  - Superset/BI는 DB를 보고
  - 운영/리뷰는 md 리포트를 본다.
- LLM 결과도 '운영 데이터'로 간주해 mart에 적재
  - "LLM output"도 재현/추적/비교가 가능해짐(관측 가능성 확보)
- 중복 방지(DELETE→INSERT)
  - 배치 재실행/리런 상황에서 idempotent 보장

Usage:
  python -m llm.insight_generator --date 2026-02-16
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
from dotenv import load_dotenv
from openai import OpenAI


# -------------------------------------------------
# Path / Config
# -------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
REPORT_DIR = DATA_DIR / "reports"
DUCKDB_PATH = DATA_DIR / "portfolio.duckdb"


# -------------------------------------------------
# Date Helpers
# -------------------------------------------------
def yyyymmdd(date_str: str) -> str:
    """YYYY-MM-DD -> YYYYMMDD (파일명 표준화 목적)"""
    return date_str.replace("-", "")


def _date_minus(date_str: str, days: int) -> str:
    """date_str에서 days만큼 이전 날짜를 YYYY-MM-DD로 반환"""
    dt = datetime.strptime(date_str, "%Y-%m-%d").date()
    return (dt - timedelta(days=days)).strftime("%Y-%m-%d")


# -------------------------------------------------
# KPI Fetch
# -------------------------------------------------
def fetch_kpis(con: duckdb.DuckDBPyConnection, date_str: str) -> dict:
    """
    목적:
    - 오늘 KPI(해당 date) 1 row 집계
    - 전일 KPI(해당 date-1) 핵심 지표
    - 최근 7일(오늘 제외) 평균 지표

    주의:
    - 현재 스키마에서 event_date는 VARCHAR로 저장됨
      => 날짜 비교는 문자열 비교로 처리해야 함 (WHERE event_date = ?)
    """

    # -------------------------
    # Today: 해당 날짜 KPI
    # -------------------------
    today = con.execute(
        """
        SELECT
          event_date,
          SUM(impressions) AS impressions,
          SUM(clicks) AS clicks,
          SUM(conversions) AS conversions,
          AVG(ctr) AS ctr,
          AVG(cvr) AS cvr,
          SUM(ad_cost) AS ad_cost,
          SUM(ad_revenue) AS ad_revenue,
          SUM(payments_total) AS payments_total,
          SUM(payments_success) AS payments_success,
          SUM(payments_failed) AS payments_failed,
          AVG(payment_success_rate) AS payment_success_rate,
          SUM(pay_amount_success) AS pay_amount_success
        FROM mart.daily_campaign_kpi
        WHERE event_date = ?
        GROUP BY event_date
        """,
        [date_str],
    ).fetchone()

    if not today:
        raise RuntimeError(
            f"[LLM] No KPI row found for date={date_str}. "
            "먼저 파이프라인 실행으로 mart를 생성하세요."
        )

    cols = [
        "event_date",
        "impressions", "clicks", "conversions", "ctr", "cvr",
        "ad_cost", "ad_revenue",
        "payments_total", "payments_success", "payments_failed", "payment_success_rate",
        "pay_amount_success"
    ]
    today_dict = dict(zip(cols, today))

    # -------------------------
    # Yesterday: 전일 KPI 요약
    # -------------------------
    yday_str = _date_minus(date_str, 1)
    yday = con.execute(
        """
        SELECT
          SUM(ad_revenue) AS ad_revenue,
          SUM(ad_cost) AS ad_cost,
          SUM(payments_failed) AS payments_failed,
          AVG(payment_success_rate) AS payment_success_rate,
          SUM(clicks) AS clicks,
          SUM(conversions) AS conversions
        FROM mart.daily_campaign_kpi
        WHERE event_date = ?
        """,
        [yday_str],
    ).fetchone()

    # -------------------------
    # Last 7 days avg: (오늘 제외)
    # -------------------------
    d7_start = _date_minus(date_str, 7)
    d1 = _date_minus(date_str, 1)

    w7 = con.execute(
        """
        SELECT
          AVG(ad_revenue) AS avg_ad_revenue,
          AVG(ad_cost) AS avg_ad_cost,
          AVG(payments_failed) AS avg_payments_failed,
          AVG(payment_success_rate) AS avg_payment_success_rate,
          AVG(clicks) AS avg_clicks,
          AVG(conversions) AS avg_conversions
        FROM (
          SELECT
            event_date,
            SUM(ad_revenue) AS ad_revenue,
            SUM(ad_cost) AS ad_cost,
            SUM(payments_failed) AS payments_failed,
            AVG(payment_success_rate) AS payment_success_rate,
            SUM(clicks) AS clicks,
            SUM(conversions) AS conversions
          FROM mart.daily_campaign_kpi
          WHERE event_date BETWEEN ? AND ?
          GROUP BY event_date
        )
        """,
        [d7_start, d1],
    ).fetchone()

    yday_dict = {
        "date": yday_str,
        "ad_revenue": yday[0] if yday else None,
        "ad_cost": yday[1] if yday else None,
        "payments_failed": yday[2] if yday else None,
        "payment_success_rate": yday[3] if yday else None,
        "clicks": yday[4] if yday else None,
        "conversions": yday[5] if yday else None,
    }

    w7_dict = {
        "range": f"{d7_start}~{d1}",
        "avg_ad_revenue": w7[0] if w7 else None,
        "avg_ad_cost": w7[1] if w7 else None,
        "avg_payments_failed": w7[2] if w7 else None,
        "avg_payment_success_rate": w7[3] if w7 else None,
        "avg_clicks": w7[4] if w7 else None,
        "avg_conversions": w7[5] if w7 else None,
    }

    return {"today": today_dict, "yday": yday_dict, "w7": w7_dict}


# -------------------------------------------------
# Prompt Utilities
# -------------------------------------------------
def pct_change(curr: float | None, prev: float | None) -> str:
    """
    변화율 계산(문자열 반환).
    - prev가 0 또는 None이면 N/A 처리 (분모 0 방지)
    """
    try:
        if curr is None or prev is None or prev == 0:
            return "N/A"
        return f"{((curr - prev) / prev) * 100:.2f}%"
    except Exception:
        return "N/A"


def build_prompt(payload: dict) -> str:
    """
    LLM 프롬프트 구성 원칙:
    - '운영' 관점(관측/가설/확인/액션)으로 구조화
    - 출력 포맷을 고정하여 결과 안정화
    - 데이터는 "오늘/전일/7일평균 + 변화율"로 최소 충분 컨텍스트만 제공
    """
    t = payload["today"]
    y = payload["yday"]
    w = payload["w7"]

    diff = {
        "ad_revenue_vs_yday": pct_change(t["ad_revenue"], y["ad_revenue"]),
        "ad_cost_vs_yday": pct_change(t["ad_cost"], y["ad_cost"]),
        "payments_failed_vs_yday": pct_change(t["payments_failed"], y["payments_failed"]),
        "pay_success_rate_vs_yday": pct_change(t["payment_success_rate"], y["payment_success_rate"]),
        "ad_revenue_vs_w7": pct_change(t["ad_revenue"], w["avg_ad_revenue"]),
        "payments_failed_vs_w7": pct_change(t["payments_failed"], w["avg_payments_failed"]),
    }

    prompt = f"""
당신은 결제/광고 KPI를 운영 관점에서 요약하는 데이터 분석가입니다.
아래 데이터는 특정 날짜의 집계 KPI와 전일/최근 7일 평균 비교입니다.

[오늘 KPI]
- date: {t["event_date"]}
- impressions: {t["impressions"]}
- clicks: {t["clicks"]}
- conversions: {t["conversions"]}
- ctr: {t["ctr"]}
- cvr: {t["cvr"]}
- ad_cost: {t["ad_cost"]}
- ad_revenue: {t["ad_revenue"]}
- payments_total: {t["payments_total"]}
- payments_success: {t["payments_success"]}
- payments_failed: {t["payments_failed"]}
- payment_success_rate: {t["payment_success_rate"]}
- pay_amount_success: {t["pay_amount_success"]}

[전일 KPI 요약]
- yday: {y["date"]}
- ad_revenue: {y["ad_revenue"]}
- ad_cost: {y["ad_cost"]}
- payments_failed: {y["payments_failed"]}
- payment_success_rate: {y["payment_success_rate"]}
- clicks: {y["clicks"]}
- conversions: {y["conversions"]}

[최근 7일 평균 요약] (기간: {w["range"]})
- avg_ad_revenue: {w["avg_ad_revenue"]}
- avg_ad_cost: {w["avg_ad_cost"]}
- avg_payments_failed: {w["avg_payments_failed"]}
- avg_payment_success_rate: {w["avg_payment_success_rate"]}
- avg_clicks: {w["avg_clicks"]}
- avg_conversions: {w["avg_conversions"]}

[변화율]
- ad_revenue vs yday: {diff["ad_revenue_vs_yday"]}
- ad_cost vs yday: {diff["ad_cost_vs_yday"]}
- payments_failed vs yday: {diff["payments_failed_vs_yday"]}
- payment_success_rate vs yday: {diff["pay_success_rate_vs_yday"]}
- ad_revenue vs 7d avg: {diff["ad_revenue_vs_w7"]}
- payments_failed vs 7d avg: {diff["payments_failed_vs_w7"]}

요구사항:
- 아래 출력 포맷을 반드시 지켜서 한국어로 작성하세요.
- 애매한 부분은 '가설'로 명시하고, 확정적 단정은 피하세요.
- 운영 액션은 '측정 가능'하게 작성하세요. (예: "결제 실패율 X% 이하로", "특정 캠페인 확인", "로그 필드 추가" 등)

[출력 포맷]
# Daily Insight ({t["event_date"]})

## 1) 오늘 요약 (3줄)
- ...
- ...
- ...

## 2) 이상 징후 Top 3 (관측 → 원인 가설 → 확인 방법)
1. 관측: ...
   가설: ...
   확인: ...
2. ...
3. ...

## 3) 추천 액션 3개 (Owner/기한/지표 포함)
- [Owner: ... | DUE: ...] ...
- [Owner: ... | DUE: ...] ...
- [Owner: ... | DUE: ...] ...

## 4) 추가로 필요한 데이터 (없으면 '없음')
- ...
""".strip()

    return prompt


# -------------------------------------------------
# OpenAI Call
# -------------------------------------------------
def call_llm(prompt: str) -> str:
    """
    OpenAI API 호출.
    - .env에서 OPENAI_API_KEY / OPENAI_MODEL 로드
    - temperature 낮게(0.2) 설정해 출력 안정화
    """
    load_dotenv(PROJECT_ROOT / ".env")

    api_key = os.getenv("OPENAI_API_KEY")
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY가 없습니다. 프로젝트 루트의 .env를 확인하세요.")

    client = OpenAI(api_key=api_key)

    resp = client.chat.completions.create(
        model=model,
        temperature=0.2,
        messages=[
            {"role": "system", "content": "당신은 데이터 마트 기반 KPI 운영 인사이트를 작성하는 분석가입니다."},
            {"role": "user", "content": prompt},
        ],
    )
    return resp.choices[0].message.content or ""


# -------------------------------------------------
# Storage: DuckDB + Markdown
# -------------------------------------------------
def ensure_insight_table(con: duckdb.DuckDBPyConnection) -> None:
    """
    결과를 저장할 테이블 보장.
    - event_date 기준으로 재실행 시 중복 방지 필요(아래 save_outputs에서 처리)
    """
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS mart_daily_insight (
          event_date VARCHAR,
          headline VARCHAR,
          risk_level VARCHAR,
          summary_md TEXT,
          created_at TIMESTAMP
        )
        """
    )


def extract_headline_and_risk(md: str) -> tuple[str, str]:
    """
    LLM 출력 md에서 Superset용 컬럼을 추출한다.

    간단 규칙 기반:
    - headline: '## 1) 오늘 요약' 아래 첫 bullet을 headline로 사용
    - risk_level: 키워드 기반 LOW/MEDIUM/HIGH 추정
      (운영 현업에서는 rule-based risk label이 1차 triage에 유용)
    """
    headline = "Daily KPI Insight"
    risk_level = "LOW"

    lines = [ln.strip() for ln in md.splitlines() if ln.strip()]

    # headline 추출: "오늘 요약" 섹션의 첫 bullet을 사용
    for i, ln in enumerate(lines):
        if ln.startswith("## 1) 오늘 요약"):
            for j in range(i + 1, min(i + 8, len(lines))):
                if lines[j].startswith("-"):
                    headline = lines[j].lstrip("-").strip()
                    break
            break

    # risk rule: 키워드 기반 간단 분류
    text = md.lower()
    if any(k in text for k in ["급락", "장애", "오류", "폭증", "실패율", "anomaly", "이상"]):
        risk_level = "MEDIUM"
    if any(k in text for k in ["중단", "결제 불가", "치명", "대규모", "심각"]):
        risk_level = "HIGH"

    return headline[:180], risk_level


def save_outputs(con: duckdb.DuckDBPyConnection, date_str: str, md: str) -> Path:
    """
    저장 전략:
    1) data/reports/insight_YYYYMMDD.md 파일 저장 (운영 리뷰/리포트)
    2) DuckDB mart_daily_insight 저장 (Superset BI 활용)

    핵심:
    - 동일 날짜 재실행 시 중복 row가 쌓이지 않게 DELETE → INSERT 수행
      => idempotent batch 보장
    """
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    # 1) 파일 저장
    report_path = REPORT_DIR / f"insight_{yyyymmdd(date_str)}.md"
    report_path.write_text(md, encoding="utf-8")

    # 2) DB 저장
    ensure_insight_table(con)
    headline, risk = extract_headline_and_risk(md)

    # 중복 방지: 같은 날짜 있으면 먼저 삭제
    con.execute(
        "DELETE FROM mart_daily_insight WHERE event_date = ?",
        [date_str],
    )

    # 그 다음 신규 INSERT
    con.execute(
        """
        INSERT INTO mart_daily_insight (event_date, headline, risk_level, summary_md, created_at)
        VALUES (?, ?, ?, ?, NOW())
        """,
        [date_str, headline, risk, md],
    )

    return report_path


# -------------------------------------------------
# Entrypoint
# -------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    if not DUCKDB_PATH.exists():
        raise RuntimeError(f"DuckDB 파일이 없습니다: {DUCKDB_PATH}. 먼저 build_duckdb.py를 실행하세요.")

    # DuckDB 연결
    con = duckdb.connect(str(DUCKDB_PATH))

    # KPI 조회 → 프롬프트 생성 → LLM 호출 → 저장
    payload = fetch_kpis(con, args.date)
    prompt = build_prompt(payload)
    md = call_llm(prompt)

    if not md.strip():
        raise RuntimeError("[LLM] empty response")

    report_path = save_outputs(con, args.date, md)
    print(f"[OK] LLM insight saved: {report_path}")
    print("[OK] DuckDB table updated (idempotent): mart_daily_insight")


if __name__ == "__main__":
    main()
