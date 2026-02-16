"""
Text2SQL (LLM)
- 자연어 질문 -> 안전하게 실행 가능한 SELECT SQL 생성
- duckdb/superset 환경 기준: 스키마/테이블은 allowlist 기반으로만 사용

원칙
- 가능한 "단일 테이블" 쿼리 유도 (JOIN 금지 정책과 호환)
- event_date 타입/포맷 주의: DuckDB에서 DATE/TIMESTAMP/VARCHAR 가능
  - 프로젝트에서는 mart.daily_campaign_kpi의 event_date가 DATE로 보일 수 있음(Superset 화면 기준)
  - 따라서 WHERE event_date BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD' 형태 권장
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

PROJECT_ROOT = Path(__file__).resolve().parents[1]


SYSTEM_PROMPT = """
당신은 DuckDB에서 실행 가능한 SQL을 작성하는 데이터 엔지니어입니다.

제약:
- 반드시 SELECT 문만 작성하세요.
- 단일 스테이트먼트만 작성하세요. (세미콜론 금지)
- JOIN 사용 금지.
- 접근 가능한 테이블은 아래 2개뿐입니다:
  1) mart.daily_campaign_kpi
  2) mart_daily_insight
- 가능한 한 컬럼을 명시하세요. (SELECT * 지양)
- 결과가 너무 크지 않도록 LIMIT을 꼭 넣으세요. (기본 1000 이하)

데이터 설명:
- mart.daily_campaign_kpi 컬럼:
  event_date, campaign_id, impressions, clicks, conversions, ctr, cvr,
  ad_cost, ad_revenue, payments_total, payments_success, payments_failed,
  payment_success_rate, pay_amount_success
- mart_daily_insight 컬럼:
  event_date, headline, risk_level, summary_md, created_at

출력:
- SQL만 출력하세요. 코드블럭(```sql) 없이 순수 SQL 텍스트로만 출력하세요.
""".strip()


@dataclass
class Text2SQLResult:
    sql: str
    model: str


def generate_sql(nl_query: str, *, model: str | None = None) -> Text2SQLResult:
    """
    자연어 -> SQL 생성
    - SQL Guard가 후단에 있으므로, 여기서는 "좋은 SQL 생성"에 집중
    """
    load_dotenv(PROJECT_ROOT / ".env")

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY가 없습니다. 프로젝트 루트의 .env를 확인하세요.")

    use_model = model or os.getenv("OPENAI_MODEL_TEXT2SQL", os.getenv("OPENAI_MODEL", "gpt-4o-mini"))
    client = OpenAI(api_key=api_key)

    resp = client.chat.completions.create(
        model=use_model,
        temperature=0.0,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": nl_query.strip()},
        ],
    )

    sql = (resp.choices[0].message.content or "").strip()

    # 혹시 코드펜스를 뱉는 모델이 있으면 1차 제거(Guard에서 한번 더 처리됨)
    sql = sql.replace("```sql", "").replace("```", "").strip()

    return Text2SQLResult(sql=sql, model=use_model)
