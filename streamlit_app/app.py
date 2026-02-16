"""
Streamlit Ask AI (Text2SQL BI Agent)
- 자연어 질문 -> LLM(Text2SQL) -> SQL Guard -> DuckDB 실행 -> 결과 테이블/차트 표시

핵심 포인트
- 반드시 llm/sql_guard.py를 사용 (import 경로 고정)
- LLM이 코드블록/세미콜론을 뱉어도 extract + guard로 흡수
"""

from __future__ import annotations

import re
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

from llm.text2sql import generate_sql
from llm.sql_guard import validate_sql, SQLGuardError


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_PATH = PROJECT_ROOT / "data" / "portfolio.duckdb"


# -----------------------
# 유틸
# -----------------------

def extract_sql(text: str) -> str:
    """
    LLM 응답에서 SQL만 추출.
    - ```sql ... ``` 형태면 내부만 꺼냄
    - 아니면 전체를 SQL로 간주
    """
    if not text:
        return ""
    m = re.search(r"```sql\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    if m:
        return m.group(1).strip()
    return text.replace("```", "").strip()


@st.cache_resource
def get_con() -> duckdb.DuckDBPyConnection:
    if not DUCKDB_PATH.exists():
        raise RuntimeError(f"DuckDB 파일이 없습니다: {DUCKDB_PATH}")
    return duckdb.connect(str(DUCKDB_PATH), read_only=True)


def render_line_chart_if_possible(df: pd.DataFrame) -> None:
    """
    결과가 (date-like column + numeric column) 형태면 라인차트도 추가로 그림
    """
    if df is None or df.empty:
        return

    # date 컬럼 후보
    date_cols = [c for c in df.columns if "date" in c.lower() or "time" in c.lower()]
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

    if not date_cols or not num_cols:
        return

    x = date_cols[0]
    y = num_cols[0]

    try:
        # 스트림릿은 x축 dtype이 datetime이면 더 예쁘게 나옴
        df2 = df.copy()
        df2[x] = pd.to_datetime(df2[x], errors="ignore")
        st.line_chart(df2.set_index(x)[y])
    except Exception:
        # 차트 실패해도 테이블은 보여주면 됨
        pass


# -----------------------
# UI
# -----------------------

st.set_page_config(page_title="Ask AI (Text2SQL BI Agent)", layout="wide")
st.title("Ask AI (Text2SQL BI Agent)")

q = st.text_input("자연어로 질문하세요", value="2026-02-15에서 2026-02-19까지 캠페인별 ROAS 상위 10개 보여줘")
run = st.button("Ask AI")

if run:
    try:
        con = get_con()

        # 1) LLM으로 SQL 생성
        t2s = generate_sql(q)
        llm_sql = extract_sql(t2s.sql)

        st.subheader("LLM Generated SQL")
        st.code(llm_sql, language="sql")

        # 2) Guard로 검증/정규화
        safe_sql = validate_sql(llm_sql)

        st.subheader("Validated SQL")
        st.code(safe_sql, language="sql")

        # 3) 실행
        df = con.execute(safe_sql).df()

        st.subheader("Result")
        st.dataframe(df, use_container_width=True)

        # 4) 차트(가능하면)
        st.subheader("Chart (auto)")
        render_line_chart_if_possible(df)

    except SQLGuardError as e:
        st.error(f"에러 발생: {str(e)}")

    except Exception as e:
        st.error(f"실행 중 오류: {repr(e)}")
