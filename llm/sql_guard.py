"""
SQL Guard (Production-ish)
- LLM이 생성한 SQL을 실행 전에 검증/정규화
- 목적: Text2SQL을 "안전하게" 운영하기 위한 방어 레이어

지원/정책
- SELECT만 허용
- 멀티 스테이트먼트 차단 (중간 세미콜론/복수 statement)
- 마지막 세미콜론은 자동 제거 (사용자 UX 개선)
- 허용된 스키마/테이블만 접근 가능
- JOIN 기본 금지 (확장 가능)
- 위험 키워드 차단 (DDL/DML, pragma, attach, copy, export 등)
- LIMIT 없으면 자동 삽입
- allowlist 컬럼 정책(옵션) 지원
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable


class SQLGuardError(ValueError):
    """SQL Guard에서 거부할 때 사용하는 예외"""
    pass


# =========================
# 정책 설정 (필요 시 조정)
# =========================

# DuckDB/Superset에서 보통 사용하는 스키마 표기: mart.daily_campaign_kpi
ALLOWED_TABLES = {
    "mart.daily_campaign_kpi",
    "mart_daily_insight",
    "mart_daily_insight_latest",  # 뷰를 만들었다면 허용
}

# 각 테이블별 허용 컬럼(강화 옵션)
# - None이면 컬럼 검증을 건너뜁니다(테이블만 제한).
ALLOWED_COLUMNS = {
    "mart.daily_campaign_kpi": {
        "event_date",
        "campaign_id",
        "impressions",
        "clicks",
        "conversions",
        "ctr",
        "cvr",
        "ad_cost",
        "ad_revenue",
        "payments_total",
        "payments_success",
        "payments_failed",
        "payment_success_rate",
        "pay_amount_success",
    },
    "mart_daily_insight": {
        "event_date",
        "headline",
        "risk_level",
        "summary_md",
        "created_at",
    },
    "mart_daily_insight_latest": {
        "event_date",
        "headline",
        "risk_level",
        "summary_md",
        "created_at",
    },
}

# 차단 키워드 (정규식)
# - DuckDB 기준 위험한 것들 포함
BLOCK_PATTERNS = [
    r"\binsert\b",
    r"\bupdate\b",
    r"\bdelete\b",
    r"\bmerge\b",
    r"\bdrop\b",
    r"\bcreate\b",
    r"\balter\b",
    r"\btruncate\b",
    r"\bgrant\b",
    r"\brevoke\b",
    r"\bcopy\b",
    r"\bexport\b",
    r"\bimport\b",
    r"\battach\b",
    r"\bdetach\b",
    r"\bpragma\b",
    r"\bcall\b",
    r"\bexecute\b",
]

# JOIN은 기본 금지(향후 allowlist join으로 확장 가능)
DISALLOW_JOIN = True

# 결과 제한(기본)
DEFAULT_LIMIT = 1000
MAX_LIMIT = 5000


# =========================
# 유틸
# =========================

def _collapse_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def strip_sql_fence(text: str) -> str:
    """
    LLM 응답이 ```sql ... ``` 형태면 내부만 추출
    """
    if not text:
        return ""
    m = re.search(r"```sql\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    if m:
        return m.group(1).strip()
    # 백틱만 있는 경우도 제거
    text = text.replace("```", "").strip()
    return text


def normalize_sql(sql: str) -> str:
    """
    비교를 위한 정규화(소문자, 공백 정리)
    """
    return _collapse_ws(sql).lower()


def _has_multi_statement(sql: str) -> bool:
    """
    멀티 스테이트먼트 감지:
    - 끝에 하나 있는 세미콜론은 허용(제거)
    - 중간에 세미콜론이 있으면 멀티로 간주
    """
    # 문자열 리터럴 내 ; 는 여기서는 고려하지 않는 단순 구현(운영급은 SQL 파서 권장)
    s = sql.strip()
    if not s:
        return False
    if s.endswith(";"):
        s = s[:-1]
    return ";" in s


def _strip_trailing_semicolon(sql: str) -> str:
    s = sql.strip()
    if s.endswith(";"):
        return s[:-1].rstrip()
    return s


def _extract_table(norm_sql: str) -> str:
    """
    FROM 다음 토큰을 테이블로 간주
    - FROM mart.daily_campaign_kpi
    - FROM mart_daily_insight_latest
    """
    m = re.search(r"\bfrom\s+([a-zA-Z0-9_.]+)", norm_sql)
    if not m:
        raise SQLGuardError("FROM 절이 필요합니다.")
    return m.group(1)


def _extract_limit(norm_sql: str) -> int | None:
    m = re.search(r"\blimit\s+(\d+)", norm_sql)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _ensure_limit(raw_sql: str) -> str:
    norm = normalize_sql(raw_sql)
    if " limit " in norm:
        # 너무 큰 limit은 안전상 제한
        lim = _extract_limit(norm)
        if lim is not None and lim > MAX_LIMIT:
            # limit을 강제로 줄임
            raw_sql = re.sub(r"(?i)\blimit\s+\d+", f"LIMIT {MAX_LIMIT}", raw_sql)
        return raw_sql

    return raw_sql.rstrip() + f" LIMIT {DEFAULT_LIMIT}"


def _extract_selected_columns(norm_sql: str) -> list[str] | None:
    """
    아주 단순한 컬럼 추출:
    - SELECT a, b, SUM(c) AS x ...
    - '*' 사용이면 None 반환(검증 불가 -> 허용/거부 정책 선택 가능)
    """
    # SELECT ... FROM 사이를 뽑는다
    m = re.search(r"\bselect\s+(.*?)\s+\bfrom\b", norm_sql)
    if not m:
        return None
    select_part = m.group(1).strip()
    if select_part == "*" or select_part.startswith("* "):
        return None

    # 쉼표로 나누되, 함수 괄호 내부 쉼표는 단순 무시(완벽하지 않음)
    parts = [p.strip() for p in select_part.split(",")]

    cols: list[str] = []
    for p in parts:
        # AS 별칭 제거
        p = re.sub(r"\bas\s+[a-zA-Z0-9_]+\b", "", p).strip()
        # 함수/연산이면 컬럼명만 최대한 뽑기
        # 예: sum(ad_revenue) -> ad_revenue
        inner = re.findall(r"[a-zA-Z_][a-zA-Z0-9_]*", p)
        # inner에는 select 키워드/함수명도 섞일 수 있음, 여기서는 "컬럼 후보"만 추정
        # 너무 공격적으로 막지 않기 위해 그냥 반환만 하고, allowlist는 완화 적용
        cols.extend(inner)

    # 중복 제거
    uniq = []
    for c in cols:
        if c not in uniq:
            uniq.append(c)
    return uniq


# =========================
# 메인 검증
# =========================

def validate_sql(sql_or_text: str) -> str:
    """
    검증 성공 시 실행 가능한 SQL(raw string)를 반환
    실패 시 SQLGuardError 발생
    """
    if not sql_or_text or not sql_or_text.strip():
        raise SQLGuardError("빈 SQL은 허용되지 않습니다.")

    raw = strip_sql_fence(sql_or_text)
    raw = raw.strip()

    # 멀티 스테이트먼트 차단 (끝 ; 하나는 허용)
    if _has_multi_statement(raw):
        raise SQLGuardError("세미콜론(멀티 스테이트먼트)은 허용되지 않습니다. (끝의 ; 는 제거해서 보내주세요)")

    # 끝 세미콜론은 UX상 자동 제거
    raw = _strip_trailing_semicolon(raw)

    norm = normalize_sql(raw)

    # SELECT만 허용
    if not norm.startswith("select "):
        raise SQLGuardError("SELECT 문만 허용됩니다.")

    # JOIN 금지
    if DISALLOW_JOIN and re.search(r"\bjoin\b", norm):
        raise SQLGuardError("JOIN은 허용되지 않습니다. (필요 시 사전 정의된 뷰를 사용하세요)")

    # 위험 키워드 차단
    for pat in BLOCK_PATTERNS:
        if re.search(pat, norm):
            raise SQLGuardError("DDL/DML 또는 위험한 키워드가 감지되어 차단되었습니다.")

    # 테이블 검증
    table = _extract_table(norm)
    if table not in ALLOWED_TABLES:
        raise SQLGuardError(f"허용되지 않은 테이블 접근: {table}")

    # 컬럼 allowlist(완화 옵션)
    allowed_cols = ALLOWED_COLUMNS.get(table)
    if allowed_cols is not None:
        selected = _extract_selected_columns(norm)
        # '*' 인 경우: 운영에서는 막는게 안전하지만, 지금은 UX 위해 허용하되 경고만(앱에서 표시)
        # -> Guard 레벨에서 막고 싶다면 아래를 raise로 바꾸세요.
        # if selected is None: raise SQLGuardError("SELECT * 는 허용되지 않습니다. 필요한 컬럼을 명시하세요.")
        if selected is not None:
            # selected에 함수명(sum, avg 등)이 섞일 수 있어, "허용 컬럼이 하나라도 포함되는지" 완화 체크
            # 너무 엄격하게 하면 LLM 출력이 자주 막힙니다.
            # 여기서는 "허용되지 않은 명확한 컬럼 토큰"만 차단하는 방식으로 구현
            suspicious = []
            for token in selected:
                # 함수명/키워드는 제외
                if token in {"sum", "avg", "min", "max", "count", "distinct", "case", "when", "then", "else", "end"}:
                    continue
                # 테이블명이 토큰에 들어오기도 해서 제외
                if token in {"select", "from", "where", "between", "and", "or", "order", "by", "limit", "asc", "desc"}:
                    continue

                # 컬럼이 아니라 테이블명일 수도 있음
                if token == table.split(".")[-1]:
                    continue

                if token not in allowed_cols:
                    # 너무 공격적으로 막지 않기 위해, "명백히 컬럼처럼 보이는 토큰"만 수집
                    # (숫자/날짜는 이미 정규식에서 제외됨)
                    suspicious.append(token)

            # suspicious가 너무 많이 나오면 차단
            # (LLM이 이상한 컬럼을 만들거나 system table 접근 시도할 때 잡힘)
            if len(suspicious) >= 3:
                raise SQLGuardError(f"허용되지 않은 컬럼/토큰이 다수 감지되었습니다: {sorted(set(suspicious))[:10]}")

    # LIMIT 보장
    raw = _ensure_limit(raw)

    return raw
