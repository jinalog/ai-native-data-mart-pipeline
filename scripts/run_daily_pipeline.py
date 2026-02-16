"""
Daily Pipeline Orchestrator (Stage 1 + LLM Insight)

이 스크립트는 하루 단위 데이터 파이프라인을 오케스트레이션한다.

수행 단계:
1) Synthetic CSV 생성 (date 기반 seed로 재현성 확보)
2) DuckDB 초기화 및 mart 재빌드
3) Data Quality 리포트 생성
4) (옵션) LLM 기반 자동 인사이트 생성

설계 의도:
- 날짜 기반 deterministic seed → 날짜마다 서로 다른 데이터 생성
- 동일 날짜 재실행 시 동일 데이터 생성 (재현성)
- LLM은 후단 모듈로 분리 (관심사 분리)
"""

import argparse
import hashlib
import subprocess
import sys
from pathlib import Path

# 프로젝트 루트 import 경로 확보
# (모듈 실행 시 import 경로 깨지는 문제 방지)
sys.path.append(str(Path(__file__).resolve().parent.parent))

from validators.data_quality import run_dq


# -------------------------------------------------
# 공통 Shell 실행 헬퍼
# -------------------------------------------------
def sh(cmd: list[str]) -> None:
    """
    외부 스크립트를 subprocess로 실행한다.
    실패 시 즉시 예외 발생하여 파이프라인 중단.
    """
    print(f"[RUN] {' '.join(cmd)}")
    subprocess.check_call(cmd)


# -------------------------------------------------
# 날짜 기반 deterministic seed 생성
# -------------------------------------------------
def stable_seed_from_date(date_str: str, modulo: int = 100000) -> int:
    """
    날짜 문자열을 해시하여 항상 동일한 seed를 생성한다.

    목적:
    - 날짜별 서로 다른 데이터 생성
    - 동일 날짜 재실행 시 동일 데이터 재현

    예:
      2026-02-19 → 항상 같은 seed
      2026-02-18 → 다른 seed
    """
    h = hashlib.sha256(date_str.encode("utf-8")).hexdigest()
    return int(h[:8], 16) % modulo


# -------------------------------------------------
# 필수 디렉토리 보장
# -------------------------------------------------
def ensure_paths():
    """
    raw / reports 디렉토리 존재 보장.
    CI나 신규 클론 환경에서 오류 방지 목적.
    """
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    Path("data/reports").mkdir(parents=True, exist_ok=True)


# -------------------------------------------------
# 메인 오케스트레이터
# -------------------------------------------------
def main():
    parser = argparse.ArgumentParser()

    # 실행 날짜 (필수)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")

    # synthetic 데이터 행 수
    parser.add_argument("--rows", type=int, default=20000)

    # seed를 직접 지정하고 싶은 경우 사용
    # (기본은 날짜 기반 자동 seed)
    parser.add_argument("--seed", type=int, default=None)

    # anomaly 시나리오 강제 주입 여부
    parser.add_argument("--anomaly", action="store_true")

    # 이미 raw CSV가 있는 경우 generate skip
    parser.add_argument("--skip-generate", action="store_true")

    # LLM 인사이트 생성 여부
    parser.add_argument("--with-llm", action="store_true")

    args = parser.parse_args()

    ensure_paths()

    # 현재 실행 중인 python (venv 자동 사용)
    py = sys.executable

    # -------------------------------------------------
    # 1) 날짜 기반 seed 계산
    # -------------------------------------------------
    seed = args.seed if args.seed is not None else stable_seed_from_date(args.date)
    print(f"[INFO] Using seed={seed}")

    # -------------------------------------------------
    # 2) Synthetic CSV 생성
    # -------------------------------------------------
    if not args.skip_generate:
        cmd = [
            py,
            "scripts/generate_realistic_data.py",
            "--date", args.date,
            "--rows", str(args.rows),
            "--seed", str(seed),
        ]

        # anomaly 플래그 전달
        if args.anomaly:
            cmd.append("--anomaly")

        sh(cmd)
    else:
        print("[SKIP] CSV generation skipped")

    # -------------------------------------------------
    # 3) DuckDB 적재 + mart 재생성
    # -------------------------------------------------
    # --init: 테이블 자동 생성
    # --rebuild-mart: 날짜 단위 집계 재계산
    sh([
        py,
        "scripts/build_duckdb.py",
        "--date", args.date,
        "--init",
        "--rebuild-mart"
    ])

    # -------------------------------------------------
    # 4) Data Quality Report 생성
    # -------------------------------------------------
    report_path = run_dq(args.date)
    print(f"[OK] DQ report generated: {report_path}")

    # -------------------------------------------------
    # 5) LLM Insight (옵션)
    # -------------------------------------------------
    # 모듈 실행 방식 (-m) 사용:
    # import 경로 충돌 방지
    if args.with_llm:
        sh([py, "-m", "llm.insight_generator", "--date", args.date])


# -------------------------------------------------
# Entry Point
# -------------------------------------------------
if __name__ == "__main__":
    main()