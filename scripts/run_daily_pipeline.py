"""
Daily Pipeline Orchestrator (Stage 1)

한 번의 커맨드로 아래를 수행:
1) Synthetic CSV 생성 (옵션)
2) DuckDB 초기화/테이블 생성 (옵션)
3) CSV -> Raw 적재
4) Mart 빌드
5) DQ 리포트 생성

실행 예:
- python scripts/run_daily_pipeline.py --date 2026-02-16 --anomaly
- python scripts/run_daily_pipeline.py --date 2026-02-16 --skip-generate  (이미 CSV가 있을 때)
"""

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from validators.data_quality import run_dq


def sh(cmd: list[str]) -> None:
    """표준 실행 헬퍼: 실행 커맨드를 로그로 남기고 실패 시 즉시 중단."""
    print(f"[RUN] {' '.join(cmd)}")
    subprocess.check_call(cmd)


def ensure_paths() -> None:
    """필수 디렉토리가 없으면 생성(빈 폴더로 인한 혼선을 줄임)."""
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    Path("data/reports").mkdir(parents=True, exist_ok=True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--rows", type=int, default=20000, help="ad_events rows for generator")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--anomaly", action="store_true", help="Inject anomaly scenario")
    p.add_argument("--skip-generate", action="store_true", help="Skip CSV generation if already exists")
    args = p.parse_args()

    ensure_paths()

    py = sys.executable  # venv 활성화 상태면 venv python을 자동 사용

    # 1) generate
    if not args.skip_generate:
        cmd = [
            py, "scripts/generate_realistic_data.py",
            "--date", args.date,
            "--rows", str(args.rows),
            "--seed", str(args.seed),
        ]
        if args.anomaly:
            cmd.append("--anomaly")
        sh(cmd)
    else:
        print("[SKIP] generate_realistic_data.py")

    # 2) init tables + load + rebuild mart
    # - --init: 최초 실행/스키마 변경 시에만 필요하지만, Stage1에서는 단순화를 위해 매일 실행해도 OK
    sh([py, "scripts/build_duckdb.py", "--date", args.date, "--init", "--rebuild-mart"])

    # 3) dq report
    report_path = run_dq(args.date)
    print(f"[OK] DQ report generated: {report_path}")


if __name__ == "__main__":
    main()
