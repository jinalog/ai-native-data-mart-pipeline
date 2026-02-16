"""
Synthetic CSV 생성기 (Stage 1)

핵심 설계:
- raw 로딩 안정성을 위해 event_date는 'YYYY-MM-DD' 문자열로 CSV에 저장합니다. (VARCHAR)
- event_ts는 ISO 형태로 저장되어 DuckDB에서 TIMESTAMP로 캐스팅 가능하게 합니다.
- anomaly 옵션으로 특정 캠페인(C007)에 이상치(매출 급락/결제실패 증가)를 주입할 수 있습니다.

출력:
- data/raw/ad_events_YYYYMMDD.csv
- data/raw/payment_events_YYYYMMDD.csv
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


RAW_DIR = Path("data/raw")


def to_yyyymmdd(date_str: str) -> str:
    return date_str.replace("-", "")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--rows", type=int, default=20000, help="ad_events row count")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--anomaly", action="store_true", help="Inject anomaly scenario")
    args = parser.parse_args()

    np.random.seed(args.seed)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    date_str = args.date
    ds = to_yyyymmdd(date_str)

    # 도메인 느낌만 살리는 최소 차원
    campaigns = [f"C{n:03d}" for n in range(1, 21)]
    ads = [f"A{n:04d}" for n in range(1, 401)]
    os_list = ["iOS", "Android", "Web"]
    countries = ["KR", "JP", "US", "SG", "TW"]

    # 1) ad_events 생성
    n = args.rows
    event_type = np.random.choice(
        ["impression", "click", "conversion"],
        size=n,
        p=[0.88, 0.10, 0.02],
    )
    campaign_id = np.random.choice(campaigns, size=n)
    ad_id = np.random.choice(ads, size=n)
    user_id = np.random.randint(1, 200000, size=n).astype(str)
    device_os = np.random.choice(os_list, size=n, p=[0.45, 0.45, 0.10])
    country = np.random.choice(countries, size=n, p=[0.55, 0.20, 0.10, 0.10, 0.05])

    # 비용/매출 (단순 모델)
    cost = np.where(event_type == "click", np.random.gamma(2.0, 0.3, size=n), 0.0)
    revenue = np.where(event_type == "conversion", np.random.gamma(4.0, 3.0, size=n), 0.0)

    # 이상치: 특정 캠페인(C007) conversion revenue 급락
    if args.anomaly:
        target = "C007"
        mask = (campaign_id == target) & (event_type == "conversion")
        revenue[mask] = revenue[mask] * 0.15

    # timestamp: 하루(0~86400초) 내 분포
    base = pd.Timestamp(date_str)
    seconds = np.random.randint(0, 24 * 3600, size=n)
    event_ts = base + pd.to_timedelta(seconds, unit="s")

    ad_df = pd.DataFrame(
        {
            # 중요: event_date는 문자열로 고정(VARCHAR 타깃)
            "event_date": date_str,
            "event_ts": event_ts,  # pandas가 ISO로 출력 -> DuckDB TIMESTAMP 캐스팅 용이
            "event_type": event_type,
            "campaign_id": campaign_id,
            "ad_id": ad_id,
            "user_id": user_id,
            "device_os": device_os,
            "country": country,
            "cost": cost,
            "revenue": revenue,
        }
    )

    ad_path = RAW_DIR / f"ad_events_{ds}.csv"
    ad_df.to_csv(ad_path, index=False)

    # 2) payment_events 생성
    # conversion 일부를 결제 이벤트로 연결한 느낌(완전 매칭은 아니고 "현실적 근사")
    conv = ad_df[ad_df["event_type"] == "conversion"]
    m = max(int(len(conv) * 0.6), 300)

    pay_campaign = np.random.choice(campaigns, size=m)

    # 이상치: C007 결제 실패율 증가
    if args.anomaly:
        fail_boost = (pay_campaign == "C007")
        fail_prob = np.where(fail_boost, 0.25, 0.06)
    else:
        fail_prob = np.full(m, 0.06)

    status = np.where(np.random.rand(m) < fail_prob, "failed", "success")
    fail_reason = np.where(
        status == "failed",
        np.random.choice(["timeout", "insufficient_funds", "3ds_failed"], size=m),
        "",
    )

    pay_df = pd.DataFrame(
        {
            "event_date": date_str,  # 문자열 고정
            "event_ts": base + pd.to_timedelta(np.random.randint(0, 24 * 3600, size=m), unit="s"),
            "order_id": [f"O{ds}{i:06d}" for i in range(m)],
            "user_id": np.random.randint(1, 200000, size=m).astype(str),
            "campaign_id": pay_campaign,
            "amount": np.round(np.random.gamma(3.0, 10.0, size=m), 2),
            "currency": "KRW",
            "status": status,
            "fail_reason": fail_reason,
        }
    )

    pay_path = RAW_DIR / f"payment_events_{ds}.csv"
    pay_df.to_csv(pay_path, index=False)

    print(f"[OK] generated: {ad_path}")
    print(f"[OK] generated: {pay_path}")


if __name__ == "__main__":
    main()