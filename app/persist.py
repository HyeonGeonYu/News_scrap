# persist.py
import os
import json
from datetime import datetime, time
import logging
from pytz import timezone
from supabase import create_client
from datetime import timedelta
from redis_client import redis_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SEOUL = timezone("Asia/Seoul")


def decode_val(v):
    if isinstance(v, (bytes, bytearray)):
        v = v.decode("utf-8", errors="ignore")

    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return v

    return v


def decode_hash(h):
    out = {}
    for k, v in h.items():
        key = k.decode("utf-8", errors="ignore") if isinstance(k, (bytes, bytearray)) else str(k)
        out[key] = decode_val(v)
    return out


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SECRET_KEY") or os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise RuntimeError("SUPABASE_URL / SUPABASE_SECRET_KEY 환경변수가 필요함")

    return create_client(url, key)


def persist_today_data(dry_run=False):
    supabase = None if dry_run else get_supabase()

    now = datetime.now(SEOUL)

    boundary = time(6, 50)

    today_0650 = SEOUL.localize(datetime.combine(now.date(), boundary))
    day_end = today_0650
    day_start = today_0650 - timedelta(days=1)
    day = day_start.strftime("%Y-%m-%d")

    def is_today_signal(s):
        ts_ms = s.get("ts_ms") or s.get("timestamp_ms")
        if not ts_ms:
            return False

        try:
            ts_ms = int(ts_ms)
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=SEOUL)
            return day_start <= dt <= day_end
        except Exception:
            return False

    log.info("📦 Supabase persist 시작 day=%s dry_run=%s", day, dry_run)

    # 1) 오늘 수집 데이터
    target_day_key = day_start.strftime("%Y%m%d")
    news_key = f"news:daily_saved_data:{target_day_key}"

    news_raw = redis_client.get(news_key)
    news_data = decode_val(news_raw) if news_raw else None

    log.info("news_data exists=%s", bool(news_data))

    # 2) 매매 기록 signals
    signal_key = "trading:bybit:signals"
    signals_raw = redis_client.xrevrange(
        signal_key,
        max="+",
        min="-",
        count=500
    )

    signals = []

    for msg_id, fields in signals_raw:
        item = decode_hash(fields)

        # stream id도 같이 넣어주는게 좋음
        item["_id"] = (
            msg_id.decode() if isinstance(msg_id, (bytes, bytearray)) else str(msg_id)
        )

        signals.append(item)

    before_count = len(signals)
    signals = [s for s in signals if is_today_signal(s)]
    log.info("%s before_count=%d today_count=%d", signal_key, before_count, len(signals))

    # 3) 현재 자산
    # 네 실제 asset key가 다르면 여기만 바꾸면 됨
    asset_key_candidates = ["trading:agent:CopyZannavi:u7c9f14d2a1:BYBIT:asset",
    ]

    asset_data = None
    used_asset_key = None

    for key in asset_key_candidates:
        if redis_client.type(key) == b"hash":
            asset_data = decode_hash(redis_client.hgetall(key))
            used_asset_key = key
            break

    log.info("asset key=%s exists=%s", used_asset_key, bool(asset_data))

    if dry_run:
        log.info("✅ dry-run 완료")
        log.info("news sample=%s", news_data)
        log.info("signals sample=%s", signals[:2])
        log.info("asset sample=%s", asset_data)
        return

    # 1) daily 저장
    supabase.table("daily_collections").upsert({
        "day": day,
        "raw_json": news_data,
        "updated_at": now.isoformat(),
    }).execute()

    log.info("✅ daily_collections 저장 완료")

    # 2) signals 저장
    trade_rows = []

    for idx, s in enumerate(signals):
        if not isinstance(s, dict):
            s = {"raw": s}

        signal_id = s.get("signal_id") or s.get("_id")

        trade_rows.append({
            "id": str(signal_id),
            "day": day,
            "symbol": s.get("symbol"),
            "side": s.get("side"),
            "kind": s.get("kind"),
            "price": s.get("price"),
            "qty": s.get("qty"),
            "pnl": s.get("pnl") or s.get("pnl_pct"),
            "raw_json": s,
        })

    if trade_rows:
        supabase.table("trade_records").upsert(trade_rows).execute()
        log.info("✅ trade_records 저장 완료 count=%d", len(trade_rows))
    else:
        log.info("⏭️ trade_records 저장할 데이터 없음")

    # 3) asset snapshot 저장
    if asset_data:
        if not isinstance(asset_data, dict):
            asset_data = {"raw": asset_data}
        wallet = asset_data.get("wallet.USDT")
        supabase.table("asset_snapshots").upsert({
            "day": day,
            "equity_usdt": wallet,
            "wallet_usdt": wallet,
            "raw_json": asset_data,
        }, on_conflict="day").execute()

        log.info("✅ asset_snapshots 저장 완료")
    else:
        log.info("⏭️ asset snapshot 저장할 데이터 없음")

    log.info("🎉 Supabase persist 완료")


if __name__ == "__main__":
    # 기본은 무조건 dry-run
    DRY_RUN = True
    with_save = False
#    persist_today_data(dry_run=DRY_RUN)
    persist_today_data(dry_run=with_save)