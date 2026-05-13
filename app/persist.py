# persist.py
import os
import json
from datetime import datetime, time
import logging
from pytz import timezone
from supabase import create_client
from datetime import timedelta
from redis_client import redis_client
import requests


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

def extract_position_symbols(asset_data):
    if not isinstance(asset_data, dict):
        return []

    symbols = []
    for key, value in asset_data.items():
        if not str(key).startswith("positions."):
            continue

        symbol = str(key).replace("positions.", "").upper()
        pos = value if isinstance(value, dict) else {}

        has_position = False
        for side in ("LONG", "SHORT"):
            side_pos = pos.get(side)
            if side_pos and float(side_pos.get("qty") or 0) != 0:
                has_position = True
                break

        if has_position:
            symbols.append(symbol)

    return sorted(set(symbols))


def fetch_bybit_last_close(symbol, end_dt):
    """
    day_end 직전 마지막 1분봉 close 조회.
    end_dt는 timezone-aware KST datetime.
    """
    end_ms = int(end_dt.timestamp() * 1000)
    start_ms = end_ms - 2 * 60 * 60 * 1000  # 마지막 2시간만 조회

    url = "https://api.bybit.com/v5/market/kline"
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": "1",
        "start": str(start_ms),
        "end": str(end_ms),
        "limit": "120",
    }

    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    j = r.json()

    if j.get("retCode") != 0:
        raise RuntimeError(f"Bybit retCode={j.get('retCode')} retMsg={j.get('retMsg')}")

    rows = j.get("result", {}).get("list", []) or []
    parsed = []

    for row in rows:
        try:
            ts_ms = int(row[0])
            close = float(row[4])
            if ts_ms < end_ms:
                parsed.append((ts_ms, close))
        except Exception:
            continue

    if not parsed:
        return None

    parsed.sort(key=lambda x: x[0])
    return parsed[-1][1]


def fetch_close_prices_for_asset(asset_data, day_end):
    symbols = extract_position_symbols(asset_data)
    out = {}

    for symbol in symbols:
        try:
            close = fetch_bybit_last_close(symbol, day_end)
            if close is not None:
                out[symbol] = close
                log.info("✅ close price %s=%s", symbol, close)
            else:
                log.warning("⚠️ close price 없음: %s", symbol)
        except Exception as e:
            log.warning("⚠️ close price 조회 실패 %s: %s", symbol, e)

    return out


def calc_asset_equity(asset_data, close_prices):
    if not isinstance(asset_data, dict):
        return None, None

    wallet = asset_data.get("wallet.USDT")
    try:
        wallet = float(wallet)
    except Exception:
        wallet = None

    if wallet is None:
        return None, None

    unrealized = 0.0

    for key, value in asset_data.items():
        if not str(key).startswith("positions."):
            continue

        symbol = str(key).replace("positions.", "").upper()
        close = close_prices.get(symbol)

        if close is None:
            continue

        pos = value if isinstance(value, dict) else {}

        for side in ("LONG", "SHORT"):
            side_pos = pos.get(side)
            if not side_pos:
                continue

            entries = side_pos.get("entries") or []
            for e in entries:
                try:
                    qty = float(e.get("qty") or 0)
                    entry = float(e.get("price") or 0)
                except Exception:
                    continue

                if side == "LONG":
                    unrealized += (close - entry) * qty
                else:
                    unrealized += (entry - close) * qty

    return wallet + unrealized, unrealized

def load_lots_by_entry_signal_id():
    pattern = "trading:agent:CopyZannavi:u7c9f14d2a1:BYBIT:lot:*"
    out = {}

    try:
        for key in redis_client.scan_iter(match=pattern, count=500):
            if redis_client.type(key) != b"hash":
                continue

            lot = decode_hash(redis_client.hgetall(key))
            entry_signal_id = lot.get("entry_signal_id")
            if not entry_signal_id:
                continue

            out[str(entry_signal_id)] = lot

    except Exception as e:
        log.warning("⚠️ lot keys 읽기 실패: %s", e)

    log.info("✅ lots loaded by entry_signal_id count=%d", len(out))
    return out


def get_latest_thresholds_by_symbol(symbols, namespace="bybit", search_back=500):
    """
    Redis Stream trading:{namespace}:OpenPctLog 에서
    심볼별 가장 최근 new 값을 ma_threshold로 가져옴.
    coin 쪽 /api/thresholds.ts 와 같은 기준.
    """
    wanted = {str(s).upper() for s in symbols if s}
    if not wanted:
        return {}

    stream_key = f"trading:{namespace}:OpenPctLog"
    found = {}

    try:
        rows = redis_client.xrevrange(
            stream_key,
            max="+",
            min="-",
            count=search_back,
        )
    except Exception as e:
        log.warning("⚠️ OpenPctLog 읽기 실패 key=%s err=%s", stream_key, e)
        return {}

    for msg_id, fields in rows:
        item = decode_hash(fields)

        sym = str(
            item.get("sym")
            or item.get("SYM")
            or item.get("symbol")
            or ""
        ).upper()

        if not sym or sym not in wanted or sym in found:
            continue

        raw_new = item.get("new") or item.get("NEW")

        try:
            ma_threshold = float(raw_new)
        except Exception:
            continue

        source_id = msg_id.decode() if isinstance(msg_id, (bytes, bytearray)) else str(msg_id)

        found[sym] = {
            "ma_threshold": ma_threshold,
            "momentum_threshold": ma_threshold / 3,
            "source_id": source_id,
            "raw": item,
        }

        if len(found) >= len(wanted):
            break

    log.info(
        "✅ OpenPctLog thresholds loaded key=%s wanted=%d found=%d",
        stream_key,
        len(wanted),
        len(found),
    )

    return found




def persist_today_data(dry_run=False):
    supabase = None if dry_run else get_supabase()

    now = datetime.now(SEOUL)

    boundary = time(6, 50)

    today_0650 = SEOUL.localize(datetime.combine(now.date(), boundary))

    # 항상 "직전 완료된 06:50"을 day_end로 잡음
    if now >= today_0650:
        day_end = today_0650
    else:
        day_end = today_0650 - timedelta(days=1)

    day_start = day_end - timedelta(days=1)
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

    def is_today_trade_record(r):
        ts_ms = r.get("ts_ms") or r.get("timestamp_ms") or r.get("saved_ts_ms")
        if not ts_ms:
            return False

        try:
            ts_ms = int(float(ts_ms))
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

    # 2) 매매 실행 기록 trade_records
    trade_record_key = "trading:agent:CopyZannavi:u7c9f14d2a1:BYBIT:trade_records"
    trade_records_raw = redis_client.xrevrange(
        trade_record_key,
        max="+",
        min="-",
        count=1000,
    )

    trade_records = []

    for msg_id, fields in trade_records_raw:
        item = decode_hash(fields)

        item["_id"] = (
            msg_id.decode() if isinstance(msg_id, (bytes, bytearray)) else str(msg_id)
        )

        trade_records.append(item)

    before_count = len(trade_records)
    trade_records = [r for r in trade_records if is_today_trade_record(r)]
    log.info("%s before_count=%d today_count=%d", trade_record_key, before_count, len(trade_records))

    signal_symbols = sorted({
        str(r.get("symbol", "")).upper()
        for r in trade_records
        if isinstance(r, dict) and r.get("symbol")
    })

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
        log.info("trade_records sample=%s", trade_records[:2])
        log.info("asset sample=%s", asset_data)
        return

    # 1) daily 저장
    supabase.table("daily_collections").upsert({
        "day": day,
        "raw_json": news_data,
        "updated_at": now.isoformat(),
    }).execute()

    log.info("✅ daily_collections 저장 완료")

    # 2) trade_records 저장
    trade_rows = []

    for idx, r in enumerate(trade_records):
        if not isinstance(r, dict):
            r = {"raw": r}

        signal_id = r.get("signal_id") or r.get("exit_signal_id") or r.get("entry_signal_id") or r.get("_id")
        row_id = str(signal_id or r.get("_id"))

        raw_json = dict(r)

        trade_rows.append({
            "id": row_id,
            "day": day,
            "symbol": r.get("symbol"),
            "side": r.get("side"),
            "kind": r.get("kind"),
            "price": r.get("price") or r.get("exit_price") or r.get("entry_price"),
            "qty": r.get("qty"),

            # ✅ 이제 pnl은 USDT 기준
            "pnl": r.get("pnl_usdt"),

            "raw_json": raw_json,
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

        # ✅ day_end 직전 마지막 close 저장
        close_prices = fetch_close_prices_for_asset(asset_data, day_end)

        # ✅ MA100 envelope 기준값 저장
        asset_symbols = extract_position_symbols(asset_data)
        symbols_for_thresholds = sorted(set(signal_symbols + asset_symbols))
        thresholds = get_latest_thresholds_by_symbol(
            symbols_for_thresholds,
            namespace="bybit",
            search_back=500,
        )

        # ✅ raw_json 안에도 저장해서 프론트에서 바로 사용 가능하게 함
        asset_data["close_prices"] = close_prices
        asset_data["close_price_at"] = day_end.isoformat()
        asset_data["thresholds"] = thresholds
        asset_data["thresholds_source"] = {
            "key": "trading:bybit:OpenPctLog",
            "saved_at": now.isoformat(),
        }

        equity_usdt, unrealized_pnl_usdt = calc_asset_equity(asset_data, close_prices)

        asset_data["equity_usdt"] = equity_usdt
        asset_data["unrealized_pnl_usdt"] = unrealized_pnl_usdt

        supabase.table("asset_snapshots").upsert({
            "day": day,
            "equity_usdt": equity_usdt if equity_usdt is not None else wallet,
            "wallet_usdt": wallet,
            "raw_json": asset_data,
        }, on_conflict="day").execute()

        log.info(
            "✅ asset_snapshots 저장 완료 equity=%s unrealized=%s close_symbols=%d",
            equity_usdt,
            unrealized_pnl_usdt,
            len(close_prices),
        )
    else:
        log.info("⏭️ asset snapshot 저장할 데이터 없음")

    log.info("🎉 Supabase persist 완료")


if __name__ == "__main__":
    # 기본은 무조건 dry-run
    DRY_RUN = True
    with_save = False
#    persist_today_data(dry_run=DRY_RUN)
    persist_today_data(dry_run=with_save)