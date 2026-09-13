# backfill_trade_labels.py
"""
Supabase trade_records의 유실된 전략 라벨(signal="ENTRY"/"EXIT"/null)을
tradingBot SIG 로그(signals*.jsonl) 리플레이로 복원한다.

배경(2026-08-20): 신호 스트림은 Redis 보존 35일이라, persist 시점에 이미 만료됐거나
(구) bybit 단일 채널만 보강하던 시절의 행들은 전략 태그가 소실됨. SIG 로그는
로테이션 없이 전 기간이 남아있는 유일한 영구 사본이므로 여기서 복원한다.

실행 (news-scrap 컨테이너 안, 로그를 /tmp/siglogs 에 복사해 두고):
  python backfill_trade_labels.py --logs /tmp/siglogs --dry-run
  python backfill_trade_labels.py --logs /tmp/siglogs
"""
import argparse
import glob
import json
import os
import re

from persist import get_supabase

GENERIC = {"", "ENTRY", "EXIT", "NONE", "OPEN", "CLOSE"}


def load_sig_map(logs_dir):
    """SIG 라인 리플레이 → signal_id -> {reasons, strategy, kind, side}"""
    out = {}
    files = sorted(glob.glob(os.path.join(logs_dir, "signals*.jsonl")))
    n_lines = 0
    for path in files:
        try:
            f = open(path, encoding="utf-8", errors="ignore")
        except Exception as e:
            print(f"⚠️ open 실패 {path}: {e}")
            continue
        with f:
            for line in f:
                if not line.startswith("SIG "):
                    continue
                try:
                    d = json.loads(line[4:].strip())
                except Exception:
                    continue
                sid = d.get("signal_id")
                if not sid:
                    continue
                n_lines += 1
                out.setdefault(str(sid), {
                    "reasons": d.get("reasons") or [],
                    "strategy": d.get("strategy"),
                    "kind": d.get("kind"),
                    "side": d.get("side"),
                })
    print(f"SIG 로그: 파일 {len(files)}개, 신호 {len(out)}개 (라인 {n_lines})")
    return out


def fetch_generic_rows(sb):
    """signal이 유실(제네릭/null)된 행 전부 — 1000행 페이지네이션."""
    rows = []
    page = 0
    while True:
        q = (sb.table("trade_records")
             .select("id, day, symbol, side, kind, signal, raw_json")
             .order("id")
             .range(page * 1000, page * 1000 + 999))
        data = q.execute().data or []
        rows.extend(data)
        if len(data) < 1000:
            break
        page += 1
    generic = [r for r in rows
               if str(r.get("signal") or "").upper().strip() in GENERIC]
    print(f"전체 {len(rows)}행 중 라벨 유실 {len(generic)}행")
    return generic


def resolve_label(row, sig_map):
    """행의 신호 id들로 로그에서 라벨 복원. 반환 (label, tag) 또는 (None, None)."""
    raw = row.get("raw_json") or {}
    if not isinstance(raw, dict):
        return None, None

    # 1) 자기 신호(EXIT면 청산 사유 포함: 예 "S1_SL") 우선
    for key in ("signal_id", "exit_signal_id"):
        sid = raw.get(key)
        if sid and str(sid) in sig_map:
            s = sig_map[str(sid)]
            label = (s["reasons"][0] if s["reasons"] else None) or s.get("strategy")
            if label:
                tag = s.get("strategy") or str(label).split("_")[0]
                return str(label), str(tag)

    # 2) 진입 신호 태그로 폴백
    for key in ("entry_signal_id", "open_signal_id", "close_open_signal_id"):
        sid = raw.get(key)
        if sid and str(sid) in sig_map:
            s = sig_map[str(sid)]
            tag = s.get("strategy") or (s["reasons"][0] if s["reasons"] else None)
            if tag:
                return str(tag), str(tag)

    return None, None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--logs", default="/tmp/siglogs")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    sig_map = load_sig_map(args.logs)
    sb = get_supabase()
    generic = fetch_generic_rows(sb)

    fixed = missed = 0
    for row in generic:
        label, tag = resolve_label(row, sig_map)
        if not label:
            missed += 1
            continue
        side = str(row.get("side") or "").strip()
        display = f"{label} {side}".strip()
        raw = row.get("raw_json") or {}
        if isinstance(raw, dict):
            raw = dict(raw)
            raw["signal"] = label
            raw["signal_kind"] = label
            raw["display_kind"] = label
            raw["display_label"] = display
            # strategy_tag는 진짜 셀 태그(S1~S15)일 때만 — 구 MA100 시절 사유코드(INIT 등)는 제외
            if tag and re.match(r"^S\d{1,2}$", str(tag).upper()):
                raw["strategy_tag"] = str(tag).upper()
            raw["label_backfilled_from"] = "sig_logs_2026-08-20"
        fixed += 1
        if args.dry_run:
            print(f"[DRY] {row['day']} {row.get('symbol')} {row.get('kind')} -> {label}")
            continue
        sb.table("trade_records").update({
            "signal": label,
            "display_label": display,
            "raw_json": raw,
        }).eq("id", row["id"]).execute()

    print(f"복원 {fixed}행 / 로그에도 없음 {missed}행 (dry_run={args.dry_run})")


if __name__ == "__main__":
    main()
