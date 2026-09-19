# -*- coding: utf-8 -*-
"""트레이딩봇 주간 보고서 (2026-09-19 도입) — ISO 주(월~일, KST) 셀별 실측 + 구독 Opus 셀별 평가.

월간 보고서(monthly_report.py)와 같은 원천(Supabase trade_records)·같은 셀 정의(계좌×심볼×책×전략)를 쓰되,
주간의 역할은 '판정'이 아니라 조기 경보다 — 1개월 표본도 노이즈라는 합의 원칙에 따라 파라미터 변경은
월간(🔴 2개월 연속 / 백테스트 최악연도 초과)에서만 다룬다. 주간은
  ① 계좌 주간 요약(4주 추이) ② 셀별 주간 실측(+4주·월누적·무진입 연속) ③ 월 누적 판정 프리뷰
  ④ 🧠 Opus 셀별 평가(국면 변화 / 엣지 소멸 / 운영 결함 구분, 계좌별 1콜) 을 낸다.

usage: python weekly_report.py [2026-W37 | 2026-09-10] [--no-llm]
       (인자 없음 = 직전 완료 주. 진행 중인 주를 지정하면 '진행중' 표기)
발행: main.py scheduled_weekly_report(월 07:30) → 텔레그램 + report_store(weekly:{label})
"""
import json
import logging
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta

from monthly_report import EXPECTED, KST, book_of, fetch_rows
from persist import get_supabase

log = logging.getLogger("weekly_report")

FEE_PCT = 0.11          # 수수료 왕복 근사 (월간과 동일)
TREND_WEEKS = 4         # 추이 표시 주 수
LOOKBACK_WEEKS = 12     # 무진입 연속 판정 창

STRAT_KO = {
    "S1": "추세(구채널)", "S2": "역추세(구채널)", "S3": "추세(일봉)", "S4": "역추세(일봉)",
    "S11": "추세추종", "S12": "역추세(급락매수)", "S13": "급락페이드(시간청산 위주)",
    "S14": "ewz추세", "S15": "유동성스윕",
}
ACCOUNT_KO = {"BYBIT": "코인(Bybit)", "MT5": "CFD(MT5)"}

EVAL_SYSTEM = """너는 자동매매 봇의 주간 성과 검토자다. 입력은 셀(계좌×심볼×책×전략)별 이번 주·직전 3주·월 누적 실측과 백테스트 기대값(있을 때)이다. 한국어로 답한다.

운영 원칙(사용자와 합의됨, 반드시 지킬 것):
- 1개월 표본도 노이즈다. 파라미터 변경은 월간 보고서에서 🔴가 2개월 연속이거나 백테스트 최악연도를 넘길 때만 검토한다. 주간 평가에서는 절대 파라미터 변경·셀 중단을 지시하지 않는다.
- 주간의 역할은 조기 경보다: (a) 국면(regime) 변화 vs 엣지 소멸 vs 운영 결함(피드 끊김·청산 누락)을 구분하고, (b) 월간 판정 후보를 미리 표시한다.
- 무거래 셀은 발동조건 미도달이면 정상이다. 단, 평소 매주 진입하던 셀이 2주 이상 0건이면 피드/구독 결함을 의심한다(2026-09-16 MT5 지수 3종이 08-27부터 틱 미수신으로 신호 0건이던 실사고).
- 미청산이 많은 셀은 결과 미확정이다. 청산%만으로 단정하지 않는다.
- 전략 의미: S11/S3 추세추종, S12/S4 역추세(급락매수), S13 급락페이드(1분, 시간청산 위주라 되돌림 못 잡으면 반납), S14 ewz추세, S15 유동성스윕, S1/S2 구채널(잔여 드레인 중 — 조정 대상 아님). 책: 1분/4h/일봉(만기가 길수록 표본이 적고 미청산이 많다).
- 같은 계좌의 다른 셀과 같은 주에 같은 방향으로 손실이 몰렸다면 개별 셀 문제가 아니라 국면(급락 클러스터 등)일 가능성을 먼저 본다.

판정 등급(grade):
- 정상: 기대 범위 내이거나 표본 부족(청산 3건 미만)이며 이상 징후 없음
- 관찰: 손실이지만 국면으로 설명되거나 표본이 작음
- 주의: 월누적 🔴 후보(합계 ≤ -3%p, 청산 3건 이상)이거나 4주 연속 손실·승률 급락 등 엣지 소멸이 의심됨
- 점검: 운영 결함 의심(무진입 연속, 진입만 있고 청산이 만기 이상 없음 등)

각 셀마다 grade, reason(한 문장, 반드시 숫자 인용), action(다음 중 하나로 시작: "없음" / "관찰 유지" / "월간에서 재검토 후보" / "운영 점검: <무엇을>")을 쓴다.
key는 입력의 key를 그대로 돌려준다. 입력에 없는 셀을 만들지 않는다. 마지막 summary는 이 계좌의 총평 2~3문장(이번 주 특징, 가장 신경 쓸 셀 1~2개)."""


def _eval_schema() -> dict:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "cells": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "key": {"type": "string"},
                        "grade": {"type": "string", "enum": ["정상", "관찰", "주의", "점검"]},
                        "reason": {"type": "string"},
                        "action": {"type": "string"},
                    },
                    "required": ["key", "grade", "reason", "action"],
                },
            },
            "summary": {"type": "string"},
        },
        "required": ["cells", "summary"],
    }


# ───────────────────────────────────────────────────────────
# 주 범위
# ───────────────────────────────────────────────────────────
def parse_week(arg: str | None):
    """→ (label '2026-W37', week_start(KST 월 00:00), week_end(배타, 다음 월 00:00))"""
    now = datetime.now(KST)
    if not arg:
        d = (now - timedelta(days=now.weekday() + 7)).date()      # 직전 완료 주의 월요일
    elif "W" in arg.upper():
        y, w = arg.upper().split("-W")
        d = date.fromisocalendar(int(y), int(w), 1)
    else:
        d0 = date.fromisoformat(arg)
        d = d0 - timedelta(days=d0.weekday())
    start = datetime(d.year, d.month, d.day, tzinfo=KST)
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}", start, start + timedelta(days=7)


def _monday(dt: datetime) -> date:
    d = dt.date()
    return d - timedelta(days=d.weekday())


def _row_dt(raw: dict, r: dict) -> datetime | None:
    try:
        ms = int(raw.get("ts_ms") or 0)
    except (TypeError, ValueError):
        ms = 0
    if ms > 0:
        return datetime.fromtimestamp(ms / 1000, tz=KST)
    try:  # ts_ms 없는 구행: day 00:00 KST
        return datetime.fromisoformat(str(r.get("day"))).replace(tzinfo=KST)
    except Exception:
        return None


def _cell_key(r: dict, raw: dict):
    account = str(raw.get("account") or "BYBIT")
    sym = str(r.get("symbol") or "?")
    tag = str(raw.get("strategy_tag") or (str(r.get("signal") or "").split("_")[0]) or "?").upper()
    if not tag.startswith("S"):
        tag = "?"
    return (account, sym, book_of(raw.get("signal_ns") or ""), tag)


def _exit_pct(r: dict, raw: dict) -> float | None:
    try:
        ep, xp = float(raw.get("entry_price") or 0), float(raw.get("exit_price") or 0)
    except (TypeError, ValueError):
        return None
    if ep <= 0 or xp <= 0:
        return None
    pct = (xp / ep - 1) * 100
    if str(r.get("side") or "").upper() == "SHORT":
        pct = -pct
    return pct - FEE_PCT


def _new_stat():
    return dict(entries=0, exits=0, wins=0, pcts=[], usdt=0.0, open=0)


def _add_exit(st: dict, r: dict, raw: dict):
    st["exits"] += 1
    pct = _exit_pct(r, raw)
    if pct is not None:
        st["pcts"].append(pct)
        if pct > 0:
            st["wins"] += 1
    try:
        st["usdt"] += float(r.get("pnl"))
    except (TypeError, ValueError):
        pass


def _merge(stats: list[dict]) -> dict:
    out = _new_stat()
    for s in stats:
        out["entries"] += s["entries"]
        out["exits"] += s["exits"]
        out["wins"] += s["wins"]
        out["pcts"] += s["pcts"]
        out["usdt"] += s["usdt"]
        out["open"] += s["open"]
    return out


def _wr(st: dict):
    return (100.0 * st["wins"] / len(st["pcts"])) if st["pcts"] else None


def _avg(st: dict):
    return (sum(st["pcts"]) / len(st["pcts"])) if st["pcts"] else None


def _f(v, fmt="{:+.1f}", empty=""):
    return empty if v is None else fmt.format(v)


def _money(account: str, v: float, has: bool) -> str:
    if not has:
        return "—"
    return f"{v:+.2f} {'USDT' if account == 'BYBIT' else 'USD'}"


def _sanitize(s: str) -> str:
    return str(s or "").replace("|", "/").replace("\n", " ").strip()


# ───────────────────────────────────────────────────────────
# 본체
# ───────────────────────────────────────────────────────────
def build(label: str, ws: datetime, we: datetime, *, use_llm: bool = True) -> str:
    now = datetime.now(KST)
    partial = we > now
    sb = get_supabase()
    fetch_from = (ws - timedelta(weeks=LOOKBACK_WEEKS) - timedelta(days=1)).date().isoformat()
    fetch_to = (max(we, now) + timedelta(days=1)).date().isoformat()   # 미청산 판정용으로 현재까지
    rows = fetch_rows(sb, fetch_from, fetch_to)

    weeks = [(_monday(ws) - timedelta(weeks=i)) for i in range(LOOKBACK_WEEKS)]   # [이번주, 지난주, ...]
    this_wk = weeks[0]
    trend = weeks[:TREND_WEEKS]
    month_start = datetime((we - timedelta(days=1)).year, (we - timedelta(days=1)).month, 1, tzinfo=KST)
    month_label = month_start.strftime("%Y-%m")

    cell_wk = defaultdict(lambda: defaultdict(_new_stat))     # cell -> monday -> stat
    acct_wk = defaultdict(lambda: defaultdict(_new_stat))     # account -> monday -> stat
    cell_mtd = defaultdict(_new_stat)
    acct_has_money = defaultdict(bool)
    exit_lots: set[str] = set()
    week_entries: list[tuple[tuple, str]] = []                 # (cell, lot_id) 이번 주 진입

    for r in rows:
        raw = r.get("raw_json") or {}
        if not isinstance(raw, dict):
            raw = {}
        kind = str(r.get("kind") or "").upper()
        lot = str(raw.get("lot_id") or "")
        if kind == "EXIT" and lot:
            exit_lots.add(lot)            # 현재까지의 모든 청산 (미청산 판정)
        dt = _row_dt(raw, r)
        if dt is None or dt >= we or dt < ws - timedelta(weeks=LOOKBACK_WEEKS):
            continue
        cell = _cell_key(r, raw)
        wk = _monday(dt)
        in_mtd = dt >= month_start
        if kind == "ENTRY":
            cell_wk[cell][wk]["entries"] += 1
            acct_wk[cell[0]][wk]["entries"] += 1
            if in_mtd:
                cell_mtd[cell]["entries"] += 1
            if wk == this_wk:
                week_entries.append((cell, lot))
        elif kind == "EXIT":
            _add_exit(cell_wk[cell][wk], r, raw)
            _add_exit(acct_wk[cell[0]][wk], r, raw)
            if in_mtd:
                _add_exit(cell_mtd[cell], r, raw)
            if r.get("pnl") is not None:
                acct_has_money[cell[0]] = True

    for cell, lot in week_entries:
        if lot and lot not in exit_lots:
            cell_wk[cell][this_wk]["open"] += 1

    # 표시 셀: 4주 내 활동 or (최근 8주 내 진입 이력이 있는데 이번 주 무진입 → 피드 결함 조기 경보용)
    #   구채널·태그 미상(책 '?', 태그 '?'/'SCALE')은 이번 주 활동이 있을 때만 — 드레인 잔여가 표·평가를 오염시키지 않게
    STALE_MAX_WEEKS = 8
    cells = []
    for cell, by_wk in cell_wk.items():
        _, _, book, tag = cell
        recent = _merge([by_wk[w] for w in trend if w in by_wk])
        this = by_wk.get(this_wk) or _new_stat()
        legacy = book == "?" or tag in ("?", "SCALE")
        if legacy and not (this["entries"] or this["exits"]):
            continue
        streak = 0
        for w in weeks:
            if by_wk.get(w, {}).get("entries", 0) > 0:
                break
            streak += 1
        had_entries = any(by_wk[w]["entries"] > 0 for w in by_wk)
        if not had_entries:
            streak = 0
        if recent["entries"] or recent["exits"] or (1 <= streak <= STALE_MAX_WEEKS):
            cells.append((cell, by_wk, recent, streak))

    # ── 렌더 ──
    out = []
    rng = f"{ws:%m-%d} ~ {(we - timedelta(days=1)):%m-%d}"
    out.append(f"# 트레이딩봇 주간 보고서 — {label} ({rng}){' · 진행중' if partial else ''}")
    llm_note = ""
    if use_llm:
        try:
            import llm
            ok, why = llm.claude_available()
            llm_note = f" · 평가: {'Claude ' + llm.model_for('weekly_eval') if ok else 'OpenAI 폴백(' + why + ')'}"
        except Exception:
            llm_note = " · 평가: 불가"
    out.append(f"\n생성: {now:%Y-%m-%d %H:%M} KST · 원천: Supabase trade_records {len(rows)}행({LOOKBACK_WEEKS}주 창)"
               f"{llm_note}{' · ⚠️ 주 미완료(' + now.strftime('%m-%d %H:%M') + ' 기준)' if partial else ''}\n")

    # 1. 계좌 주간 요약
    out.append(f"## 1. 계좌 주간 요약 (최근 {TREND_WEEKS}주)\n")
    out.append("| 주 | 계좌 | 진입 | 청산 | 승률 | 합계%p | 실현 |")
    out.append("|---|---|---|---|---|---|---|")
    for w in trend:
        wl = f"{w.isocalendar()[0]}-W{w.isocalendar()[1]:02d}"
        for a in sorted(acct_wk):
            s = acct_wk[a].get(w) or _new_stat()
            mark = "**" if w == this_wk else ""
            out.append(f"| {mark}{wl}{mark} | {a} | {s['entries']} | {s['exits']} | {_f(_wr(s), '{:.0f}%')} | "
                       f"{sum(s['pcts']):+.1f} | {_money(a, s['usdt'], acct_has_money[a])} |")
    out.append("\n> MT5 금액은 2026-09-18 브로커 원장(딜) 기준 정정 이후 값. %는 진입/청산가 기반(수수료 0.11% 차감).")

    # 2. 셀별 주간 실측
    out.append(f"\n## 2. 셀별 주간 실측 (청산 기준 · 월누적={month_label})\n")
    out.append("| 계좌 | 심볼 | 책 | 태그 | 진입 | 청산 | 미청산 | 승률 | 건당% | 합계%p | 4주%p | 4주n | 월누적%p | 월누적n | 무진입 |")
    out.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    cells.sort(key=lambda c: -sum(c[1].get(this_wk, _new_stat())["pcts"]))
    llm_input = defaultdict(list)
    for cell, by_wk, recent, streak in cells:
        a, sym, book, tag = cell
        s = by_wk.get(this_wk) or _new_stat()
        m = cell_mtd.get(cell) or _new_stat()
        series = [round(sum(by_wk[w]["pcts"]), 2) if w in by_wk else 0.0 for w in trend]
        out.append(f"| {a} | {sym} | {book} | {tag} | {s['entries']} | {s['exits']} | {s['open'] or ''} | "
                   f"{_f(_wr(s), '{:.0f}%')} | {_f(_avg(s), '{:+.2f}%')} | {sum(s['pcts']):+.1f} | "
                   f"{sum(recent['pcts']):+.1f} | {recent['exits']} | {sum(m['pcts']):+.1f} | {m['exits']} | "
                   f"{(str(streak) + '주') if streak >= 1 else ''} |")
        llm_input[a].append({
            "key": "|".join(cell), "symbol": sym, "book": book, "tag": tag,
            "strategy": STRAT_KO.get(tag, tag),
            "this_week": {"entries": s["entries"], "exits": s["exits"], "open_from_this_week": s["open"],
                          "win_rate": None if _wr(s) is None else round(_wr(s), 1),
                          "avg_pct": None if _avg(s) is None else round(_avg(s), 2),
                          "sum_pct": round(sum(s["pcts"]), 2)},
            "weekly_sum_pct_recent_first": series,
            "weekly_exits_recent_first": [by_wk[w]["exits"] if w in by_wk else 0 for w in trend],
            "mtd": {"exits": m["exits"], "win_rate": None if _wr(m) is None else round(_wr(m), 1),
                    "sum_pct": round(sum(m["pcts"]), 2)},
            "expected_pct_per_trade": EXPECTED.get((sym, book, tag)),
            "weeks_without_entry": streak,
        })

    # 3. 월 누적 판정 프리뷰
    out.append(f"\n## 3. 월 누적 판정 프리뷰 ({month_label} 1일 ~ {(we - timedelta(days=1)):%m-%d})\n")
    flags = []
    for cell, m in cell_mtd.items():
        n, avg, wr, tot = m["exits"], _avg(m), _wr(m), sum(m["pcts"])
        if n < 3 or avg is None:
            continue
        a, sym, book, tag = cell
        exp = EXPECTED.get((sym, book, tag))
        if tot < -3:
            flags.append(f"- 🔴 {a} {sym} {book} {tag}: 월누적 {tot:+.1f}%p (n={n}, 승률 {wr:.0f}%) — 월간 🔴 후보")
        elif wr is not None and wr < 40:
            flags.append(f"- 🟡 {a} {sym} {book} {tag}: 승률 {wr:.0f}% (n={n}) — 저승률")
        elif exp and avg < exp * 0.3:
            flags.append(f"- 🟡 {a} {sym} {book} {tag}: 건당 {avg:+.2f}% vs 기대 {exp:+.2f}% — 기대미달")
    out.extend(flags if flags else ["- 없음 — 월누적 기준 전 셀 기준 내(청산 3건 이상 셀 대상)"])

    # 4. Opus 평가
    out.append("\n## 4. 🧠 셀별 평가 (구독 Claude)\n")
    if not use_llm:
        out.append("- 평가 생략(--no-llm)")
    elif not llm_input:
        out.append("- 평가 대상 셀 없음")
    else:
        out.extend(_evaluate(label, rng, partial, llm_input))

    out.append("\n> 판단 가이드: 주간은 조기 경보용 — 파라미터 변경은 월간 보고서(🔴 2개월 연속 / 백테스트 최악연도 초과)에서만. "
               "'점검'은 운영 결함(피드·청산) 의심이라 즉시 확인, '주의'는 월간 재검토 후보.")
    return "\n".join(out)


def _evaluate(label: str, rng: str, partial: bool, llm_input: dict) -> list[str]:
    import llm
    out = []
    for account in sorted(llm_input):
        cells = llm_input[account]
        user = json.dumps({
            "week": label, "range_kst": rng, "week_in_progress": partial,
            "account": account, "account_desc": ACCOUNT_KO.get(account, account),
            "cells": cells,
        }, ensure_ascii=False)
        out.append(f"### {ACCOUNT_KO.get(account, account)}\n")
        try:
            data = llm.structured("weekly_eval", user, _eval_schema(), system=EVAL_SYSTEM)
        except Exception as e:  # noqa: BLE001
            log.exception("weekly_eval 실패 account=%s", account)
            out.append(f"- ⚠️ 평가 실패: {str(e)[:200]}\n")
            continue
        by_key = {str(c.get("key")): c for c in (data.get("cells") or []) if isinstance(c, dict)}
        out.append("| 심볼 | 책 | 태그 | 판정 | 근거 | 액션 |")
        out.append("|---|---|---|---|---|---|")
        icon = {"정상": "✅", "관찰": "👀", "주의": "🟠", "점검": "🔧"}
        for c in cells:
            ev = by_key.get(c["key"])
            if not ev:
                out.append(f"| {c['symbol']} | {c['book']} | {c['tag']} | — | (평가 누락) | |")
                continue
            g = str(ev.get("grade") or "")
            out.append(f"| {c['symbol']} | {c['book']} | {c['tag']} | {icon.get(g, '')} {g} | "
                       f"{_sanitize(ev.get('reason'))} | {_sanitize(ev.get('action'))} |")
        if data.get("summary"):
            out.append(f"\n**총평:** {_sanitize(data['summary'])}\n")
    return out


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    use_llm = "--no-llm" not in sys.argv
    label, ws, we = parse_week(args[0] if args else None)
    print(build(label, ws, we, use_llm=use_llm))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)
    main()
