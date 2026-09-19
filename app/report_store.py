# -*- coding: utf-8 -*-
"""트레이딩봇 보고서 저장소 (2026-09-19 도입) — 사이트/앱 열람용.

월간(monthly)·심층(deep)·주간(weekly) 보고서 마크다운을 Upstash Redis 해시 하나에 보관한다.
프론트(hyeongeonnoil /api/reports)와 앱이 같은 키를 읽으므로 반드시 Upstash(redis_client)에 쓴다.

키: trading:reports  (hash)
  field = "{kind}:{label}"          예) monthly:2026-08 / deep:2026-08 / weekly:2026-W38
  value = JSON {id, kind, label, title, generated_at, md}

CLI(백필/수동 발행):  python report_store.py publish <kind> <label> < report.md
                     python report_store.py list
"""
import json
import sys
from datetime import datetime, timedelta, timezone

from redis_client import redis_client  # Upstash — 프론트가 읽는 인스턴스

KST = timezone(timedelta(hours=9))
REPORTS_KEY = "trading:reports"
KINDS = ("monthly", "deep", "weekly")


def _title_of(md: str, fallback: str) -> str:
    for line in (md or "").splitlines():
        s = line.strip()
        if s.startswith("#"):
            return s.lstrip("#").strip() or fallback
    return fallback


def publish_report(kind: str, label: str, md: str, *, title: str | None = None) -> str:
    """보고서 1건 저장(같은 kind:label은 덮어씀). 반환: id."""
    kind = (kind or "").strip().lower()
    if kind not in KINDS:
        raise ValueError(f"kind must be one of {KINDS}: {kind!r}")
    label = (label or "").strip()
    if not label:
        raise ValueError("label required (e.g. 2026-08)")
    md = (md or "").strip()
    if not md:
        raise ValueError("empty markdown")
    rid = f"{kind}:{label}"
    doc = {
        "id": rid,
        "kind": kind,
        "label": label,
        "title": title or _title_of(md, rid),
        "generated_at": datetime.now(KST).isoformat(timespec="seconds"),
        "md": md,
    }
    redis_client.hset(REPORTS_KEY, rid, json.dumps(doc, ensure_ascii=False))
    return rid


def list_reports() -> list[dict]:
    """메타만(md 제외) — label 내림차순, 같은 label은 monthly→deep→weekly."""
    raw = redis_client.hgetall(REPORTS_KEY) or {}
    out = []
    for _, v in raw.items():
        try:
            d = json.loads(v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else v)
        except Exception:
            continue
        d.pop("md", None)
        out.append(d)
    order = {k: i for i, k in enumerate(KINDS)}
    out.sort(key=lambda d: (d.get("label", ""), -order.get(d.get("kind"), 9)), reverse=True)
    return out


def _main(argv: list[str]) -> int:
    if len(argv) >= 1 and argv[0] == "list":
        for d in list_reports():
            print(f"{d['id']:<24} {d.get('generated_at','')}  {d.get('title','')}")
        return 0
    if len(argv) >= 3 and argv[0] == "publish":
        md = sys.stdin.read()
        rid = publish_report(argv[1], argv[2], md)
        print(f"published {rid} ({len(md)} chars)")
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
