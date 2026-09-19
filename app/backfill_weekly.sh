#!/bin/sh
# 컨테이너 안에서 주간 보고서 여러 주를 생성(없을 때만)하고 Redis(trading:reports)에 발행.
#   usage (WSL): docker cp app/backfill_weekly.sh news-scrap:/tmp/ && docker exec news-scrap sh /tmp/backfill_weekly.sh 2026-W36 2026-W37
#   결과 md: 컨테이너 /tmp/weekly_<W>.md  (docker cp 로 꺼내 reversion_research/reports/weekly/ 에 보관)
cd /app || exit 1
for W in "$@"; do
  f="/tmp/weekly_$W.md"
  if [ ! -s "$f" ]; then
    echo "== generate $W"
    python weekly_report.py "$W" > "$f" 2> "/tmp/weekly_$W.err" || { echo "FAIL $W"; tail -3 "/tmp/weekly_$W.err"; continue; }
  else
    echo "== reuse $W ($(wc -c < "$f") bytes)"
  fi
  python report_store.py publish weekly "$W" < "$f"
done
echo "== list"
python report_store.py list
