#!/bin/sh
# 컨테이너 안에서 주간 보고서 여러 주를 생성(Opus 평가 포함)하고 --publish 로 Redis(trading:reports)에 md+data 발행.
#   usage (WSL): docker cp app/backfill_weekly.sh news-scrap:/tmp/ && docker exec news-scrap sh /tmp/backfill_weekly.sh 2026-W36 2026-W37
#   결과 md: 컨테이너 /tmp/weekly_<W>.md  (docker cp 로 꺼내 reversion_research/reports/weekly/ 에 보관)
cd /app || exit 1
for W in "$@"; do
  echo "== generate+publish $W"
  python weekly_report.py "$W" --publish > "/tmp/weekly_$W.md" 2> "/tmp/weekly_$W.err" || { echo "FAIL $W"; tail -3 "/tmp/weekly_$W.err"; continue; }
  grep 'published weekly' "/tmp/weekly_$W.err" || echo "  (publish 로그 없음)"
done
echo "== list"
python report_store.py list
