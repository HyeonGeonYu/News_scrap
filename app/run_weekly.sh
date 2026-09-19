#!/bin/sh
# 컨테이너 안에서 주간 보고서 1건 생성 + 확인 출력.  usage: sh /tmp/run_weekly.sh 2026-W37 [--no-llm]
# (docker exec 인용부호 중첩을 피하려고 스크립트로 분리 — 로컬 실행용 도구, 스케줄러는 main.py가 담당)
W="$1"; shift
cd /app || exit 1
start=$(date +%s)
python weekly_report.py "$W" "$@" > "/tmp/weekly_$W.md" 2> "/tmp/weekly_$W.err"
rc=$?
echo "EXIT=$rc secs=$(( $(date +%s) - start ))"
echo "cells=$(grep -c '^| BYBIT\|^| MT5' "/tmp/weekly_$W.md")"
sed -n '/^## 4/,$p' "/tmp/weekly_$W.md"
echo ---stderr
grep -v 'HTTP Request' "/tmp/weekly_$W.err" | tail -6
