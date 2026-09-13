# llm.py — 구조화(JSON) LLM 호출 단일 진입점 (2026-09-12 도입)
#
# 1순위: 구독 Claude(claude -p, 인증 = CLAUDE_CODE_OAUTH_TOKEN, `claude setup-token`으로 발급)
# 2순위: OpenAI gpt-4.1-mini (Claude 실패·한도 소진·토큰 없음 → 자동 폴백, 종전 동작 그대로)
#
# 역할(role)별 모델을 .env로 바꿀 수 있다 — 코드 수정 없이 재시작만:
#   CLAUDE_MODEL=opus                      # 기본값 (opus | sonnet | haiku | 정식 모델ID)
#   CLAUDE_MODEL_SUMMARY=…                 # 나라별 자막 요약 (하루 8회, 자막 3k~50k자)
#   CLAUDE_MODEL_WORLD_MAP=…               # 세계정세 나라별 (하루 8회, 12k~33k자)
#   CLAUDE_MODEL_WORLD_REDUCE=…            # 세계정세 관계 (하루 1회, ~210k자 — 최대 단일 호출)
#   CLAUDE_MODEL_BRIEFING=…                # 전일 브리핑 (하루 1회, ~10k자)
#   CLAUDE_DISABLE=1                       # Claude 전부 끄고 OpenAI만
#   CLAUDE_THINKING_TOKENS=0               # 확장사고 토큰 상한(0=끔, 비우면 CLI 기본)
#   CLAUDE_EFFORT=low|medium|high          # 비우면 CLI 기본
#   CLAUDE_TIMEOUT_SEC=420
#
# 소모량 추적: Redis(뉴스 Upstash) 해시 news:llm:usage:YYYYMMDD 에 역할별 호출수·토큰·폴백 누적(TTL 60일).
#   확인: docker exec news-scrap python -c "import llm; print(llm.usage_report(7))"
#
# ⚠️ `--bare`는 OAuth 토큰을 읽지 않으므로 쓰지 않는다. `--tools ""`로 도구를 끄고 세션을 남기지 않는다.
import os
import json
import time
import shutil
import logging
import subprocess
from pathlib import Path
from datetime import datetime, timedelta

from pytz import timezone
from dotenv import load_dotenv

_HERE = Path(__file__).resolve().parent
load_dotenv(dotenv_path=_HERE / ".env")

log = logging.getLogger(__name__)
SEOUL = timezone("Asia/Seoul")

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

CLAUDE_MODEL_DEFAULT = os.getenv("CLAUDE_MODEL", "opus")
CLAUDE_TIMEOUT_SEC = int(os.getenv("CLAUDE_TIMEOUT_SEC", "420"))
CLAUDE_THINKING_TOKENS = os.getenv("CLAUDE_THINKING_TOKENS", "0")
CLAUDE_EFFORT = os.getenv("CLAUDE_EFFORT", "").strip()
CLAUDE_DISABLE = os.getenv("CLAUDE_DISABLE", "").strip().lower() in ("1", "true", "yes")
# 로컬 개발 PC처럼 /login 세션으로 인증된 경우 토큰 없이도 쓰게 허용
CLAUDE_USE_LOGIN = os.getenv("CLAUDE_USE_LOGIN", "").strip().lower() in ("1", "true", "yes")

USAGE_TTL_SEC = 60 * 86400


class ClaudeError(Exception):
    pass


# ───────────────────────────────────────────────────────────
# 설정 조회
# ───────────────────────────────────────────────────────────
def claude_bin() -> str | None:
    cand = os.getenv("CLAUDE_BIN") or "claude"
    return shutil.which(cand)


def model_for(role: str) -> str:
    return (os.getenv(f"CLAUDE_MODEL_{role.upper()}") or CLAUDE_MODEL_DEFAULT).strip()


def claude_available() -> tuple[bool, str]:
    """(사용가능, 사유)"""
    if CLAUDE_DISABLE:
        return False, "CLAUDE_DISABLE=1"
    if not claude_bin():
        return False, "claude 바이너리 없음"
    if not os.getenv("CLAUDE_CODE_OAUTH_TOKEN") and not CLAUDE_USE_LOGIN:
        return False, "CLAUDE_CODE_OAUTH_TOKEN 없음"
    return True, "ok"


# ───────────────────────────────────────────────────────────
# 소모량 기록 (실패해도 본 흐름에 영향 없음)
# ───────────────────────────────────────────────────────────
def _usage_key(day: datetime | None = None) -> str:
    d = day or datetime.now(SEOUL)
    return f"news:llm:usage:{d.strftime('%Y%m%d')}"


def _usage_incr(role: str, **fields: int):
    try:
        from redis_client import redis_client
        key = _usage_key()
        pipe = redis_client.pipeline()
        for f, v in fields.items():
            if not v:
                continue
            pipe.hincrby(key, f"{role}:{f}", int(v))
            pipe.hincrby(key, f"total:{f}", int(v))
        pipe.expire(key, USAGE_TTL_SEC)
        pipe.execute()
    except Exception as e:  # noqa: BLE001
        log.debug("usage 기록 실패(무시): %s", e)


def usage_report(days: int = 7) -> dict:
    """최근 N일 역할별 소모량 {날짜: {필드: 값}} — 다음 주 추이 확인용."""
    from redis_client import redis_client
    out = {}
    now = datetime.now(SEOUL)
    for i in range(days):
        d = now - timedelta(days=i)
        raw = redis_client.hgetall(_usage_key(d)) or {}
        row = {}
        for k, v in raw.items():
            k = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
            v = v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
            row[k] = int(v)
        if row:
            out[d.strftime("%Y-%m-%d")] = dict(sorted(row.items()))
    return out


# ───────────────────────────────────────────────────────────
# Claude (claude -p)
# ───────────────────────────────────────────────────────────
def _strip_fence(s: str) -> str:
    s = s.strip()
    if s.startswith("```"):
        s = s.split("\n", 1)[1] if "\n" in s else ""
        if s.rstrip().endswith("```"):
            s = s.rstrip()[:-3]
    return s.strip()


# claude 서브프로세스에 넘기는 환경변수 화이트리스트 — 컨테이너의 OPENAI/SUPABASE/KIS/REDIS 키가
# 자식 프로세스로 새지 않게 한다(보안검토 2026-09-13). ANTHROPIC_API_KEY 는 일부러 제외(구독 대신 API 과금 방지).
_ENV_PASS = (
    "PATH", "HOME", "TZ", "LANG", "LC_ALL", "TERM", "TMPDIR", "SHELL",
    # Windows(로컬 테스트)에서 CLI가 자격증명·npm 경로를 찾는 데 필요
    "USERPROFILE", "APPDATA", "LOCALAPPDATA", "HOMEDRIVE", "HOMEPATH", "TEMP", "TMP",
    "SYSTEMROOT", "COMSPEC", "PATHEXT", "PROGRAMFILES", "PROGRAMDATA",
    # Claude CLI
    "CLAUDE_CODE_OAUTH_TOKEN", "CLAUDE_CONFIG_DIR",
)


def _claude_env() -> dict:
    env = {k: v for k, v in os.environ.items() if k in _ENV_PASS or k.upper() in _ENV_PASS}
    if CLAUDE_THINKING_TOKENS != "":
        env["MAX_THINKING_TOKENS"] = CLAUDE_THINKING_TOKENS
    env["DISABLE_AUTOUPDATER"] = "1"
    env["CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC"] = "1"
    return env


def _run_claude(role: str, user: str, schema: dict, system: str | None) -> tuple[dict, dict]:
    model = model_for(role)
    args = [
        claude_bin(), "-p",
        "--model", model,
        "--output-format", "json",
        "--json-schema", json.dumps(schema, ensure_ascii=False),
        "--tools", "",
        "--no-session-persistence",
    ]
    if system:
        args += ["--system-prompt", system]
    if CLAUDE_EFFORT:
        args += ["--effort", CLAUDE_EFFORT]

    env = _claude_env()

    t0 = time.time()
    try:
        proc = subprocess.run(
            args, input=user, capture_output=True, text=True, encoding="utf-8", errors="replace",
            timeout=CLAUDE_TIMEOUT_SEC, env=env, cwd=str(_HERE),
        )
    except subprocess.TimeoutExpired:
        raise ClaudeError(f"timeout {CLAUDE_TIMEOUT_SEC}s (model={model})")
    except (FileNotFoundError, OSError) as e:
        raise ClaudeError(f"실행 실패: {e}")
    dur = time.time() - t0

    out = (proc.stdout or "").strip()
    err = (proc.stderr or "").strip()
    if not out:
        raise ClaudeError(f"exit={proc.returncode} stdout 없음: {err[:300]}")
    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        raise ClaudeError(f"exit={proc.returncode} JSON 아님: {out[:300]}")
    if data.get("is_error"):
        raise ClaudeError(f"is_error subtype={data.get('subtype')}: {str(data.get('result'))[:300]}")

    payload = data.get("structured_output")
    if payload is None:
        try:
            payload = json.loads(_strip_fence(str(data.get("result") or "")))
        except json.JSONDecodeError:
            raise ClaudeError(f"structured_output 없음: {str(data.get('result'))[:300]}")
    if not isinstance(payload, dict):
        raise ClaudeError(f"객체 아님: {str(payload)[:200]}")

    usage = data.get("usage") or {}
    meta = {
        "model": model,
        "in": int(usage.get("input_tokens") or 0),
        "cache": int(usage.get("cache_read_input_tokens") or 0) + int(usage.get("cache_creation_input_tokens") or 0),
        "out": int(usage.get("output_tokens") or 0),
        "cost_usd": float(data.get("total_cost_usd") or 0.0),
        "sec": dur,
    }
    return payload, meta


# ───────────────────────────────────────────────────────────
# OpenAI 폴백
# ───────────────────────────────────────────────────────────
_openai_client = None


def _openai():
    global _openai_client
    if _openai_client is None:
        from openai import OpenAI
        _openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    return _openai_client


def _run_openai(role: str, user: str, schema: dict, system: str | None,
                response_format: dict | None, model: str | None) -> dict:
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": user})
    rf = response_format or {
        "type": "json_schema",
        "json_schema": {"name": role, "strict": True, "schema": schema},
    }
    completion = _openai().chat.completions.create(
        model=model or OPENAI_MODEL, messages=messages, response_format=rf,
    )
    data = json.loads(completion.choices[0].message.content)
    u = getattr(completion, "usage", None)
    _usage_incr(role, openai_calls=1,
                openai_in=int(getattr(u, "prompt_tokens", 0) or 0),
                openai_out=int(getattr(u, "completion_tokens", 0) or 0))
    return data


# ───────────────────────────────────────────────────────────
# 공개 진입점
# ───────────────────────────────────────────────────────────
def structured(role: str, user: str, schema: dict, *, system: str | None = None,
               openai_response_format: dict | None = None, openai_model: str | None = None) -> dict:
    """스키마를 만족하는 dict 반환. Claude → 실패 시 OpenAI. 둘 다 실패면 예외.

    role: summary | world_map | world_reduce | briefing (모델 선택·소모량 집계 키)
    openai_response_format: 폴백 때 종전과 동일한 response_format을 쓰고 싶으면 지정(예: json_object)
    """
    ok, why = claude_available()
    if ok:
        model = model_for(role)
        try:
            payload, m = _run_claude(role, user, schema, system)
            log.info("🤖 CLAUDE OK role=%s model=%s in=%d cache=%d out=%d est=$%.4f %.1fs",
                     role, m["model"], m["in"], m["cache"], m["out"], m["cost_usd"], m["sec"])
            _usage_incr(role, claude_calls=1, claude_in=m["in"], claude_cache=m["cache"],
                        claude_out=m["out"], claude_cost_usd_x1e4=int(round(m["cost_usd"] * 1e4)))
            return payload
        except ClaudeError as e:
            log.warning("⚠️ CLAUDE FAIL role=%s model=%s → OPENAI FALLBACK: %s", role, model, e)
            _usage_incr(role, claude_fail=1, fallback=1)
    else:
        log.info("ℹ️ CLAUDE 미사용(%s) → OPENAI role=%s", why, role)
    return _run_openai(role, user, schema, system, openai_response_format, openai_model)


if __name__ == "__main__":
    # 연결 점검: python llm.py [role]  → 작은 프롬프트 1회 (haiku 권장: CLAUDE_MODEL=haiku python llm.py)
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    r = sys.argv[1] if len(sys.argv) > 1 else "summary"
    print("available:", claude_available(), "model:", model_for(r), "bin:", claude_bin())
    sch = {"type": "object", "properties": {"ok": {"type": "boolean"}, "echo": {"type": "string"}},
           "required": ["ok", "echo"], "additionalProperties": False}
    print(structured(r, "ok에 true, echo에 '연결확인'을 넣어 JSON으로만 답해.", sch,
                     system="너는 JSON만 출력하는 점검 도구다."))
