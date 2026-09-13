# setup_claude_token.ps1 (2026-09-13)
# One-window flow: run `claude setup-token` (browser approval) -> paste token -> write app/.env -> recreate news-scrap -> self-test.
# Windows PowerShell 5.1 compatible. ASCII only (PS5.1 reads BOM-less files as ANSI).
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)   # ...\News_scrap
$envFile = Join-Path $root "app\.env"

Write-Host "=== [1/4] claude setup-token (approve in the browser; then the token prints below) ===" -ForegroundColor Cyan
claude setup-token
Write-Host ""
$tok = Read-Host "Paste the token (sk-ant-oat...) and press Enter"
$tok = $tok.Trim()
if ($tok -notmatch '^sk-ant-oat[A-Za-z0-9_\-]{20,}$') { throw "Not a token: '$($tok.Substring(0, [Math]::Min(12,$tok.Length)))...'" }

Write-Host "=== [2/4] write CLAUDE_CODE_OAUTH_TOKEN into app\.env (UTF-8, no BOM) ===" -ForegroundColor Cyan
$raw = [System.IO.File]::ReadAllText($envFile)
$raw = [regex]::Replace($raw, '(?m)^CLAUDE_CODE_OAUTH_TOKEN=.*\r?\n?', '')
if ($raw.Length -gt 0 -and -not $raw.EndsWith("`n")) { $raw += "`n" }
$raw += "CLAUDE_CODE_OAUTH_TOKEN=$tok`n"
[System.IO.File]::WriteAllText($envFile, $raw, (New-Object System.Text.UTF8Encoding($false)))
Write-Host "written: $envFile"

Write-Host "=== [3/4] recreate news-scrap container (WSL docker) ===" -ForegroundColor Cyan
$wslRoot = "/mnt/c/" + ($root.Substring(3) -replace '\\', '/')
$cmd = "cd '$wslRoot' && docker rm -f news-scrap >/dev/null 2>&1; docker run -d --name news-scrap --network tradingbot_default --env-file app/.env -e TRADING_REDIS_URL=redis://redis:6379/0 -e TZ=Asia/Seoul --shm-size=1g --restart unless-stopped --gpus all news-scrap && sleep 6 && docker ps --filter name=news-scrap --format '{{.Names}} {{.Status}}'"
wsl -e bash -c $cmd

Write-Host "=== [4/4] self-test inside the container (haiku, 1 tiny call) ===" -ForegroundColor Cyan
wsl -e bash -c "docker exec -e CLAUDE_MODEL=haiku news-scrap python llm.py 2>&1 | tail -4"
Write-Host ""
Write-Host "Expect a line '... CLAUDE OK role=summary model=haiku ...'. If you see 'CLAUDE 미사용' or 'FALLBACK', tell Claude in the chat." -ForegroundColor Yellow
