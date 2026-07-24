#!/usr/bin/env bash
set -Eeuo pipefail

DOMAIN="designvps.cafe24.com"
SNIPPET="/etc/nginx/snippets/aibot-location.conf"
INCLUDE_LINE="    include /etc/nginx/snippets/aibot-location.conf;"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "ERROR: run this script through sudo." >&2
  exit 1
fi

mapfile -t enabled_matches < <(
  {
    grep -lER --include='*' \
      "server_name[[:space:]][^;]*${DOMAIN//./\\.}" \
      /etc/nginx/sites-enabled 2>/dev/null || true
    grep -lER --include='*' \
      "listen[[:space:]]+([^;[:space:]]*:)?80[[:space:]][^;]*default_server" \
      /etc/nginx/sites-enabled 2>/dev/null || true
  } | sort -u
)

if [[ "${#enabled_matches[@]}" -lt 1 ]]; then
  echo "ERROR: no enabled Nginx site was found for $DOMAIN or the HTTP default server." >&2
  printf 'Candidate: %s\n' "${enabled_matches[@]}" >&2
  exit 1
fi

declare -a site_files=()
declare -A seen_files=()
for enabled_file in "${enabled_matches[@]}"; do
  resolved="$(readlink -f "$enabled_file")"
  test -f "$resolved"
  if [[ -z "${seen_files[$resolved]:-}" ]]; then
    site_files+=("$resolved")
    seen_files["$resolved"]=1
  fi
done

backup_dir="$(mktemp -d /tmp/aibot-nginx-sites.XXXXXX)"
snippet_backup="$(mktemp /tmp/aibot-nginx-snippet.XXXXXX)"
snippet_existed=0
for index in "${!site_files[@]}"; do
  cp -a "${site_files[$index]}" "$backup_dir/site-$index"
done
if [[ -f "$SNIPPET" ]]; then
  cp -a "$SNIPPET" "$snippet_backup"
  snippet_existed=1
fi

rollback() {
  for index in "${!site_files[@]}"; do
    cp -a "$backup_dir/site-$index" "${site_files[$index]}"
  done
  if [[ "$snippet_existed" -eq 1 ]]; then
    cp -a "$snippet_backup" "$SNIPPET"
  else
    rm -f "$SNIPPET"
  fi
  nginx -t || true
}

cleanup() {
  rm -r -- "$backup_dir"
  rm -f "$snippet_backup"
}

trap cleanup EXIT

install -d -m 755 /etc/nginx/snippets
cat >"$SNIPPET" <<'NGINX'
location = /ai-bot {
    return 301 /ai-bot/;
}

location ^~ /ai-bot/ {
    # No URI suffix: preserve /ai-bot for Streamlit's baseUrlPath.
    proxy_pass http://127.0.0.1:8501;
    proxy_http_version 1.1;

    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";

    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;

    proxy_buffering off;
    proxy_read_timeout 86400;
    proxy_send_timeout 86400;
}
NGINX
chmod 644 "$SNIPPET"

for site_file in "${site_files[@]}"; do
python3 - "$site_file" "$DOMAIN" "$INCLUDE_LINE" <<'PY'
from __future__ import annotations

import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
domain = sys.argv[2]
include_line = sys.argv[3]
text = path.read_text(encoding="utf-8")

if include_line.strip() in text:
    raise SystemExit(0)

server_starts = list(re.finditer(r"\bserver\s*\{", text))
domain_matches: list[tuple[int, int, str]] = []
default_http_matches: list[tuple[int, int, str]] = []

for start_match in server_starts:
    opening = text.find("{", start_match.start())
    depth = 0
    quote = ""
    escaped = False
    comment = False
    closing = -1

    for index in range(opening, len(text)):
        char = text[index]
        if comment:
            if char == "\n":
                comment = False
            continue
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = ""
            continue
        if char == "#":
            comment = True
        elif char in {"'", '"'}:
            quote = char
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                closing = index
                break

    if closing < 0:
        continue
    block = text[start_match.start() : closing + 1]
    if re.search(
        rf"\bserver_name\s+[^;]*\b{re.escape(domain)}\b[^;]*;",
        block,
    ):
        domain_matches.append((start_match.start(), closing, block))
    if re.search(
        r"\blisten\s+(?:\[[^\]]+\]:)?80\b[^;]*\bdefault_server\b[^;]*;",
        block,
    ):
        default_http_matches.append((start_match.start(), closing, block))

selected = [(start, closing) for start, closing, _ in domain_matches]
selected.extend(
    (start, closing) for start, closing, _ in default_http_matches
)

selected = sorted(set(selected), key=lambda item: item[1], reverse=True)
if not selected:
    raise SystemExit(
        f"no {domain} or HTTP default server block was found in {path}"
    )

updated = text
for _, closing in selected:
    updated = updated[:closing] + include_line + "\n" + updated[closing:]
path.write_text(updated, encoding="utf-8")
PY
done

if ! nginx -t; then
  echo "ERROR: Nginx syntax test failed; restoring previous configuration." >&2
  rollback
  exit 1
fi

systemctl reload nginx

status="$(curl --silent --show-error --output /dev/null \
  --write-out '%{http_code}' \
  --header "Host: $DOMAIN" \
  http://127.0.0.1/ai-bot/)"
if [[ "$status" != "200" ]]; then
  echo "ERROR: local Nginx proxy check returned HTTP $status; restoring configuration." >&2
  echo "Active Nginx server mapping:" >&2
  nginx -T 2>&1 | grep -E \
    '^# configuration file |^[[:space:]]*listen[[:space:]]|^[[:space:]]*server_name[[:space:]]' \
    >&2 || true
  rollback
  systemctl reload nginx
  exit 1
fi

printf 'Nginx /ai-bot proxy configured in %s\n' "${site_files[@]}"
echo "Local proxy verification: HTTP $status."
