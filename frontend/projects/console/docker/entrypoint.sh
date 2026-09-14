#!/bin/sh
set -e

CONFIG_FILE="${EVENTIFY_CONFIG:-/etc/eventify/config.yaml}"
OUTPUT="/usr/share/nginx/html/console/config.json"

if [ ! -f "$CONFIG_FILE" ]; then
  echo '{"mode":"standalone","apps":[]}' > "$OUTPUT"
  exec nginx -g "daemon off;"
fi

apps=$(yq -o=json '.eventify.apps // []' "$CONFIG_FILE")
printf '{"mode":"standalone","apps":%s}' "$apps" > "$OUTPUT"

exec nginx -g "daemon off;"
