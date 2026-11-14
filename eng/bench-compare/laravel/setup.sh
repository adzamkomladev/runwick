#!/usr/bin/env bash
# Builds the Laravel app (database queue driver) next to this script: laravel/framework 13.x (pinned by composer.lock after the first run), PHP 8.3
# with pdo_sqlite and pdo_pgsql (apt: php8.3-sqlite3 php8.3-pgsql). Durability knobs are patched into config/database.php from env, so the harness sets them per run.
set -euo pipefail; cd "$(dirname "$0")"; export COMPOSER_ALLOW_SUPERUSER=1
C="php ${COMPOSER:-$HOME/sdk/composer/composer.phar}"
php -r 'exit(extension_loaded("pdo_pgsql") && extension_loaded("pdo_sqlite") ? 0 : 1);' || { echo "need php8.3-pgsql and php8.3-sqlite3" >&2; exit 1; }
[[ -d app/vendor ]] || $C create-project laravel/laravel app "13.*" --no-interaction --no-scripts -q
cp overlay/env app/.env
cp -r overlay/app/. app/app/
# Laravel's sqlite connection takes these keys; the stock file has null (transaction_mode: 'DEFERRED'). Read them from env so each run sets them.
# IMMEDIATE matters under 16 workers: a deferred read-then-write upgrade fails with SQLITE_BUSY without waiting for busy_timeout.
cfg=app/config/database.php
for k in busy_timeout journal_mode synchronous transaction_mode; do
  grep -qE "'$k' => (null|'DEFERRED')," "$cfg" || grep -q "env('DB_${k^^}')" "$cfg" || { echo "config/database.php has no '$k' key to patch" >&2; exit 1; }
  sed -i -E "s#'$k' => (null|'DEFERRED'),#'$k' => env('DB_${k^^}'),#" "$cfg"
done
(cd app && php artisan config:clear >/dev/null && php artisan list --raw | grep -q '^bench:produce')
php app/artisan --version
