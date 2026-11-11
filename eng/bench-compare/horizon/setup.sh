#!/usr/bin/env bash
# Builds the Laravel + Horizon app (pinned in composer.lock after first run) next to this script: laravel/framework 13.40.0, laravel/horizon 5.50.1, PHP 8.3.
set -euo pipefail; cd "$(dirname "$0")"; export COMPOSER_ALLOW_SUPERUSER=1
C="php ${COMPOSER:-$HOME/sdk/composer/composer.phar}"
if [[ ! -d app/vendor ]]; then
  $C create-project laravel/laravel app "13.*" --no-interaction --no-scripts -q
  (cd app && $C require laravel/horizon:5.50.1 --no-interaction -q && php artisan horizon:install >/dev/null)
fi
cp overlay/horizon.php app/config/horizon.php; cp overlay/queue.php app/config/queue.php; cp overlay/env app/.env
cp -r overlay/app/. app/app/
(cd app && php artisan config:clear >/dev/null)
