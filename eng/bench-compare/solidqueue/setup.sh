#!/usr/bin/env bash
# Builds a Rails app with Solid Queue next to this script (app/, git-ignored), pinned below. Needs Ruby + a C toolchain + libpq.
# The queue tables live in the app's primary PostgreSQL database (DATABASE_URL), loaded from Solid Queue's own db/queue_schema.rb.
set -euo pipefail; cd "$(dirname "$0")"
RAILS=8.0.3 SQ=1.2.1
if [[ ! -d app ]]; then
  gem list -i rails -v "$RAILS" >/dev/null || gem install rails -v "$RAILS" --no-document
  rails "_${RAILS}_" new app --database=postgresql --api --skip-git --skip-solid --skip-action-mailer --skip-action-mailbox --skip-action-text \
    --skip-active-storage --skip-action-cable --skip-test --skip-system-test --skip-bootsnap --skip-kamal --skip-thruster --skip-docker \
    --skip-rubocop --skip-brakeman --skip-ci --skip-dev-gems --quiet
  (cd app && bundle add solid_queue --version "$SQ" --quiet && bin/rails solid_queue:install >/dev/null)
fi
cp -r overlay/. app/
(cd app && bin/rails db:create >/dev/null 2>&1 || true; bin/rails runner 'load Rails.root.join("db/queue_schema.rb")' >/dev/null)
