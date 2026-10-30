# Store-clock steps for TIME-13 (07 I-9): PostgreSQL with libfaketime preloaded into the server only. The harness
# rewrites /etc/faketimerc ("+0", "+30s", "-30s", ...) through container exec; FAKETIME_NO_CACHE makes every
# clock read observe the current offset.
# The preload is not global: gosu (a Go binary, used by the image entrypoint) deadlocks under libfaketime, so the
# postgres binary is replaced by a wrapper that preloads it. Backends are forked from the postmaster and inherit it.
ARG BASE=postgres:18
FROM ${BASE}
RUN apt-get update \
 && apt-get install -y --no-install-recommends libfaketime \
 && rm -rf /var/lib/apt/lists/* \
 && echo "+0" > /etc/faketimerc && chmod 666 /etc/faketimerc \
 && ln -s "$(dpkg -L libfaketime | grep 'libfaketime.so.1$' | head -1)" /usr/local/lib/libfaketime.so.1 \
 && bin="$(dirname "$(readlink -f "$(command -v postgres)")")" \
 && mv "$bin/postgres" "$bin/postgres.real" \
 && printf '#!/bin/sh\nLD_PRELOAD=/usr/local/lib/libfaketime.so.1 exec %s/postgres.real "$@"\n' "$bin" > "$bin/postgres" \
 && chmod 755 "$bin/postgres"
ENV FAKETIME_TIMESTAMP_FILE=/etc/faketimerc \
    FAKETIME_NO_CACHE=1 \
    FAKETIME_DONT_FAKE_MONOTONIC=1
