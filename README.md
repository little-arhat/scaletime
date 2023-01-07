
# Timescale db playground

Ingest data to timescale (cloud) and use it.

## Installation on M1 Mac

`pgcopy` can only use `psycopg2` itself, not `psycopg2-binary` so `psycopg2` has to be built (on m1 macs).

Requirements are `libpq` and `openssl`. `pip` should be able to find `libpq` using `pg_config`, so make sure it's in `$PATH`.
For `openssl` you might have to modify `CFLAGS`: `env CFLAGS="-I/Users/r/.brew/opt/openssl@3/include -L/Users/r/.brew/opt/openssl@3/lib poetry install`,
depending on your `homebrew` setup.
