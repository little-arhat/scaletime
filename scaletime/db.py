from contextlib import contextmanager
from io import BytesIO
from typing import Callable, Collection, Iterator

import psycopg2

from pgcopy import CopyManager  # type: ignore


class Db:
    def __init__(self, conn_string: str, retries_on_disconnect: int):
        self._conn_string = conn_string
        self._retries = retries_on_disconnect
        self._conn = psycopg2.connect(self._conn_string)

    def _connect(self) -> None:
        if self._is_closed():
            self._conn = psycopg2.connect(self._conn_string)

    def _close(self) -> None:
        if not self._is_closed():
            self._conn.close()

    def _is_closed(self) -> bool:
        return self._conn.closed != 0

    def commit(self) -> None:
        self._with_reconnects(
            lambda: self._conn.commit(), lambda: None, "perform commit"
        )

    def bulk_insert(
        self,
        table: str,
        columns: Collection[str],
        data: Iterator[Collection[object]],
    ) -> None:
        attempt = 1
        mgr = CopyManager(self._conn, table, columns)
        with BytesIO() as tmp:
            mgr.writestream(data, tmp)

            def ins() -> None:
                tmp.seek(0)
                mgr.copystream(tmp)

            def rc() -> None:
                mgr.conn = self._conn

            self._with_reconnects(ins, rc, f"insert to {table}")

    def _with_reconnects(
        self,
        operation: Callable[[], None],
        reconnect_fn: Callable[[], None],
        msg: str,
    ) -> None:
        attempt = 1
        while True:
            try:
                operation()
                return
            except Exception as e:
                if not self._is_closed():
                    raise e
                elif attempt <= self._retries:
                    attempt += 1
                    self._connect()
                    print(f"Reconnected; attempt={attempt}")
                    reconnect_fn()
                    continue
                else:
                    raise IOError(
                        f"Unable to {msg} after {attempt} attempts"
                    ) from e


CONN_STRING = (
    "postgres://{user}:{passwd}@{host}:{port}/{dbname}?sslmode=require"
)


@contextmanager
def connect(
    user: str,
    passwd: str,
    host: str,
    port: int,
    dbname: str,
    retries_on_disconnect: int = 3,
) -> Iterator[Db]:
    conn_string = CONN_STRING.format(**locals())
    auto_reconnect = Db(conn_string, retries_on_disconnect)
    try:
        yield auto_reconnect
    finally:
        auto_reconnect._close()
