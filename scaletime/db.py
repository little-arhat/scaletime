import argparse

from contextlib import contextmanager
from typing import Any, Callable, Collection, Dict, Iterator

import connectorx as cx  # type: ignore
import pandas as pd
import polars
import psycopg2

from pgcopy import CopyManager  # type: ignore


class Db:
    def __init__(self, conn_string: str, retries_on_disconnect: int):
        self._conn_string = conn_string
        self._retries = retries_on_disconnect
        self._conn = psycopg2.connect(self._conn_string)
        self._mgrs: Dict[str, CopyManager] = {}

    def _connect(self) -> None:
        if self._is_closed():
            self._conn = psycopg2.connect(self._conn_string)

    def _close(self) -> None:
        if not self._is_closed():
            self._conn.close()

    def _is_closed(self) -> bool:
        return self._conn.closed != 0

    def to_pandas_df(self, sql: str, **params: Any) -> pd.DataFrame:
        df: pd.DataFrame = self._read_sql(sql, "pandas", **params)
        return df

    def to_polars_df(self, sql: str, **params: Any) -> polars.DataFrame:
        df: polars.DataFrame = self._read_sql(sql, "polars", **params)
        return df

    def _read_sql(
        self, sql: str, return_type: str = "pandas", **params: Any
    ) -> Any:
        sql = sql
        if params:
            with self._conn.cursor() as cursor:
                b: bytes = cursor.mogrify(sql, params)  # type: ignore
                sql = b.decode("utf-8")
        return cx.read_sql(self._conn_string, sql, return_type=return_type)

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
        mgr = self._copy_mgr(table, columns)

        def ins() -> None:
            mgr.threading_copy(data)

        def rc() -> None:
            mgr.conn = self._conn

        self._with_reconnects(ins, rc, f"insert to {table}")

    def _copy_mgr(self, table: str, columns: Collection[str]) -> CopyManager:
        m = self._mgrs.get(table)
        if m is None:
            self._mgrs[table] = m = CopyManager(self._conn, table, columns)
        return m

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


def connect_string_from_args(arguments: argparse.Namespace) -> str:
    return CONN_STRING.format(
        user=arguments.user,
        passwd=arguments.passwd,
        host=arguments.host,
        port=arguments.port,
        dbname=arguments.dbname,
    )


@contextmanager
def connect(
    arguments: argparse.Namespace,
    retries_on_disconnect: int = 3,
) -> Iterator[Db]:
    conn_string = connect_string_from_args(arguments)
    auto_reconnect = Db(conn_string, retries_on_disconnect)
    try:
        yield auto_reconnect
    finally:
        auto_reconnect._close()
