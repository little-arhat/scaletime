from datetime import date, datetime

from . import args, db


def raw_pricing_size() -> None:
    query = "SELECT pg_size_pretty(hypertable_size('raw_pricing')) as size"
    with db.connect(args.create_arg_parser().parse_args()) as conn:
        df = conn.to_pandas_df(query)
        print(df.iloc[0].to_dict())


def chunk_compression_stats() -> None:
    query = "SELECT * FROM chunk_compression_stats('raw_pricing')"
    with db.connect(args.create_arg_parser().parse_args()) as conn:
        df = conn.to_pandas_df(query)
        print(df.iloc[0].to_dict())


MAT_VIEW_SIZE_QUERY = """
SELECT pg_size_pretty(sum(total_bytes)) as size,
aggs.view_name::TEXT
FROM "_timescaledb_internal".hypertable_chunk_local_size hcls,
timescaledb_information.continuous_aggregates aggs
WHERE hcls.hypertable_name = aggs.materialization_hypertable_name
GROUP BY aggs.view_name
"""


def materialized_view_sizes() -> None:
    with db.connect(args.create_arg_parser().parse_args()) as conn:
        print(conn.to_pandas_df(MAT_VIEW_SIZE_QUERY).set_index("view_name"))


OPEN_DATA_DAILY_QUERY = """
SELECT time_bucket(INTERVAL '1 day', "time") as bucket,
       (first(raw_pricing.*, "time")).*
  FROM raw_pricing
  where
    fund = %(fund)s
    AND time BETWEEN %(start)s
    AND %(end)s
  GROUP BY
  bucket,
  fund,
  expiry_date,
  expiry_time,
  expiry_cycle,
  contract_type,
  option_kind,
  strike
"""


def open_data_daily() -> None:
    parser = args.create_arg_parser()
    parser.add_argument("--fund", required=True, type=str)
    parser.add_argument("--start", required=True, type=date.fromisoformat)
    parser.add_argument("--end", required=True, type=date.fromisoformat)
    parser.add_argument(
        "--format", choices=["pandas", "polars"], default="pandas"
    )
    arguments = parser.parse_args()
    with db.connect(arguments) as conn:
        fn = (
            conn.to_polars_df
            if arguments.format == "polars"
            else conn.to_pandas_df
        )
        print(
            fn(
                OPEN_DATA_DAILY_QUERY,
                fund=arguments.fund,
                start=arguments.start,
                end=arguments.end,
            )
        )


DAY_DATA_QUERY = """
SELECT *
  FROM raw_pricing
  where
    fund = %(fund)s
    AND time >= %(day)s and time < %(day)s + interval '1 day'
  ORDER by time desc
"""


def day_data() -> None:
    parser = args.create_arg_parser()
    parser.add_argument("--fund", required=True, type=str)
    parser.add_argument("--day", required=True, type=date.fromisoformat)
    parser.add_argument(
        "--format", choices=["pandas", "polars"], default="pandas"
    )
    arguments = parser.parse_args()
    with db.connect(arguments) as conn:
        fn = (
            conn.to_polars_df
            if arguments.format == "polars"
            else conn.to_pandas_df
        )
        print(fn(DAY_DATA_QUERY, fund=arguments.fund, day=arguments.day))


SNAPSHOT_QUERY_T1 = """
SELECT rp.* FROM raw_pricing AS rp
WHERE fund = %(fund)s
and rp.time = ({subq})
"""


SNAPSHOT_QUERY_T2 = """
SELECT time from raw_pricing
WHERE fund = %(fund)s
AND time >= %(start)s
AND time < %(end)s
ORDER BY time
"""


SNAPSHOT_QUERIES = dict(
    desc=SNAPSHOT_QUERY_T1.format(subq=SNAPSHOT_QUERY_T2 + " desc limit 1"),
    asc=SNAPSHOT_QUERY_T1.format(subq=SNAPSHOT_QUERY_T2 + " asc limit 1"),
)


def snapshot() -> None:
    parser = args.create_arg_parser()
    parser.add_argument("--fund", required=True, type=str)
    parser.add_argument("--start", required=True, type=datetime.fromisoformat)
    parser.add_argument("--end", required=True, type=datetime.fromisoformat)
    parser.add_argument("--dir", choices=["desc", "asc"], default="desc")
    parser.add_argument(
        "--format", choices=["pandas", "polars"], default="pandas"
    )
    arguments = parser.parse_args()
    with db.connect(arguments) as conn:
        fn = (
            conn.to_polars_df
            if arguments.format == "polars"
            else conn.to_pandas_df
        )
        print(
            fn(
                SNAPSHOT_QUERIES[arguments.dir],
                fund=arguments.fund,
                start=arguments.start.astimezone(arguments.tz),
                end=arguments.end.astimezone(arguments.tz),
            )
        )


EXACT_SNAPSHOT_QUERY = """
SELECT *
  FROM raw_pricing
  where
    fund = %(fund)s
    AND time = %(ts)s
"""


def exact_snapshot() -> None:
    parser = args.create_arg_parser()
    parser.add_argument("--fund", required=True, type=str)
    parser.add_argument("--ts", required=True, type=datetime.fromisoformat)
    parser.add_argument(
        "--format", choices=["pandas", "polars"], default="pandas"
    )
    arguments = parser.parse_args()
    with db.connect(arguments) as conn:
        fn = (
            conn.to_polars_df
            if arguments.format == "polars"
            else conn.to_pandas_df
        )
        print(
            fn(
                EXACT_SNAPSHOT_QUERY,
                fund=arguments.fund,
                ts=arguments.ts.astimezone(arguments.tz),
            )
        )
