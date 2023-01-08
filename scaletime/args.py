import argparse

from zoneinfo import ZoneInfo


def create_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tz", type=ZoneInfo, default=ZoneInfo("America/Chicago")
    )
    parser.add_argument("--user", type=str, required=True)
    parser.add_argument("--passwd", type=str, required=True)
    parser.add_argument("--host", type=str, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--dbname", type=str, required=True)

    return parser
