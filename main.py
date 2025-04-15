import json
from pathlib import Path
from dataclasses import dataclass
from enum import Enum
import enum
import datetime
import psycopg
from psycopg import sql
import os
import logging


class ActivityType(Enum):
    run = enum.auto()
    weight = enum.auto()
    core = enum.auto()


@dataclass
class HeartrateData:
    activityType: ActivityType
    time: datetime.datetime
    activeSeconds: int
    averageHR: int
    maxHR: int


def parse_file(f: Path) -> HeartrateData:
    activityTable = {
        "run": ActivityType.run,
        "core": ActivityType.core,
        "weight": ActivityType.weight,
    }
    with open(f, "r", encoding="utf-8") as json_file:
        run_data = json.load(json_file)
        t = activityTable[f.parent.name]
        dt = datetime.datetime.strptime(
            f.with_suffix("").name.replace("_", ":"), "%Y-%m-%dT%H:%M:%S%z"
        )
        return HeartrateData(
            activityType=t,
            time=dt,
            activeSeconds=run_data["activeSeconds"]["value"],
            averageHR=run_data["averageHR"]["value"],
            maxHR=run_data["maxHR"]["value"],
        )


def parse_activities(p: Path) -> list[HeartrateData]:
    result: list[HeartrateData] = []
    for d in p.iterdir():
        if not d.is_dir():
            continue
        for f in d.iterdir():
            result.append(parse_file(f))
    return result


def insert_query(table_name: str = "activity_log"):
    return sql.SQL(
        (
            "INSERT INTO {table_name} "
            "(activityTimestamp, activitySeconds, activityType, heartrateAverage, heartrateMax)"
            "VALUES (%s, %s, %s, %s, %s)"
        )
    ).format(
        table_name=sql.Identifier(table_name),
    )


def apply_to_db(data: list[HeartrateData], db_connection_string: str):
    table_name = "activity_log"
    with psycopg.connect(db_connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("TRUNCATE TABLE {table_name}").format(
                    table_name=sql.Identifier(table_name),
                )
            )

            cur.executemany(
                insert_query(),
                (
                    (
                        d.time.isoformat(),
                        d.activeSeconds,
                        d.activityType.name,
                        d.averageHR,
                        d.maxHR,
                    )
                    for d in data
                ),
            )


def dump_all(path: Path, dbhost: str, dbpass: str, dbuser: str, dbname: str):
    act = parse_activities(path)
    connection_string = f"host={dbhost} password={dbpass} user={dbuser} dbname={dbname}"
    logging.info("Connection string: %s", connection_string)
    logging.info("#activities: %s", len(act))
    apply_to_db(act, connection_string)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    dbhost = os.getenv("DBHOST", "localhost")
    dbpass = os.environ["DBPASS"]
    dbuser = os.environ["DBUSER"]
    dbname = os.getenv("DBNAME", "postgres")
    watchpath = Path(os.environ["WATCHPATH"])

    dump_all(watchpath, dbhost, dbpass, dbuser, dbname)


if __name__ == "__main__":
    main()
