import json
from pathlib import Path
from dataclasses import dataclass
from enum import Enum
import enum
import datetime
import psycopg
from psycopg import sql
import os
from watchdog.observers import Observer
import watchdog.events
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


class Watcher(watchdog.events.FileSystemEventHandler):
    """Logs all the events captured."""

    def __init__(self, conn):
        super().__init__()
        self.conn = conn

    def on_created(self, event):
        super().on_created(event)
        if event.is_directory:
            return
        logging.info("Created file: %s", event.src_path)
        act = parse_file(Path(event.src_path))
        with self.conn.cursor() as cur:
            cur.execute(
                insert_query(),
                (
                    act.time.isoformat(),
                    act.activeSeconds,
                    act.activityType.name,
                    act.averageHR,
                    act.maxHR,
                ),
            )
        self.conn.commit()


def watch():

    dbhost = os.getenv("DBHOST", "localhost")
    dbpass = os.getenv("DBPASS")
    dbuser = os.getenv("DBUSER")
    dbname = os.getenv("DBNAME", "postgres")

    connection_string = f"host={dbhost} password={dbpass} user={dbuser} dbname={dbname}"

    watchpath = Path(os.getenv("WATCHPATH"))

    dump_all(path=watchpath, dbhost=dbhost, dbpass=dbpass, dbuser=dbuser, dbname=dbname)

    with psycopg.connect(connection_string) as conn:

        observer = Observer()
        observer.schedule(Watcher(conn), watchpath, recursive=True)
        observer.start()
        try:
            logging.info("Listening")
            while observer.is_alive():

                observer.join(1)
        finally:
            observer.stop()
            observer.join()


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
    watch()


if __name__ == "__main__":
    main()
