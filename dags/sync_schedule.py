from airflow.decorators import dag
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from common import get_sheet
import pandas as pd
from psycopg2.extras import execute_batch


@dag(
    schedule_interval=None,
    tags=["step-of-faith"],
)
def sync_schedule() -> None:
    @task
    def load(data: list) -> None:
        conn = PostgresHook("postgres").get_conn()
        with conn.cursor() as cur:
            cur.execute("truncate step_of_faith.schedule;")
            execute_batch(
                cur,
                """insert into step_of_faith.schedule (day, time, event) values (%s, %s, %s);""",
                data,
            )
            conn.commit()

    @task
    def extract() -> pd.DataFrame:
        sheet = get_sheet(745768165)
        records = sheet.get_all_records()
        data = [(r["День"], r["Время"], r["Ивент"]) for r in records]

        return data

    load(extract())


sync_schedule()
