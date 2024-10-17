import pendulum
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable


@dag(
    schedule_interval=timedelta(minutes=5),
    start_date=pendulum.datetime(2024, 1, 1, 0, 1, tz="UTC"),
    catchup=False,
    tags=["step-of-faith"],
)
def sync_counselings():
    @task
    def extract() -> list:
        q = """select counselor_id, time::varchar, user_id is not null as occupied
            from step_of_faith.schedule_counselor_appointment
            order by counselor_id, time;
        """

        with PostgresHook("postgres").get_cursor() as cur:
            data = cur.execute(q)
            data = cur.fetchall()

        return data
    
    @task
    def load(data: list):
        import gspread
        import tempfile
        import pandas as pd


        data = pd.DataFrame(data, columns=["counselor", "time", "occupied"])
        data = data.pivot(values="occupied", index="counselor", columns="time")
        data.index = data.index.rename(None)
        data.columns = data.columns.rename(None)
        columns = [None] + data.columns.tolist()
        data = data.to_records().tolist()
        data = [columns] + data




        with tempfile.NamedTemporaryFile("w") as f:
            f.write(Variable.get("google-service-json"))
            f.flush()
            client = gspread.service_account(f.name)
        book = client.open_by_key(Variable.get("google-token"))
        sheet = book.get_worksheet_by_id(406103439)
        sheet.update(data)
   
   
    load(extract())


sync_counselings()
