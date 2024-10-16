import pendulum
from datetime import timedelta
from collections import defaultdict

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
    def extract() -> dict:
        q = """select counselor_id, time::varchar, user_id is not null as occupied
            from step_of_faith.schedule_counselor_appointment
            order by counselor_id, time;
        """

        with PostgresHook("postgres").get_cursor() as cur:
            data = cur.execute(q)
            data = cur.fetchall()

        out = defaultdict(list)
        for row in data:
            out[row[0]].append(row[1:])

        return out
    
    @task
    def load(data: dict):
        import gspread
        import tempfile


        with tempfile.NamedTemporaryFile("w") as f:
            f.write(Variable.get("google-service-json"))
            f.flush()
            client = gspread.service_account(f.name)
        book = client.open_by_key(Variable.get("google-token"))
        for counselor_id, schedule in data.items():
            sheet = book.get_worksheet_by_id(counselor_id)
            sheet.clear()
            sheet.update([("Время","Занято")] + schedule)
   
   
    load(extract())


sync_counselings()
