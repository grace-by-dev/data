import pendulum
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable


@dag(
    schedule_interval=timedelta(minutes=5),
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["step-of-faith"],
)
def sync_seminars():
    @task
    def extract() -> list:
        q = """select 
            coalesce(seminar, '<none>'), 
            count(user_id) as c 
        from step_of_faith.users 
        group by seminar 
        order by seminar;"""

        with PostgresHook("postgres").get_cursor() as cur:
            data = cur.execute(q)
            data = cur.fetchall()

        return data
    
    @task
    def load(data: list):
        import gspread
        import tempfile


        with tempfile.NamedTemporaryFile("w") as f:
            f.write(Variable.get("google-service-json"))
            f.flush()
            client = gspread.service_account(f.name)
        book = client.open_by_key(Variable.get("google-token"))
        sheet = book.get_worksheet_by_id(1712708177)
        sheet.clear()
        sheet.update([("Название","Записавшиеся")] + data)
   
   
    load(extract())


sync_seminars()
