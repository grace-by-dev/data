from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable


@dag(
    schedule_interval=None,
    tags=["step-of-faith"],
)
def sync_schedule():
    @task
    def load(data: list):
        from psycopg2.extras import execute_batch


        with PostgresHook("postgres").get_cursor() as cur:
            execute_batch(
                cur,
                 "insert into step_of_faith.schedule (day, time, event) values (%s, %s, %s);",
                 data
            )
    
    @task
    def extract():
        import gspread
        import tempfile

        with tempfile.NamedTemporaryFile("w") as f:
            f.write(Variable.get("google-service-json"))
            f.flush()
            client = gspread.service_account(f.name)
        book = client.open_by_key(Variable.get("google-token"))
        sheet = book.get_worksheet_by_id(745768165)
        records = sheet.get_all_records()
        data = [(r["День"], r["Время"], r["Ивент"]) for r in records]

        return data
   
    load(extract())


sync_schedule()
