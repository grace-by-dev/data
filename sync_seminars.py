import pendulum
from datetime import timedelta

from airflow.decorators import dag, task
from common import postgres_retrieve, upload_to_sheet


@dag(
    schedule_interval="2-59/5 8-19 * * *",
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    tags=["step-of-faith"],
)
def sync_seminars():
    @task
    def upload(data: list) -> None:
        upload_to_sheet(
            data=[("Название","Записавшиеся")] + data, 
            sheet_id=1712708177,
            clear=True
        )


    q = """select 
            coalesce(seminar, '<none>'), 
            count(user_id) as c 
        from step_of_faith.users 
        group by seminar 
        order by seminar;"""
    
    upload(postgres_retrieve(q))


sync_seminars()
