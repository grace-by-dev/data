import pendulum
from datetime import timedelta

from airflow.decorators import dag, task
import pandas as pd

from common import postgres_retrieve, upload_to_sheet

@dag(
    schedule_interval=timedelta(minutes=5),
    start_date=pendulum.datetime(2024, 1, 1, 0, 1, tz="UTC"),
    catchup=False,
    tags=["step-of-faith"],
)
def sync_counselings():
    @task
    def load(data: list):
        data = pd.DataFrame(data, columns=["counselor", "time", "occupied"])
        data = data.pivot(values="occupied", index="counselor", columns="time")
        data.index = data.index.rename(None)
        data.columns = data.columns.rename(None)
        columns = [None] + data.columns.tolist()
        data = data.to_records().tolist()
        data = [columns] + data

        upload_to_sheet(
            data=data, 
            sheet_id=406103439
        )

    q = """select counselor_id, time::varchar, user_id is not null as occupied
        from step_of_faith.schedule_counselor_appointment
        order by counselor_id, time;
    """
   
    load(postgres_retrieve(q))


sync_counselings()
