from airflow.decorators import dag
from airflow.decorators import task
from common import postgres_retrieve
from common import upload_to_sheet
import pandas as pd
import pendulum


@dag(
    schedule_interval="0-59/5 8-19 * * *",
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    tags=["step-of-faith"],
)
def sync_counselings() -> None:
    @task
    def load(data: list) -> None:
        data = pd.DataFrame(data, columns=["counselor", "time", "occupied"])
        data = data.pivot(values="occupied", index="counselor", columns="time")  # noqa: PD010
        data.index = data.index.rename(None)
        data.columns = data.columns.rename(None)
        columns = ["✝️", *data.columns.tolist()]
        data = data.to_records().tolist()
        data = [columns, *data]

        upload_to_sheet(data=data, sheet_id=406103439)

    q = """select counselor_id, time::varchar, user_id is not null as occupied
        from step_of_faith.schedule_counselor_appointment
        order by counselor_id, time;
    """

    load(postgres_retrieve(q))


sync_counselings()
