from common import postgres_retrieve
from common import upload_to_sheet
import pandas as pd
import pendulum

from airflow.decorators import dag
from airflow.decorators import task


@dag(
    schedule_interval="2-59/5 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Minsk"),
    catchup=False,
    tags=["step-of-faith"],
)
def sync_seminars() -> None:
    @task
    def upload(data: list) -> None:
        data = pd.DataFrame(data, columns=["seminar", "seminar_number", "total"])
        data = data.pivot(values="total", index="seminar_number", columns="seminar")  # noqa: PD010
        data.index = data.index.rename(None)
        data.columns = data.columns.rename(None)
        columns = ["✝️", *data.columns.tolist()]
        data = data.to_records().tolist()
        data = [columns, *data]

        upload_to_sheet(data=data, sheet_id=1712708177, clear=True)

    q = """
    select s.title, sn.starts_at::varchar, count(se.user_id) as total
    from step_of_faith.seminars s
    cross join step_of_faith.seminar_numbers sn
    left join step_of_faith.seminar_enrollement se
        on s.id = se.seminar_id 
            and sn.seminar_number = se.seminar_number
    group by s.id, s.title, sn.starts_at
    order by s.id;
    """

    upload(postgres_retrieve(q))


sync_seminars()
