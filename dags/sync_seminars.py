from airflow.decorators import dag
from airflow.decorators import task
from common import postgres_retrieve
from common import upload_to_sheet
import pandas as pd
import pendulum


@dag(
    schedule_interval="2-59/5 8-19 * * *",
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
    with combinations as (
        select id, title, seminar_number 
        from step_of_faith.seminars
        cross join (values (1), (2)) temp (seminar_number)
    )
    select title, comb.seminar_number, count(user_id) total
    from step_of_faith.seminar_enrollement enr
    right join combinations comb 
        on enr.seminar_id = comb.id 
            and enr.seminar_number = comb.seminar_number
    group by title, comb.seminar_number
    order by enr.seminar_id
    """

    upload(postgres_retrieve(q))


sync_seminars()
