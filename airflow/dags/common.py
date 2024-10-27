import logging
import string
import tempfile
import time

import gspread
import numpy as np

from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


cols = string.ascii_uppercase


def backoff(mult: int) -> None:
    timeout = np.random.noncentral_chisquare(10, 3) * mult
    logging.info(f"Sleeping for {timeout:.2f} seconds")
    time.sleep(timeout)


def get_sheet(sheet_id: int) -> gspread.worksheet.Worksheet:
    backoff(3)
    with tempfile.NamedTemporaryFile("w") as f:
        f.write(Variable.get("google-service-json"))
        f.flush()
        client = gspread.service_account(f.name)
    book = client.open_by_key(Variable.get("google-token"))

    return book.get_worksheet_by_id(sheet_id)


def upload_to_sheet(data: list, sheet_id: int, clear: bool = False) -> None:
    sheet = get_sheet(sheet_id)
    if clear:
        sheet.clear()
    strategy = np.random.choice(["full", "quarters", "rows"])
    logging.info(f'Got "{strategy}" strategy')
    if strategy == "full":
        backoff(0.4)
        sheet.update(data)
    elif strategy == "quarters":
        leny, lenx = len(data), len(data[0])
        midy = leny // 2
        midx = lenx // 2
        data = np.array(data)
        quarters = [data[:midy, :midx], data[:midy, midx:], data[midy:, :midx], data[midy:, midx:]]
        ranges = [
            f"{cols[0]}{1}:{cols[midx-1]}{midy}",
            f"{cols[midx]}{1}:{cols[lenx-1]}{midy}",
            f"{cols[0]}{midy+1}:{cols[midx-1]}{leny}",
            f"{cols[midx]}{midy+1}:{cols[lenx-1]}{leny}",
        ]

        for q, r in zip(quarters, ranges):
            backoff(0.3)
            sheet.update(q.tolist(), r)
    else:
        if not clear:
            sheet.clear()
        for row in data:
            backoff(0.2)
            sheet.append_row(row)


@task
def postgres_retrieve(query: str) -> list:
    with PostgresHook("postgres").get_cursor() as cur:
        data = cur.execute(query)
        data = cur.fetchall()

    return data
