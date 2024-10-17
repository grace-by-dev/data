from airflow.decorators import task
import tempfile
import gspread
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import numpy as np
import time
import string


cols = string.ascii_uppercase


def backoff(mult):
    timeout = np.random.noncentral_chisquare(10, 3) * mult
    logging.info(f"Sleeping for {timeout:.2f} seconds")
    time.sleep(timeout)

def get_sheet(sheet_id: int) -> gspread.worksheet.Worksheet:
    backoff(4)
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
    strategy = np.random.choice(["full","quarters","rows"])
    logging.info(f"Got \"{strategy}\" strategy")
    if strategy == "full":
        backoff(0.2)
        sheet.update(data)
    elif strategy == "quarters":
        leny, lenx = len(data), len(data[0])
        midy, resy = divmod(leny, 2)
        midx = lenx // 2
        ranges = [
            [f"{cols[0]}{1}:{cols[midx-1]}{midy}", f"{cols[midx]}{1}:{cols[lenx-1]}{midy}"], 
            [f"{cols[0]}{midy+1}:{cols[midx-1]}{leny}", f"{cols[midx]}{midy+1}:{cols[lenx-1]}{leny}"]
        ]
        quarters = [[[], []], [[], []]]
        for i in range(midy):
            quarters[0][0].append(data[i][:midx])
            quarters[0][1].append(data[i][midx:])
            quarters[1][0].append(data[midy+i][:midx])
            quarters[1][1].append(data[midy+i][midx:])

        if resy:
            quarters[1][0].append(data[-1][:midx])
            quarters[1][1].append(data[-1][midx:])

        for qrow, rrow in zip(quarters, ranges):
            for q, r in zip(qrow, rrow):
                backoff(0.2)
                sheet.update(q, r)
    else:
        if not clear:
            sheet.clear()
        for row in data:
            backoff(0.1)
            sheet.append_row(row)


@task
def postgres_retrieve(query: str) -> list:
    with PostgresHook("postgres").get_cursor() as cur:
        data = cur.execute(query)
        data = cur.fetchall()

    return data

