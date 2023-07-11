import boto3
import pandas as pd
from sqlalchemy import create_engine, text
import datetime
from io import StringIO
import os

db_url = os.environ["DB_URL"]
engine = create_engine(db_url)


def get_latest_update_prod(conn):
    query = """
    SELECT *
    FROM User
    WHERE DATE(updated_at) = CURRENT_DATE() - 1
    AND status = 'U'
    """
    result = conn.execute(text(query)).fetchall()
    return result


def get_latest_insert_prod(conn):
    query = """
    SELECT *
    FROM User
    WHERE DATE(updated_at) = CURRENT_DATE() - 1
    AND status = 'I'
    """
    result = conn.execute(text(query)).fetchall()
    return result


def get_latest_snapshot_datalake():
    snap_dir = get_path_snapshot(1)
    s3_client = boto3.client("s3")
    s3_bucket = "analytics-ninjas-85ncy6"
    s3_object_key = snap_dir.lstrip("/")
    s3_response = s3_client.get_object(Bucket=s3_bucket, Key=s3_object_key)
    df = pd.read_csv(s3_response["Body"])
    return df


def get_path_snapshot(days=2):
    today = str(datetime.datetime.now() - datetime.timedelta(days=days))[0:10].replace(
        "-", ""
    )
    return f"/ETL/stock_db/user/partition={today}/user.csv"


def update_snapshot(conn):
    updated_users = get_latest_update_prod(conn)
    updated_users = pd.DataFrame(updated_users)
    updated_users_id = updated_users["user_id"].values
    current_users = get_latest_snapshot_datalake()
    current_users = current_users.loc[~current_users["user_id"].isin(updated_users_id)]
    return pd.concat([current_users, updated_users]).sort_values(by="user_id")


def insert_snapshot(conn):
    inserted_users = get_latest_insert_prod(conn)
    inserted_users = pd.DataFrame(inserted_users)
    current_users = update_snapshot(conn)
    return pd.concat([current_users, inserted_users]).sort_values(by="user_id")


def get_full_prod_data(conn):
    query = """
    SELECT *
    FROM User
    """
    result = conn.execute(query).fetchall()
    return pd.DataFrame(result)


def upload_to_datalake(df):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    file_dir = get_path_snapshot()
    s3 = boto3.client("s3")
    s3_bucket = "analytics-ninjas-85ncy6"
    s3.put_object(
        Body=csv_buffer.getvalue(), Bucket=s3_bucket, Key=file_dir.lstrip("/")
    )
    csv_buffer.close()


def incremental_load():
    with engine.connect() as conn:
        upload_to_datalake(insert_snapshot(conn))


def full_load():
    with engine.connect() as conn:
        upload_to_datalake(get_full_prod_data(conn))
