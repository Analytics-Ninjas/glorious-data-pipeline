from sqlalchemy import create_engine, text
from datetime import datetime
from io import StringIO
import os
import boto3
import pandas as pd


# Connect MySQL db
db_user = "admin"
db_password = "password"
db_host = "my-db-instance.cxjkxlbvg4uh.ap-northeast-1.rds.amazonaws.com"
db_name = "stock_db"

db_url = os.environ['DB_URL']

engine = create_engine(db_url)

conn = engine.connect()

# Detect updated records and upload into s3
etl = '''
            SELECT 
                new.*
            FROM stock AS new
            LEFT ANTI JOIN stock_staging AS old
                ON new.stock_id = old.stock_id AND new.company = old.company AND new.category = old.category AND new.price = old.price
      '''
updated_records = conn.execute(text(etl)).fetchall()

# Get column name
col = conn.execute(text("SHOW COLUMNS FROM stock")).fetchall()
col_list = [x[0] for x in col]

# Convert to dataframe
df = pd.DataFrame(updated_records, columns=col_list)

# Get current date for file path
today = str(datetime.now())[0:10].replace('-','')

# Save CSV to memory but not solid state disk
csv_buffer = StringIO()
df.to_csv(csv_buffer, index = False)
file_dir =  '/engines_' + today + '.csv'

# Upload to s3 bucket
s3 = boto3.client('s3',aws_access_key_id='AKIAV52JQWWTEJDQH3JM', aws_secret_access_key='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')
bucket_name = 'analytics-ninjas-85ncy6'
s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=file_dir.lstrip('/'))

# Closing allocate memory
csv_buffer.close()


# Truncate stock_staging table and insert all current data
sql = '''
            TRUNCATE TABLE stock_staging;
            SELECT *
            INTO stock_staging
            FROM stock;
        '''
conn.execute(text(sql))
conn.commit()
conn.close()