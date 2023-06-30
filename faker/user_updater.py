from sqlalchemy import create_engine, text
from faker import Faker
import random

db_user = "admin"
db_password = "password"
db_host = "my-db-instance.cxjkxlbvg4uh.ap-northeast-1.rds.amazonaws.com"
db_name = "stock_db"


db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:3306/{db_name}"

engine = create_engine(db_url)

# Get the number of users that update email
def update_rate():
    result = conn.execute(text('SELECT * FROM stock_db.User')).fetchall()
    update_rate = random.uniform(0,0.02)
    update_count = round(update_rate * len(result))
    # Get update user data
    update_user = conn.execute(text(f'SELECT * FROM stock_db.User ORDER BY RAND() LIMIT {update_count}')).fetchall()
    return update_user


# Update these users info
def update_current_user():
    fake = Faker()
    update_user = update_rate()
    for usr in update_user:
        user_id = usr[0]
        new_email = fake.email()
        update_query = f"UPDATE stock_db.User SET email = '{new_email}' WHERE user_id = {user_id}"
        conn.execute(text(update_query))

# Create new users
def create_new_user(new_user_count):
    fake = Faker()
    new_user_count = new_user_count
    new_user_list = []
    for new_usr in range(new_user_count):
        user_name = fake.name()
        user_email = fake.email()
        new_user_list.append(str((user_name, user_email)))
    # Create query string to insert new users
    insert_val = ','.join(new_user_list)
    insert_query = f'''
    INSERT INTO stock_db.User (name, email) 
    VALUES {insert_val}
    '''
    conn.execute(text(insert_query))

def updater(new_user_count):
    update_current_user()
    create_new_user(new_user_count)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    with engine.connect() as conn:
        updater(10)