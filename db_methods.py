from datetime import datetime
from calendar import monthrange
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import sql, OperationalError

db_password = '123'
db_name = 'main'


def create_database(name, password):
    connect = psycopg2.connect(dbname='postgres',
                               user='postgres',
                               host='localhost',
                               password=password)

    connect.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connect.cursor()
    try:
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        print('successfully created database')
    except:
        print('Database already exists')


def connect_database(name, password):
    try:
        con = psycopg2.connect(
            database=name,
            user="postgres",
            password=password,
            host="localhost",
            port="5432"
        )
        con.autocommit = True
        cursor = con.cursor()
        return cursor
    except:
        print("Can not connect to database. Maybe database does not exists")


def create_table(cursor, table):
    try:
        cursor.execute(table)
        print("Table executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def create_workers_table(cursor):
    table_name = 'workers'
    workers_table = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        worker text
    )
    """
    create_table(cursor, workers_table)
    cursor.execute(f"SELECT * FROM {table_name}")
    if not cursor.fetchone():
        for i in range(1, 11):
            worker = "worker" + str(i)
            cursor.execute(f"""INSERT INTO "{table_name}" ("worker") VALUES ('{worker}');""")


def get_name_by_num(num):
    months_dict = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug',
                   9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
    now_year = datetime.now().year
    return months_dict[num] + str(now_year)


def create_month_tables(cursor):
    for month in range(1, 13):
        table_name = get_name_by_num(month)

        month_table = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            day integer,
            main_worker text,
            extra_worker text
        )
        """
        create_table(cursor, month_table)
        cursor.execute(f"SELECT day FROM {table_name}")
        if not cursor.fetchone():
            count_days = monthrange(datetime.now().year, month)[1]
            for day in range(1, count_days + 1):
                date = datetime(datetime.now().year, month, day)
                weekday = date.weekday()
                if weekday > 4:
                    cursor.execute(f"""INSERT INTO {table_name} (day, extra_worker) VALUES ('{day}', '-');""")
                else:
                    cursor.execute(f"""INSERT INTO {table_name} (day) VALUES ('{day}');""")


def count_day_offs(cursor, table_name):
    cursor.execute(f"SELECT extra_worker FROM {table_name};")
    extra_worker = cursor.fetchall()
    day_off_count = 0
    for i in extra_worker:
        if i[0] == '-':
            day_off_count += 1
    return day_off_count


def distribute_main_workers(cursor, table_name, workers, extra_workers, shifts_count, days_count):
    mid_work_shifts = shifts_count // len(workers)
    day = 1
    cycle = 0
    worker_num = 0
    while day < shifts_count + 2:
        if cycle == mid_work_shifts + 1:
            cycle = 0
            worker_num += 1
        if day < days_count + 1:
            cursor.execute(
                f"""UPDATE {table_name} SET main_worker = '{workers[worker_num][0]}' where day = {day}""")
            day += 1
            cycle += 1
        else:
            shift_val = extra_workers[day - days_count - 1][0]
            if shift_val == '-':
                day += 1
            else:
                cursor.execute(
                    f"""UPDATE {table_name} SET extra_worker = '{workers[worker_num][0]}' where day = '{day - days_count}'""")
                day += 1
                cycle += 1
    print("Main shifts successfully located")


def distribute_extra_workers(cursor, table_name, workers):
    cursor.execute(f"SELECT * FROM {table_name};")
    table = cursor.fetchall()
    worker_num = 0
    for i in range(0, len(table)):
        if table[i][3] is None:
            if table[i][2] != workers[worker_num][0]:
                cursor.execute(
                    f"""UPDATE {table_name} SET extra_worker = '{workers[worker_num][0]}' where day = '{table[i][1]}'""")
                worker_num += 1
            else:
                worker_num += 1
                cursor.execute(
                    f"""UPDATE {table_name} SET extra_worker = '{workers[worker_num][0]}' where day = '{table[i][1]}'""")
    print("Extra shifts successfully located")


def distribution_workers(cursor):
    for month in range(1, 13):
        table_name = get_name_by_num(month)

        cursor.execute(f"SELECT day FROM {table_name} ORDER BY id DESC LIMIT 1 OFFSET 0;")
        days_count = cursor.fetchall()[0][0]

        cursor.execute(f"SELECT extra_worker FROM {table_name};")
        extra_workers = cursor.fetchall()
        day_off_count = count_day_offs(cursor, table_name)
        cursor.execute("SELECT worker FROM workers;")
        workers = cursor.fetchall()
        shifts_count = days_count * 2 - day_off_count

        distribute_main_workers(cursor, table_name, workers, extra_workers, shifts_count, days_count)

        distribute_extra_workers(cursor, table_name, workers)


# create_database(db_name, db_password)
# cur = connect_database(db_name, db_password)
#
# create_workers(cur)
# create_month_tables(cur)
def init_database():
    cursor = connect_database(db_name, db_password)
    create_database(db_name, db_password)
    create_workers_table(cursor)
    create_month_tables(cursor)
