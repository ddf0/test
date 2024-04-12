import random
from faker import Faker
import psycopg2
from datetime import datetime, timedelta

# Настройка Faker
fake = Faker()

# Подключение к базе данных
conn = psycopg2.connect(
    dbname="your_dbname", 
    user="your_username", 
    password="your_password", 
    host="your_host", 
    port="your_port"
)
cur = conn.cursor()

# Генерация данных для таблицы adsb_type_1_4
for _ in range(100):  # Генерация 100 записей
    rec_time = datetime.now() - timedelta(days=random.randint(0, 365))
    icao = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=6))
    category = random.randint(1, 10)
    indet = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=8))
    cur.execute("INSERT INTO adsb_type_1_4 (rec_time, icao, category, indet) VALUES (%s, %s, %s, %s)",
                (rec_time, icao, category, indet))
# Генерация данных для таблицы adsb_type_5_8
for _ in range(100):
    rec_time = fake.date_time_between(start_date='-1y', end_date='now')
    icao = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=6))
    movement = random.uniform(0, 400)
    ground_track = random.uniform(0, 360)
    time_ = rec_time.date()
    latitude = random.uniform(-90, 90)
    longitude = random.uniform(-180, 180)
    cur.execute("INSERT INTO adsb_type_5_8 (rec_time, icao, movement, ground_track, time_, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (rec_time, icao, movement, ground_track, time_, latitude, longitude))

# Генерация данных для таблицы adsb_type_9_18_20_22
for _ in range(100):
    rec_time = fake.date_time_between(start_date='-1y', end_date='now')
    icao = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=6))
    surveillance_status = random.randint(0, 3)
    single_antenna = random.choice([True, False])
    altitude = random.randint(100, 12000)
    time_ = rec_time.date()
    latitude = random.uniform(-90, 90)
    longitude = random.uniform(-180, 180)
    cur.execute("INSERT INTO adsb_type_9_18_20_22 (rec_time, icao, surveillance_status, single_antenna, altitude, time_, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (rec_time, icao, surveillance_status, single_antenna, altitude, time_, latitude, longitude))

# Генерация данных для таблицы adsb_type_19
for _ in range(100):
    rec_time = fake.date_time_between(start_date='-1y', end_date='now')
    icao = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=6))
    vertical_speed = random.randint(-5000, 5000)
    horizontal_speed = random.randint(0, 1000)
    angle = random.uniform(0, 360)
    ver_speed_type = fake.word(ext_word_list=['GS', 'TAS'])
    gnss_bar_dif = random.randint(-100, 100)
    cur.execute("INSERT INTO adsb_type_19 (rec_time, icao, vertical_speed, horizontal_speed, angle, ver_speed_type, gnss_bar_dif) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (rec_time, icao, vertical_speed, horizontal_speed, angle, ver_speed_type, gnss_bar_dif))

# Заполнение других таблиц можно реализовать по аналогии
# Не забудьте закрыть подключение
conn.commit()
cur.close()
conn.close()
