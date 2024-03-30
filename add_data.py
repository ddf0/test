import pathlib
import psycopg2
from pyais.stream import FileReaderStream
from datetime import datetime
import calendar
import pyModeS as pms

curr_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def is_number(stroka: str):
    try:
        float(stroka)
        return True
    except ValueError:
        return False
    
def insert_ais_type_1_2_3(conn, data):
    new_data = {}
    new_data['rec_time'] = curr_date
    if not str(data['mmsi']).isnumeric():
        data['mmsi'] = 0
    new_data['mmsi'] = data['mmsi']

    if not str(data['status']).isnumeric():
        data['status'] = 0
    new_data['navstatus'] = data['status'] # https://datalastic.com/blog/ais-navigational-status/

    if not is_number(str(data['turn'])):
        data['turn'] = 0
    new_data['rot'] = data['turn']

    if not is_number(data['speed']):
        data['speed'] = 0
    new_data['sog'] = data['speed']

    new_data['posAccuracy'] = data['accuracy']

    if not is_number(str(data['lon'])): 
        data['lon'] = 0
    new_data['posLon'] = data['lon']

    if not is_number(str(data['lat'])):
        data['lat'] = 0
    new_data['posLat'] = data['lat']

    if not is_number(str(data['course'])):
        data['course'] = 0
    new_data['cog'] = data['course']

    if not is_number(str(data['heading'])):
        data['heading'] = 0
    new_data['heading'] = data['heading']
    query = """
    INSERT INTO ais_type_1_2_3
    (rec_time, mmsi, navstatus, rot, sog, posAccuracy, posLon, posLat, cog, heading)
    VALUES (%(rec_time)s, %(mmsi)s, %(navstatus)s, %(rot)s, %(sog)s, %(posAccuracy)s, %(posLon)s, %(posLat)s, %(cog)s, %(heading)s);
    """
    conn.cursor().execute(query, new_data)
    conn.commit()
# full tested
def insert_ais_type_4_11(conn, data):
    new_data = {}
    new_data['rec_time'] = curr_date
    if not str(data['mmsi']).isnumeric():
        data['mmsi'] = 0
    new_data['mmsi'] = data['mmsi']
    if not data['year'] or data['year'] > datetime.now().year or data['year'] < 1970:
        data['year'] = 1970
    if not data['month'] or data['month'] > 12 or data['month'] < 1:
        data['month'] = 1
    if not data['day'] or data['day'] > int(calendar.monthrange(1970, data['month'])[1]) or data['day'] < 1:
        data['day'] = 1
    if not data['hour'] or data['hour'] > 23 or data['hour'] < 0:
        data['hour'] = 00
    if not data['minute'] or data['minute'] > 59 or data['minute'] < 0:
        data['minute'] = 00
    if not data['second'] or data['second'] > 59 or data['second'] < 0:
        data['second'] = 00
    date = datetime(data['year'], data['month'], data['day'], data['hour'], data['minute'], data['second'])
    new_data['date'] = date.strftime("%Y-%m-%d %H:%M:%S")
    new_data['posAccuracy'] = data['accuracy']
    if not is_number(str(data['lon'])): 
        data['lon'] = 0
    new_data['posLon'] = data['lon']

    if not is_number(str(data['lat'])):
        data['lat'] = 0
    new_data['posLat'] = data['lat']
    query = """
    INSERT INTO ais_type_4_11
    (rec_time, mmsi, date, posAccuracy, posLon, posLat)
    VALUES (%(rec_time)s, %(mmsi)s, %(date)s, %(posAccuracy)s, %(posLon)s, %(posLat)s);
    """
    cur = conn.cursor()
    cur.execute(query, new_data)
    cur.close()
    conn.commit()
# full tested
def insert_ais_type_5(conn, data):
    new_data = {}
    new_data['rec_time'] = curr_date
    if not str(data['mmsi']).isnumeric():
        data['mmsi'] = 0
    new_data['mmsi'] = data['mmsi']
    new_data['imo'] = data['imo']
    new_data['callsign'] = data['callsign']
    new_data['name_'] = data['shipname']
    new_data['type_'] = data['ship_type']
    new_data['toBow'] = data['to_bow']
    new_data['toStern'] = data['to_stern']
    new_data['toPort'] = data['to_port']
    new_data['toStarboard'] = data['to_starboard']
    new_data['fixType'] = data['epfd']
    if not data['month'] or data['month'] > 12 or data['month'] < 1:
        data['month'] = 1
    if not data['day'] or data['day'] > int(calendar.monthrange(1970, data['month'])[1]) or data['day'] < 1:
        data['day'] = 1
    if not data['hour'] or data['hour'] > 23 or data['hour'] < 0:
        data['hour'] = 00
    if not data['minute'] or data['minute'] > 59 or data['minute'] < 0:
        data['minute'] = 00
    date = datetime(1970, data['month'], data['day'], data['hour'], data['minute'], 0)
    new_data['etaTime'] = date.strftime("%Y-%m-%d %H:%M:%S")
    new_data['draught'] = data['draught']
    new_data['destination'] = data['destination']
    query = """
    INSERT INTO ais_type_5
    (rec_time, mmsi, imo, callsign, name_, type_, toBow, toStern, toPort, toStarboard, fixType, etaTime, draught, destination)
    VALUES (%(rec_time)s, %(mmsi)s, %(imo)s, %(callsign)s, %(name_)s, %(type_)s, %(toBow)s, %(toStern)s, %(toPort)s, %(toStarboard)s, %(fixType)s, %(etaTime)s, %(draught)s, %(destination)s);
    """
    cur = conn.cursor()
    cur.execute(query, new_data)
    cur.close()
    conn.commit()
# full tested
def insert_ais_type_9(conn, data):
    new_data = {}
    new_data['rec_time'] = curr_date
    if not str(data['mmsi']).isnumeric():
        data['mmsi'] = 0
    new_data['mmsi'] = data['mmsi']
    if not is_number(str(data['alt'])):
        data['alt'] = 0
    new_data['altitude'] = data['alt']
    if not is_number(str(data['speed'])):
        data['speed'] = 0
    new_data['sog'] = data['speed']
    new_data['posAccuracy'] = data['accuracy']
    if not is_number(str(data['lon'])): 
        data['lon'] = 0
    new_data['posLon'] = data['lon']

    if not is_number(str(data['lat'])):
        data['lat'] = 0
    new_data['posLat'] = data['lat']
    if not is_number(str(data['course'])):
        data['course'] = 0
    new_data['cog'] = data['course']
    query = """
    INSERT INTO ais_type_9
    (rec_time, mmsi, altitude, sog, posAccuracy, posLon, posLat, cog)
    VALUES (%(rec_time)s, %(mmsi)s, %(altitude)s, %(sog)s, %(posAccuracy)s, %(posLon)s, %(posLat)s, %(cog)s);
    """
    cur = conn.cursor()
    cur.execute(query, new_data)
    cur.close()
    conn.commit()
# full tested
def insert_ais_type_18(conn, data):
    new_data = {}
    new_data['rec_time'] = curr_date
    if not str(data['mmsi']).isnumeric():
        data['mmsi'] = 0
    new_data['mmsi'] = data['mmsi']
    if not is_number(str(data['speed'])):
        data['speed'] = 0
    new_data['sog'] = data['speed']
    new_data['posAccuracy'] = data['accuracy']
    if not is_number(str(data['lon'])): 
        data['lon'] = 0
    new_data['posLon'] = data['lon']

    if not is_number(str(data['lat'])):
        data['lat'] = 0
    new_data['posLat'] = data['lat']
    if not str(data['course']).isnumeric():
        data['course'] = 0
    new_data['cog'] = data['course']
    if not str(data['heading']).isnumeric():
        data['heading'] = 0
    new_data['heading'] = data['heading']
    query = """
    INSERT INTO ais_type_18
    (rec_time, mmsi, sog, posAccuracy, posLon, posLat, cog, heading)
    VALUES (%(rec_time)s, %(mmsi)s, %(sog)s, %(posAccuracy)s, %(posLon)s, %(posLat)s, %(cog)s, %(heading)s);
    """
    cur = conn.cursor()
    cur.execute(query, new_data)
    cur.close()
    conn.commit()
# full tested
def insert_ais_type_19(conn, data):
    new_data = {}
    new_data['rec_time'] = curr_date
    if not str(data['mmsi']).isnumeric():
        data['mmsi'] = 0
    new_data['mmsi'] = data['mmsi']
    if not is_number(str(data['speed'])):
        data['speed'] = 0
    new_data['sog'] = data['speed']
    new_data['posAccuracy'] = data['accuracy']
    if not is_number(str(data['lon'])): 
        data['lon'] = 0
    new_data['posLon'] = data['lon']

    if not is_number(str(data['lat'])):
        data['lat'] = 0
    new_data['posLat'] = data['lat']
    if not str(data['course']).isnumeric():
        data['course'] = 0
    new_data['cog'] = data['course']
    if not str(data['heading']).isnumeric():
        data['heading'] = 0
    new_data['heading'] = data['heading']
    new_data['name_'] = data['shipname']
    new_data['type_'] = data['ship_type']
    new_data['toBow'] = data['to_bow']
    new_data['toStern'] = data['to_stern']
    new_data['toPort'] = data['to_port']
    new_data['toStarboard'] = data['to_starboard']
    query = """
    INSERT INTO ais_type_19
    (rec_time, mmsi, sog, posAccuracy, posLon, posLat, cog, heading, name_, type_, toBow, toStern, toPort, toStarboard)
    VALUES (%(rec_time)s, %(mmsi)s, %(sog)s, %(posAccuracy)s, %(posLon)s, %(posLat)s, %(cog)s, %(heading)s, %(name_)s, %(type_)s, %(toBow)s, %(toStern)s, %(toPort)s, %(toStarboard)s);
    """
    cur = conn.cursor()
    cur.execute(query, new_data)
    cur.close()
    conn.commit()
# full tested
def insert_ais_type_21(conn, data):
    new_data = {}
    new_data['rec_time'] = curr_date
    if not str(data['mmsi']).isnumeric():
        data['mmsi'] = 0
    new_data['mmsi'] = data['mmsi']
    new_data['aidType'] = data['aid_type']
    new_data['name_'] = data['name']
    new_data['posAccuracy'] = data['accuracy']
    if not is_number(str(data['lon'])): 
        data['lon'] = 0
    new_data['posLon'] = data['lon']

    if not is_number(str(data['lat'])):
        data['lat'] = 0
    new_data['posLat'] = data['lat']
    new_data['toBow'] = data['to_bow']
    new_data['toStern'] = data['to_stern']
    new_data['toPort'] = data['to_port']
    new_data['toStarboard'] = data['to_starboard']
    query = """
    INSERT INTO ais_type_21
    (rec_time, mmsi, aidType, name_, posAccuracy, posLon, posLat, toBow, toStern, toPort, toStarboard)
    VALUES (%(rec_time)s, %(mmsi)s, %(aidType)s, %(name_)s, %(posAccuracy)s, %(posLon)s, %(posLat)s, %(toBow)s, %(toStern)s, %(toPort)s, %(toStarboard)s);
    """
    cur = conn.cursor()
    cur.execute(query, new_data)
    cur.close()
    conn.commit()
# full tested
def insert_ais_type_24a(conn, data):
    new_data = {}
    new_data['rec_time'] = curr_date
    if not str(data['mmsi']).isnumeric():
        data['mmsi'] = 0
    new_data['mmsi'] = data['mmsi']
    new_data['name_'] = data['shipname']
    query = """
    INSERT INTO ais_type_24a
    (rec_time, mmsi, name_)
    VALUES (%(rec_time)s, %(mmsi)s, %(name_)s);
    """
    cur = conn.cursor()
    cur.execute(query, new_data)
    cur.close()
    conn.commit()
# full tested
def insert_ais_type_24b(conn, data):
    new_data = {}
    new_data['rec_time'] = curr_date
    if not str(data['mmsi']).isnumeric():
        data['mmsi'] = 0
    new_data['mmsi'] = data['mmsi']
    new_data['type_'] = data['ship_type']
    new_data['callsign'] = data['callsign']
    new_data['toBow'] = data['to_bow']
    new_data['toStern'] = data['to_stern']
    new_data['toPort'] = data['to_port']
    new_data['toStarboard'] = data['to_starboard']
    query = """
    INSERT INTO ais_type_24b
    (rec_time, mmsi, type_, callsign, toBow, toStern, toPort, toStarboard)
    VALUES (%(rec_time)s, %(mmsi)s, %(type_)s, %(callsign)s, %(toBow)s, %(toStern)s, %(toPort)s, %(toStarboard)s);
    """
    cur = conn.cursor()
    cur.execute(query, new_data)
    cur.close()
    conn.commit()
# full tested
def insert_ais_type_27(conn, data):
    new_data = {}
    new_data['rec_time'] = curr_date
    if not str(data['mmsi']).isnumeric():
        data['mmsi'] = 0
    new_data['mmsi'] = data['mmsi']
    new_data['posAccuracy'] = data['accuracy']
    new_data['navstatus'] = data['status']
    if not is_number(str(data['lon'])): 
        data['lon'] = 0
    new_data['posLon'] = data['lon']

    if not is_number(str(data['lat'])):
        data['lat'] = 0
    new_data['posLat'] = data['lat']
    if not str(data['course']).isnumeric():
        data['course'] = 0
    new_data['sog'] = data['speed']
    if not str(data['course']).isnumeric():
        data['course'] = 0
    new_data['cog'] = data['course']
    query = """
    INSERT INTO ais_type_27
    (rec_time, mmsi, posAccuracy, navstatus, posLon, posLat, sog, cog)
    VALUES (%(rec_time)s, %(mmsi)s, %(posAccuracy)s, %(navstatus)s, %(posLon)s, %(posLat)s, %(sog)s, %(cog)s);
    """
    cur = conn.cursor()
    cur.execute(query, new_data)
    cur.close()
    conn.commit()

def insert_adsb_type_1_4(conn, data):
    query = """
    INSERT INTO adsb_type_1_4
    (rec_time, icao, category, indet)
    VALUES (%(rec_time)s, %(icao)s, %(category)s, %(indet)s);
    """
    cur = conn.cursor()
    cur.execute(query, data)
    conn.commit()

def insert_adsb_type_5_8(conn, data):
    query = """
    INSERT INTO adsb_type_5_8
    (rec_time, icao, movement, ground_track, time_, latitude, longitude)
    VALUES (%(rec_time)s, %(icao)s, %(movement)s, %(ground_track)s, %(time_)s, %(latitude)s, %(longitude)s);
    """
    cur = conn.cursor()
    cur.execute(query, data)
    conn.commit()

def insert_adsb_type_9_18_20_22(conn, data):
    query = """
    INSERT INTO adsb_type_9_18_20_22
    (rec_time, icao, surveillance_status, single_antenna, altitude, time_, latitude, longitude)
    VALUES (%(rec_time)s, %(icao)s, %(surveillance_status)s, %(single_antenna)s, %(altitude)s, %(time_)s, %(latitude)s, %(longitude)s);
    """
    cur = conn.cursor()
    cur.execute(query, data)
    conn.commit()

def insert_adsb_type_19(conn, data):
    query = """
    INSERT INTO adsb_type_19
    (rec_time, icao, vertical_speed, horizontal_speed, angle, ver_speed_type, gnss_bar_dif)
    VALUES (%(rec_time)s, %(icao)s, %(vertical_speed)s, %(horizontal_speed)s, %(angle)s, %(ver_speed_type)s, %(gnss_bar_dif)s);
    """
    cur = conn.cursor()
    cur.execute(query, data)
    conn.commit()
 
def add_data(f, f_type, dbn, u, pas, h, p):
    filepath = pathlib.Path(__file__).parent.joinpath(f)
    if f_type == 'ais':
        conn = psycopg2.connect(dbname=dbn, user=u, password=pas, host=h, port = p)
        try:
            for msg in FileReaderStream(str(filepath)):
                try:
                    decoded = msg.decode().asdict()
                    if decoded['msg_type'] == 1 or decoded['msg_type'] == 2 or decoded['msg_type'] == 3:
                        insert_ais_type_1_2_3(conn, decoded)
                    if decoded['msg_type'] == 4 or decoded['msg_type'] == 11:
                        insert_ais_type_4_11(conn, decoded)
                    if decoded['msg_type'] == 5:
                        insert_ais_type_5(conn, decoded)
                    if decoded['msg_type'] == 9:
                        insert_ais_type_9(conn, decoded)
                    if decoded['msg_type'] == 18:
                        insert_ais_type_18(conn, decoded)
                    if decoded['msg_type'] == 19:
                        insert_ais_type_19(conn, decoded)
                    if decoded['msg_type'] == 21:
                        insert_ais_type_21(conn, decoded)
                    if decoded['msg_type'] == 24:
                        if decoded['partno'] == 0:
                            insert_ais_type_24a(conn, decoded)
                        if decoded['partno'] == 1:
                            insert_ais_type_24b(conn, decoded)
                    if decoded['msg_type'] == 27:
                        insert_ais_type_27(conn, decoded)
                except (Exception) as error:
                    print(error)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
    elif f_type == 'adsb':
        conn = psycopg2.connect(dbname=dbn, user=u, password=pas, host=h, port = p)
        try:
            print('adsb')
            with open(filepath, 'rb') as file:
                1 == 1
                #while True:
                    # Длина сообщения 18 байт
                    #fir = file.read(1) # channel
                    #if not fir:
                    #    break
                    #time = int.from_bytes(file.read(3), "little", signed=False)
                    #file.read(1) # type
                    #tmp = file.read(11)
                    #msg = format(int.from_bytes(tmp, "little", signed=False),'x')
                    #file.read(2)
                    #print(pms.adsb.typecode(msg))
                    #id = pms.adsb.typecode(msg)
                    #if id == 1:
                    #    insert_adsb_type_1_4(conn, msg)
                    #if id >= 5 and id <= 8:
                    #    insert_adsb_type_5_8(conn, msg)
                    #if (id >= 9 and id <= 18) or (id >= 20 and id <= 22):
                    #    insert_adsb_type_9_18_20_22(conn, msg)
                    #if id == 19:
                    #    insert_adsb_type_19(conn, msg)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

filename = 'test_data.ais'
#filename = '0.loic'
f_type = 'ais'
#f_type = 'adsb'
dbname = "adsb_ais_data"
user = "postgres"
password = ""
host = "127.0.0.1"
port = "5432"

add_data(filename, f_type, dbname, user, password, host, port)