import psycopg2
import requests
import json
import time
from datetime import datetime
from shapely.geometry import Point, Polygon
import logging
from plane import Plane
from config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, API_URL

# Configure logging
logging.basicConfig(level=logging.INFO)

# Dictionary to track the plane state
planes = {}

def main():
    while True:
        process_snapshot()
        time.sleep(10)

def connect_to_db():
    conn = psycopg2.connect(
        dbname=DB_NAME, 
        user=DB_USER, 
        password=DB_PASSWORD, 
        host=DB_HOST)
    return conn

def download(URL, retries=3, delay=5):
    for attempt in range(retries):
        try:
            response = requests.get(URL)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logging.error(f"Error downloading data (attempt {attempt + 1}/{retries}): {e}")
            time.sleep(delay)
    return None

airport_bounds = {
    'FYV': Polygon([(35.993452, -94.158442), (35.991369, -94.171840), (36.017515, -94.165646), (36.015293, -94.177069)]),
    'SPZ': Polygon([(36.184300, -94.121102), (36.184161, -94.114488), (36.167603, -94.117838), (36.167741, -94.124279)]),
    'XNA': Polygon([(36.295641, -94.319638), (36.298795, -94.305666), (36.270152, -94.290206), (36.265723, -94.304463)]),
    'VBT': Polygon([(36.355708, -94.222470), (36.355777, -94.215685), (36.336005, -94.215342), (36.336420, -94.223071)]),
    'ROG': Polygon([(36.384020, -94.106592), (36.382178, -94.095432), (36.362153, -94.105602), (36.363536, -94.115050)])
}

def get_airport(lat, lon):
    point = Point(lat, lon)
    for airport, polygon in airport_bounds.items():
        if polygon.contains(point):
            return airport
    return None

def check_takeoff(cur, flight_record, snapshot_id):
    first_snapshotid = flight_record[15]
    first_detected_gs = flight_record[3]
    first_detected_alt_baro = flight_record[4]
    first_detected_alt_geom = flight_record[5]
    first_detected_lat = flight_record[6]
    first_detected_lon = flight_record[7]

    airport = get_airport(first_detected_lat, first_detected_lon)

    if airport and (first_detected_alt_baro is None or first_detected_alt_geom is None or first_detected_gs is None or first_detected_gs < 30):
        cur.execute("""
            INSERT INTO tbl_takeoff (flightid, airportid, takeoff_snapshotid)
            VALUES (%s, (SELECT airport_id FROM tbl_airports WHERE iata_code = %s), %s)
        """, (flight_record[0], airport, first_snapshotid))

def check_landing(cur, flight_record, snapshot_id):
    last_snapshotid = flight_record[16]
    last_detected_gs = flight_record[9]
    last_detected_alt_baro = flight_record[10]
    last_detected_alt_geom = flight_record[11]
    last_detected_lat = flight_record[12]
    last_detected_lon = flight_record[13]

    airport = get_airport(last_detected_lat, last_detected_lon)

    if airport and (last_detected_alt_baro is None or last_detected_alt_geom is None or last_detected_gs is None or last_detected_gs < 30):
        cur.execute("""
            SELECT 1 FROM tbl_landing WHERE flightid = %s
        """, (flight_record[0],))
        exists = cur.fetchone()

        if exists:
            cur.execute("""
                UPDATE tbl_landing
                SET airportid = (SELECT airport_id FROM tbl_airports WHERE iata_code = %s),
                    landing_snapshotid = %s
                WHERE flightid = %s
            """, (airport, last_snapshotid, flight_record[0]))
        else:
            cur.execute("""
                INSERT INTO tbl_landing (flightid, airportid, landing_snapshotid)
                VALUES (%s, (SELECT airport_id FROM tbl_airports WHERE iata_code = %s), %s)
            """, (flight_record[0], airport, last_snapshotid))

        delete_duplicate_landing(cur, flight_record[0], airport, last_snapshotid)

def delete_duplicate_landing(cur, flightid, airport, landing_snapshotid):
    cur.execute("""
        SELECT landingid, landing_snapshotid
        FROM tbl_landing
        WHERE flightid = %s AND airportid = (SELECT airport_id FROM tbl_airports WHERE iata_code = %s)
        ORDER BY landingid DESC
        LIMIT 2
    """, (flightid, airport))
    rows = cur.fetchall()

    if len(rows) == 2 and rows[1][1] > rows[0][1]:
        cur.execute("""
            DELETE FROM tbl_landing
            WHERE landingid = %s
        """, (rows[0][0],))

def process_snapshot():
    URL = API_URL
    temp = download(URL)
    if temp is None:
        logging.error("No data downloaded.")
        return

    try:
        data = json.loads(temp)
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON: {e}")
        return

    if data is not None:
        conn = None
        try:
            conn = connect_to_db()
            cur = conn.cursor()

            cur.execute("INSERT INTO tbl_SnapShot (SnapshotDateTime) VALUES (%s) RETURNING SnapshotID;", (datetime.now(),))
            new_snapshot_id = cur.fetchone()[0]
            conn.commit()

            for item in data.get('ac', []):
                if 'r' not in item:
                    continue

                aircraft_id = item['r']
                flight = item.get('flight', None)
                t = item.get('t', None)
                alt_baro = item.get('alt_baro', None)
                try:
                    if alt_baro is not None:
                        alt_baro = int(alt_baro)
                    else:
                        alt_baro = None
                except ValueError:
                    alt_baro = None
                alt_geom = item.get('alt_geom', None)
                gs = item.get('gs', None)
                lat = item.get('lat', None)
                lon = item.get('lon', None)
                track = item.get('track', None)
                squawk = item.get('squawk', "")
                
                if squawk == '':
                    squawk = None
                
                cur.execute("""
                    INSERT INTO tbl_Aircraft (SnapShotID, AircraftID, flight, t, alt_baro, alt_geom, gs, lat, lon, track, squawk)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING AircraftRecordID;
                """, (new_snapshot_id, aircraft_id, flight, t, alt_baro, alt_geom, gs, lat, lon, track, squawk))
                
                if aircraft_id is not None:
                    if aircraft_id not in planes:
                        planes[aircraft_id] = Plane(aircraft_id)
                    
                    planes[aircraft_id].update_state(gs, alt_baro, alt_geom)

                if aircraft_id is not None and flight is not None and squawk is not None:
                    cur.execute("""
                        SELECT FlightID, first_detected_time, last_detected_time, 
                               first_detected_gs, first_detected_alt_baro, first_detected_alt_geom, 
                               first_detected_lat, first_detected_lon, first_detected_track,
                               last_detected_gs, last_detected_alt_baro, last_detected_alt_geom, 
                               last_detected_lat, last_detected_lon, last_detected_track,
                               first_snapshotid, last_snapshotid
                        FROM tbl_uniqueflights
                        WHERE AircraftID = %s AND flight = %s AND squawk = %s
                    """, (aircraft_id, flight, squawk))
                    
                    flight_record = cur.fetchone()

                    if flight_record:
                        flight_id = flight_record[0]
                        last_detected_time = flight_record[2]

                        if datetime.now() > last_detected_time:
                            cur.execute("""
                                UPDATE tbl_uniqueflights
                                SET last_detected_time = %s,
                                    last_detected_gs = %s,
                                    last_detected_alt_baro = %s,
                                    last_detected_alt_geom = %s,
                                    last_detected_lat = %s,
                                    last_detected_lon = %s,
                                    last_detected_track = %s,
                                    last_snapshotid = %s
                                WHERE FlightID = %s
                            """, (datetime.now(), gs, alt_baro, alt_geom, lat, lon, track, new_snapshot_id, flight_id))

                            # Call check_landing with new_snapshot_id
                            if planes[aircraft_id].is_on_ground:
                                check_landing(cur, flight_record, new_snapshot_id)

                    else:
                        try:
                            cur.execute("""
                                INSERT INTO tbl_uniqueflights (
                                    AircraftID, flight, squawk,
                                    first_detected_time, first_detected_gs,
                                    first_detected_alt_baro, first_detected_alt_geom,
                                    first_detected_lat, first_detected_lon,
                                    first_detected_track,
                                    last_detected_time, last_detected_gs,
                                    last_detected_alt_baro, last_detected_alt_geom,
                                    last_detected_lat, last_detected_lon,
                                    last_detected_track,
                                    first_snapshotid, last_snapshotid
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                )
                            """, (
                                aircraft_id, flight, squawk,
                                datetime.now(), gs, alt_baro, alt_geom, lat, lon, track,
                                datetime.now(), gs, alt_baro, alt_geom, lat, lon, track,
                                new_snapshot_id, new_snapshot_id
                            ))

                            cur.execute("""
                                SELECT FlightID, first_detected_time, last_detected_time, 
                                       first_detected_gs, first_detected_alt_baro, first_detected_alt_geom, 
                                       first_detected_lat, first_detected_lon, first_detected_track,
                                       last_detected_gs, last_detected_alt_baro, last_detected_alt_geom, 
                                       last_detected_lat, last_detected_lon, last_detected_track,
                                       first_snapshotid, last_snapshotid
                                FROM tbl_uniqueflights
                                WHERE AircraftID = %s AND flight = %s AND squawk = %s
                            """, (aircraft_id, flight, squawk))
                            flight_record = cur.fetchone()

                            # Call check_takeoff with the new flight_record
                            if planes[aircraft_id].is_on_ground:
                                check_takeoff(cur, flight_record, new_snapshot_id)

                        except Exception as e:
                            logging.error(f"Error inserting into tbl_uniqueflights: {e}")

            conn.commit()
            logging.info(datetime.now())

        except Exception as e:
            logging.error(f"Error processing snapshot: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

if __name__ == "__main__":
    main()