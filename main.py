import psycopg2
import requests
import json
import time
from datetime import datetime
from shapely.geometry import Point, Polygon
import logging
from geopy.distance import geodesic
from plane import Plane
from config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, API_URL

# configure logging
logging.basicConfig(level=logging.INFO)

# dictionary to track the plane state
planes = {}

# main function to rerun process_snapshot every n seconds
def main():
    while True:
        process_snapshot()
        time.sleep(10) # however many seconds I want between runs

# getting creds for DB connection
def connect_to_db():
    conn = psycopg2.connect(
        dbname=DB_NAME, 
        user=DB_USER, 
        password=DB_PASSWORD, 
        host=DB_HOST)
    return conn

# download API url stuff made to retry if there are issues
def download(URL, retries=3, delay=5):
    # loops for however many retries
    for attempt in range(retries):
        try:
            response = requests.get(URL)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logging.error(f"Error downloading data (attempt {attempt + 1}/{retries}): {e}")
            time.sleep(delay)
    return None

# create dictionary of airport bounds to use for takeoff and landing logic
# NO LONGER USING THIS DUE TO SWITCH TO GEOFENCING
airport_bounds = {
    'FYV': Polygon([(35.977155, -94.141844), (35.953203, -94.179686), (36.042017, -94.198473), (36.043991, -94.164063)]),
    'SPZ': Polygon([(36.184300, -94.121102), (36.184161, -94.114488), (36.167603, -94.117838), (36.167741, -94.124279)]),
    'XNA': Polygon([(36.295641, -94.319638), (36.298795, -94.305666), (36.270152, -94.290206), (36.265723, -94.304463)]),
    'VBT': Polygon([(36.355708, -94.222470), (36.355777, -94.215685), (36.336005, -94.215342), (36.336420, -94.223071)]),
    'ROG': Polygon([(36.384020, -94.106592), (36.382178, -94.095432), (36.362153, -94.105602), (36.363536, -94.115050)])
}

airport_centers = {
    'FYV': (36.0034, -94.1719),
    'ROG': (36.372, -94.107),
    'XNA': (36.2806, -94.3046),
    'SPZ': (36.1740, -94.1222),
    'VBT': (36.3458, -94.2198)
}

# geofence radius in kilometers
GEOFENCE_RADIUS = 1

# function to get airport based on coordinates of plane
def get_airport(lat, lon):
    point = Point(lat, lon)
    for airport, polygon in airport_bounds.items():
        if polygon.contains(point):
            return airport
    return None

# check proximity to airports using geofencing
def get_nearby_airport(lat, lon):
    plane_coords = (lat, lon)
    for airport, center_coords in airport_centers.items():
        if geodesic(center_coords, plane_coords).km <= GEOFENCE_RADIUS:
            return airport
    return None

def check_takeoff(cur, plane, lat, lon, flight_record):
    last_snapshotid = flight_record[16]
    airport = get_nearby_airport(lat, lon)
    if airport and plane.is_takeoff():
        logging.info(f"Detected takeoff at {airport} for plane {plane.aircraft_id}")

        # check if the flight exists in tbl_takeoff
        cur.execute("""
            SELECT 1 FROM tbl_takeoff WHERE flightid = %s
        """, (flight_record[0],))
        exists = cur.fetchone()

        if exists:
            cur.execute("""
                UPDATE tbl_takeoff
                SET airportid = (SELECT airport_id FROM tbl_airports WHERE iata_code = %s),
                    takeoff_snapshotid = %s
                WHERE flightid = %s
            """, (airport, last_snapshotid, flight_record[0]))
        else:
            cur.execute("""
                INSERT INTO tbl_takeoff (flightid, airportid, takeoff_snapshotid)
                VALUES (%s, (SELECT airport_id FROM tbl_airports WHERE iata_code = %s), %s)
            """, (flight_record[0], airport, last_snapshotid))

        delete_duplicate_takeoff(cur, flight_record[0], airport)

# going back to delete any duplicate takeoff entries
def delete_duplicate_takeoff(cur, flightid, airport):
    cur.execute("""
        SELECT takeoffid, takeoff_snapshotid
        FROM tbl_takeoff
        WHERE flightid = %s AND airportid = (SELECT airport_id FROM tbl_airports WHERE iata_code = %s)
        ORDER BY takeoffid DESC
        LIMIT 2
    """, (flightid, airport))
    rows = cur.fetchall()

    if len(rows) == 2:
        # delete the older record (with the smaller snapshot ID)
        if rows[1][1] < rows[0][1]:
            cur.execute("""
                DELETE FROM tbl_takeoff
                WHERE takeoffid = %s
            """, (rows[1][0],))
        else:
            cur.execute("""
                DELETE FROM tbl_takeoff
                WHERE takeoffid = %s
            """, (rows[0][0],))

def check_landing(cur, plane, lat, lon, flight_record):
    last_snapshotid = flight_record[16]
    airport = get_nearby_airport(lat, lon)
    if airport and plane.is_landing():
        logging.info(f"Detected landing at {airport} for plane {plane.aircraft_id}")

        # check if the flight exists in tbl_landing
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

        delete_duplicate_landing(cur, flight_record[0], airport)

def delete_duplicate_landing(cur, flightid, airport):
    cur.execute("""
        SELECT landingid, landing_snapshotid
        FROM tbl_landing
        WHERE flightid = %s AND airportid = (SELECT airport_id FROM tbl_airports WHERE iata_code = %s)
        ORDER BY landingid DESC
        LIMIT 2
    """, (flightid, airport))
    rows = cur.fetchall()

    if len(rows) == 2:
        # delete the older record (with the smaller snapshot ID)
        if rows[1][1] < rows[0][1]:
            cur.execute("""
                DELETE FROM tbl_landing
                WHERE landingid = %s
            """, (rows[1][0],))
        else:
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
                aircraft_id = item.get('r') or item.get('hex')
                if not aircraft_id or not aircraft_id.strip():
                    logging.warning(f"Skipping plane due to missing or invalid aircraft_id: {item}")
                    continue
                flight = item.get('flight', None)
                if flight is None or not flight.strip():  # Validate flight
                    logging.warning(f"Skipping plane {aircraft_id} due to missing or invalid flight value")
                    continue
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
                
                # update plane states
                if aircraft_id is not None:
                    if aircraft_id not in planes:
                        planes[aircraft_id] = Plane(aircraft_id)

                    planes[aircraft_id].update_state(gs, alt_baro, alt_geom, new_snapshot_id)

                    # check for snapshot gap and reset flight if needed
                    if planes[aircraft_id].has_snapshot_gap(new_snapshot_id):
                        # reset the flight tracking to treat it as a new unique flight
                        flight = None

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

                            # call check_takeoff and check_landing here
                            if lat is not None and lon is not None:
                                check_takeoff(cur, planes.get(aircraft_id, Plane(aircraft_id)), lat, lon, flight_record)
                                check_landing(cur, planes.get(aircraft_id, Plane(aircraft_id)), lat, lon, flight_record)

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
