# -*- coding: utf-8 -*-

import collections
import csv
import datetime
import cassandra.cluster
import textwrap
import numpy as np

limiteur = lambda generator, limit: (data for _, data in zip(range(limit), generator))

Trip = collections.namedtuple(
    "Trip",
    ("start", "stop", "start_lon", "start_lat", "stop_lon", "stop_lat", "bikeid"),
)

def read_csv(filename):
    """Read the file.

    Parameters
    ----------
    filename : string
               The file to read.
    """
    with open(filename) as f:
        for row in csv.DictReader(f):
            start = datetime.datetime.strptime(
                row["starttime"] + "00", "%Y-%m-%d %H:%M:%S.%f"
            )
            stop = datetime.datetime.strptime(
                row["stoptime"] + "00", "%Y-%m-%d %H:%M:%S.%f"
            )
            start_lat = float(row["start station latitude"])
            start_lon = float(row["start station longitude"])
            stop_lat = float(row["end station latitude"])
            stop_lon = float(row["end station longitude"])
            bikeid = int(row["bikeid"])
            yield Trip(start, stop, start_lon, start_lat, stop_lon, stop_lat, bikeid)

def _insert_query_by_startdayhour(trip):
    """Build the query to insert datas in the DB.

    Parameters
    ----------
    trip : Trip
           The trip to insert in the DB.

    Return
    ------
    query : string
            The query to execute.
    """
    query = textwrap.dedent(
        f"""
        INSERT INTO trip_by_startdayhour
        (
            starttime_year,
            starttime_month,
            starttime_day,
            starttime_hour,
            starttime,
            stoptime,
            start_lat,
            stop_lat,
            start_lon,
            stop_lon,
            bikeid
        )
        VALUES
        (
            {trip.start.year},
            {trip.start.month},
            {trip.start.day},
            {trip.start.hour},
            '{trip.start.isoformat()[:23]}',
            '{trip.stop.isoformat()[:23]}',
            {trip.start_lat:.10f},
            {trip.stop_lat:.10f},
            {trip.start_lon:.10f},
            {trip.stop_lon:.10f},
            {trip.bikeid}
        );
        """
    )
    return query

INSERTS_Q = (
    _insert_query_by_startdayhour,
)

def insert_datastream(stream):
    """Insert datas into the DB.

    Parameters
    ----------
    stream : iterable
             Iterable where values to insert are taken.
    """
    cluster = cassandra.cluster.Cluster()
    session = cluster.connect("paroisem")
    for trip in stream:
        for q in INSERTS_Q:
            query = q(trip)
            session.execute(query)

def get_trip_one_day_hour(dt):
    """Retrieve datas of a precise clock.

    Parameters
    ----------
    dt : object datetime
         Clock of the datas to retrieve.
    """
    def run():
        query = textwrap.dedent(
            f"""
            SELECT
            starttime, stoptime, start_lon, start_lat, stop_lon, stop_lat, bikeid
            FROM
            trip_by_startdayhour
            WHERE
                    starttime_year={dt.year}
                AND
                    starttime_month={dt.month}
                AND
                    starttime_day={dt.day}
                AND
                    starttime_hour={dt.hour}
            ;
            """
        )
        cluster = cassandra.cluster.Cluster()
        session = cluster.connect("paroisem")
        for r in session.execute(query):
            yield Trip(
                r.starttime,
                r.stoptime,
                r.start_lon,
                r.start_lat,
                r.stop_lon,
                r.stop_lat,
                r.bikeid,
            )
	# Ou se connecter une seule fois seulement, faire une classe avec la connection en attribut
	cluster.shutdown() 
    return run
