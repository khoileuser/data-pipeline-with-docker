import datetime
import gzip
import os
import re
import sys
import uuid

import pandas as pd
from cassandra.cluster import BatchStatement, Cluster, ConsistencyLevel
from cassandra.query import dict_factory

tablename = os.getenv("weather.table", "weatherreport")
fakertable = os.getenv("fakertable.table", "fakerdata")

# Must match cassandra/schema-faker.cql and the producer's payload keys.
FAKER_COLUMNS = [
    "user_id",
    "username",
    "name",
    "email",
    "phone",
    "birthdate",
    "job",
    "company",
    "address",
    "city",
    "postcode",
    "country",
    "passport_number",
    "credit_card_provider",
    "credit_card_number",
    "credit_card_expire",
    "ip_address",
    "user_agent",
    "registered_at",
]

# Must match cassandra/schema-earthquake.cql and the producer's payload keys.
EARTHQUAKE_COLUMNS = [
    "id",
    "place",
    "magnitude",
    "mag_type",
    "event_time",
    "updated_time",
    "latitude",
    "longitude",
    "depth_km",
    "significance",
    "felt_reports",
    "alert_level",
    "tsunami",
    "status",
    "event_type",
    "network",
    "url",
]
EARTHQUAKE_INT_COLUMNS = ["significance", "felt_reports", "tsunami"]
EARTHQUAKE_TIME_COLUMNS = ["event_time", "updated_time"]


CASSANDRA_HOST = (
    os.environ.get("CASSANDRA_HOST")
    if os.environ.get("CASSANDRA_HOST")
    else "localhost"
)
CASSANDRA_KEYSPACE = (
    os.environ.get("CASSANDRA_KEYSPACE")
    if os.environ.get("CASSANDRA_KEYSPACE")
    else "kafkapipeline"
)

WEATHER_TABLE = (
    os.environ.get("WEATHER_TABLE") if os.environ.get("WEATHER_TABLE") else "weather"
)
FAKER_TABLE = (
    os.environ.get("FAKER_TABLE") if os.environ.get("FAKER_TABLE") else "faker"
)
EARTHQUAKE_TABLE = os.environ.get("EARTHQUAKE_TABLE") or "earthquakedata"


def saveFakerDf(dfrecords):
    if isinstance(CASSANDRA_HOST, list):
        cluster = Cluster(CASSANDRA_HOST)
    else:
        cluster = Cluster([CASSANDRA_HOST])

    session = cluster.connect(CASSANDRA_KEYSPACE)

    counter = 0
    totalcount = 0

    cqlsentence = (
        "INSERT INTO "
        + fakertable
        + " ("
        + ", ".join(FAKER_COLUMNS)
        + ") VALUES ("
        + ", ".join("?" * len(FAKER_COLUMNS))
        + ")"
    )
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    insert = session.prepare(cqlsentence)
    batches = []
    for idx, val in dfrecords.iterrows():
        row = dict(val)
        row["user_id"] = uuid.UUID(str(row["user_id"]))
        row["birthdate"] = pd.to_datetime(row["birthdate"]).date()
        row["registered_at"] = pd.to_datetime(row["registered_at"]).to_pydatetime()
        batch.add(insert, tuple(row[c] for c in FAKER_COLUMNS))
        counter += 1
        if counter >= 100:
            print("inserting " + str(counter) + " records")
            totalcount += counter
            counter = 0
            batches.append(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    if counter != 0:
        batches.append(batch)
        totalcount += counter
    rs = [session.execute(b, trace=True) for b in batches]

    print("Inserted " + str(totalcount) + " rows in total")


def saveEarthquakeDf(dfrecords):
    if isinstance(CASSANDRA_HOST, list):
        cluster = Cluster(CASSANDRA_HOST)
    else:
        cluster = Cluster([CASSANDRA_HOST])

    session = cluster.connect(CASSANDRA_KEYSPACE)

    counter = 0
    totalcount = 0

    cqlsentence = (
        "INSERT INTO "
        + EARTHQUAKE_TABLE
        + " ("
        + ", ".join(EARTHQUAKE_COLUMNS)
        + ") VALUES ("
        + ", ".join("?" * len(EARTHQUAKE_COLUMNS))
        + ")"
    )
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    insert = session.prepare(cqlsentence)
    batches = []
    for idx, val in dfrecords.iterrows():
        # Missing values (e.g. felt_reports, alert_level) must be None, not NaN
        row = {c: (None if pd.isna(val[c]) else val[c]) for c in EARTHQUAKE_COLUMNS}
        for c in EARTHQUAKE_TIME_COLUMNS:
            if row[c] is not None:
                row[c] = pd.to_datetime(row[c]).to_pydatetime()
        for c in EARTHQUAKE_INT_COLUMNS:
            if row[c] is not None:
                row[c] = int(row[c])
        batch.add(insert, tuple(row[c] for c in EARTHQUAKE_COLUMNS))
        counter += 1
        if counter >= 100:
            print("inserting " + str(counter) + " records")
            totalcount += counter
            counter = 0
            batches.append(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    if counter != 0:
        batches.append(batch)
        totalcount += counter
    rs = [session.execute(b, trace=True) for b in batches]

    print("Inserted " + str(totalcount) + " rows in total")


def saveWeatherreport(dfrecords):
    if isinstance(CASSANDRA_HOST, list):
        cluster = Cluster(CASSANDRA_HOST)
    else:
        cluster = Cluster([CASSANDRA_HOST])

    session = cluster.connect(CASSANDRA_KEYSPACE)

    counter = 0
    totalcount = 0

    cqlsentence = (
        "INSERT INTO "
        + tablename
        + " (forecastdate, location, description, temp, feels_like, temp_min, temp_max, pressure, humidity, wind, sunrise, sunset) \
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    insert = session.prepare(cqlsentence)
    batches = []
    for idx, val in dfrecords.iterrows():
        batch.add(
            insert,
            (
                val["report_time"],
                val["location"],
                val["description"],
                val["temp"],
                val["feels_like"],
                val["temp_min"],
                val["temp_max"],
                val["pressure"],
                val["humidity"],
                val["wind"],
                val["sunrise"],
                val["sunset"],
            ),
        )
        counter += 1
        if counter >= 100:
            print("inserting " + str(counter) + " records")
            totalcount += counter
            counter = 0
            batches.append(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    if counter != 0:
        batches.append(batch)
        totalcount += counter
    rs = [session.execute(b, trace=True) for b in batches]

    print("Inserted " + str(totalcount) + " rows in total")


def loadDF(targetfile, target):
    if target == "weather":
        colsnames = [
            "description",
            "temp",
            "feels_like",
            "temp_min",
            "temp_max",
            "pressure",
            "humidity",
            "wind",
            "sunrise",
            "sunset",
            "location",
            "report_time",
        ]
        dfData = pd.read_csv(targetfile, header=None, parse_dates=True, names=colsnames)
        dfData["report_time"] = pd.to_datetime(dfData["report_time"])
        saveWeatherreport(dfData)
    elif target == "faker":
        dfData = pd.read_csv(targetfile, header=None, names=FAKER_COLUMNS)
        saveFakerDf(dfData)
    elif target == "earthquake":
        dfData = pd.read_csv(targetfile, header=None, names=EARTHQUAKE_COLUMNS)
        saveEarthquakeDf(dfData)


def getWeatherDF():
    return getDF(WEATHER_TABLE)


def getFakerDF():
    print(FAKER_TABLE)
    return getDF(FAKER_TABLE)


def getEarthquakeDF():
    return getDF(EARTHQUAKE_TABLE)


def getDF(source_table):
    if isinstance(CASSANDRA_HOST, list):
        cluster = Cluster(CASSANDRA_HOST)
    else:
        cluster = Cluster([CASSANDRA_HOST])

    if source_table not in (
        WEATHER_TABLE,
        FAKER_TABLE,
        EARTHQUAKE_TABLE,
    ):
        return None

    session = cluster.connect(CASSANDRA_KEYSPACE)
    session.row_factory = dict_factory
    cqlquery = "SELECT * FROM " + source_table + ";"
    rows = session.execute(cqlquery)
    return pd.DataFrame(rows)


if __name__ == "__main__":
    action = sys.argv[1]
    target = sys.argv[2]
    targetfile = sys.argv[3]
    if action == "save":
        loadDF(targetfile, target)
    elif action == "get":
        getDF(target)
