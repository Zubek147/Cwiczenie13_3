import csv
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer, Date, exc
from datetime import datetime

# Tworzenie bazy danych SQLite
engine = create_engine('sqlite:///weather_data.db', echo=True)

metadata = MetaData()

# Tworzenie tabeli stations, jeśli nie istnieje
stations = Table('stations', metadata,
    Column('station', String, primary_key=True),
    Column('latitude', Float),
    Column('longitude', Float),
    Column('elevation', Float),
    Column('name', String),
    Column('country', String),
    Column('state', String)
)

if not engine.dialect.has_table(engine, "stations"):
    metadata.create_all(engine)

# Tworzenie tabeli measure, jeśli nie istnieje
measure = Table('measure', metadata,
    Column('station', String),
    Column('date', Date),
    Column('precip', Float),
    Column('tobs', Integer)
)

if not engine.dialect.has_table(engine, "measure"):
    metadata.create_all(engine)

# Wczytanie danych z pliku "clean_stations.csv" do tabeli stations
with open('clean_stations.csv', 'r') as file:
    reader = csv.DictReader(file)
    conn = engine.connect()
    trans = conn.begin()  # Rozpocznij transakcję

    try:
        for row in reader:
            conn.execute(stations.insert().values(
                station=row['station'],
                latitude=float(row['latitude']),
                longitude=float(row['longitude']),
                elevation=float(row['elevation']),
                name=row['name'],
                country=row['country'],
                state=row['state']
            ))
        trans.commit()  # Zakończ transakcję po poprawnym wstawieniu wszystkich danych
    except exc.IntegrityError:
        trans.rollback()  # Anuluj transakcję w przypadku błędu IntegrityError (np. duplikat klucza głównego)

# Wczytanie danych z pliku "clean_measure.csv" do tabeli measure
with open('clean_measure.csv', 'r') as file:
    reader = csv.DictReader(file)
    conn = engine.connect()
    trans = conn.begin()  # Rozpocznij transakcję

    try:
        for row in reader:
            conn.execute(measure.insert().values(
                station=row['station'],
                date=datetime.strptime(row['date'], '%Y-%m-%d'),
                precip=float(row['precip']),
                tobs=int(row['tobs'])
            ))
        trans.commit()  # Zakończ transakcję po poprawnym wstawieniu wszystkich danych
    except exc.IntegrityError:
        trans.rollback()  # Anuluj transakcję w przypadku błędu IntegrityError (np. duplikat klucza głównego)


result = conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
print(result)

result = conn.execute("SELECT * FROM measure LIMIT 5").fetchall()
print(result)
