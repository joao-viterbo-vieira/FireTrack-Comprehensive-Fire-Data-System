import psycopg
import pandas as pd


conn = psycopg.connect("""
  dbname= xxxxx
  user=xxxxxx
  password=xxxxx
  host= xxxxx
  port=xxxx
  options=xxxxxx
  """)

delete_order = [
    "fire_vehicle_assignment",
    "fire_firefighter_assignment",
    "firetruck",
    "watertank",
    "helicopter",
    "ambulance",
    "vehicles",
    "firefighters",
    "fires",
    "firestations",
    "parish",
    "model",
    "cause",
]


cur = conn.cursor()

for table in delete_order:
    cur.execute(f"DELETE FROM {table}")

conn.commit()

print("Loading... This may take 3-4 minutes for 180,000 rows.")

# Use this for the application fires.py, it inserts fast!!!
# df = pd.read_csv('Registos_Incendios_SGIF_2011_2020.csv', encoding='ISO-8859-1', nrows=1000)

df = pd.read_csv(
    "Registos_Incendios_SGIF_2011_2020.csv", encoding="ISO-8859-1", nrows=177000
)

df.rename(
    columns={"Ano": "year", "Mes": "month", "Dia": "day", "Hora": "hour"}, inplace=True
)

# Now, try to create the datetime column
df["datetime"] = pd.to_datetime(df[["year", "month", "day", "hour"]])


print(df[["year", "month", "day", "hour", "datetime"]].head())


df_fires_extract = df[
    [
        "Codigo_SGIF",
        "datetime",
        "AreaTotal_ha",
        "DataHoraAlerta",
        "DataHora_PrimeiraIntervencao",
        "DataHora_Extincao",
        "Latitude",
        "Longitude",
        "DSR",
        "FWI",
        "ISI",
        "DC",
        "DMC",
        "FFMC",
        "BUI",
        "FonteAlerta",
        "CodCausa",
        "DTCCFR",
    ]
]
df_parish = df[["DTCCFR", "Freguesia", "Concelho", "Distrito"]]
df_cause = df[["CodCausa", "TipoCausa", "GrupoCausa", "DescricaoCausa"]]

df_parish = df_parish.drop_duplicates(subset=["DTCCFR"])
df_parish = df_parish.drop_duplicates(subset=["DTCCFR"])
df_cause = df_cause.drop_duplicates()

df_parish = df_parish.dropna()
df_cause = df_cause.dropna()


# print(df_fires_extract.head())
# print(df_parish.head())
# print(df_cause.head())
from psycopg2 import connect


def insert_data(table, columns, df, batch_size=100):
    placeholders = ", ".join(["%s"] * len(columns))
    query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

    batch = []
    for index, row in df.iterrows():
        batch.append(tuple(row.where(pd.notnull(row), None)))

        # Commit every batch_size rows
        if len(batch) == batch_size:
            try:
                cur.executemany(query, batch)
                conn.commit()
                batch = []  # Clear batch after committing
            except Exception as e:
                print(f"Error inserting batch ending at row {index} into {table}: {e}")
                conn.rollback()  # Rollback if there's an error in the batch
                batch = []  # Clear batch after rollback

    # Commit any remaining rows
    if batch:
        try:
            cur.executemany(query, batch)
            conn.commit()
        except Exception as e:
            print(f"Error inserting final batch into {table}: {e}")
            conn.rollback()


cur = conn.cursor()

# Tabela "parish"
parish_columns = ["code_DTCCFR", "name", "municipality", "district"]
insert_data("parish", parish_columns, df_parish)

# Tabela "cause"
cause_columns = ["cod", "type", "grp", "description"]
insert_data("cause", cause_columns, df_cause)

# Convert date and timestamp columns to datetime format using .loc to avoid SettingWithCopyWarning
df_fires_extract.loc[:, "datetime"] = pd.to_datetime(
    df_fires_extract["datetime"], errors="coerce"
)
df_fires_extract.loc[:, "DataHoraAlerta"] = pd.to_datetime(
    df_fires_extract["DataHoraAlerta"], errors="coerce"
)
df_fires_extract.loc[:, "DataHora_PrimeiraIntervencao"] = pd.to_datetime(
    df_fires_extract["DataHora_PrimeiraIntervencao"], errors="coerce"
)
df_fires_extract.loc[:, "DataHora_Extincao"] = pd.to_datetime(
    df_fires_extract["DataHora_Extincao"], errors="coerce"
)


fires_columns = [
    "code_SGIF",
    "date_time",
    "total_area_ha",
    "TIMESTAMP_alert",
    "TIMESTAMP_first_intervention",
    "TIMESTAMP_extinction",
    "latitude",
    "longitude",
    "DSR",
    "FWI",
    "ISI",
    "DC",
    "DMC",
    "FFMC",
    "BUI",
    "alert_source",
    "cod",
    "code_DTCCFR",
]
insert_data("fires", fires_columns, df_fires_extract)

query = "INSERT INTO model (id, name, make) VALUES \
        (1, 'Atego', 'Mercedes-Benz'), \
        (2, 'FMX', 'Volvo'), \
        (3, 'H135', 'Airbus'), \
        (4, 'Transit', 'Ford'), \
        (5, 'TGM', 'MAN'), \
        (6, 'P-Series', 'Scania'),\
        (7, 'Bell 412', 'Bell'), \
        (8, 'Express', 'Chevrolet'); \
        \
        INSERT INTO firestations (id, name, capacity_vehicles, capacity_firefighters, address, code_DTCCFR) VALUES \
        (1, 'Quartel de Bombeiros Central', 10, 50, 'Rua Principal, Centro da Cidade', 101514),\
        (2, 'Quartel de Bombeiros Leste', 8, 40, 'Rua do Leste, Bairro Leste', 61307),\
        (3, 'Quartel de Bombeiros Oeste', 6, 30, 'Rua do Oeste, Bairro Oeste', 11909),\
        (4, 'Quartel de Bombeiros Norte', 5, 25, 'Rua do Norte, Bairro Norte', 101402),\
        (5, 'Quartel de Bombeiros Sul', 7, 35, 'Rua do Sul, Bairro Sul', 131701),\
        (6, 'Quartel de Bombeiros Aeroporto', 10, 60, 'Rua do Aeroporto, Zona Aeroporto', 161014),\
        (7, 'Quartel de Bombeiros Universidade', 4, 20, 'Rua da Universidade, Bairro Universitário', 60703),\
        (8, 'Quartel de Bombeiros Baixa', 9, 45, 'Rua da Baixa, Centro Histórico', 61301),\
        (9, 'Quartel de Bombeiros Industrial', 8, 40, 'Rua da Indústria, Zona Industrial', 90608),\
        (10, 'Quartel de Bombeiros Subúrbio', 7, 30, 'Rua do Subúrbio, Bairro Suburbano', 50314);\
        \
        INSERT INTO vehicles (registration_plate, status, last_maintenance_date, capacity, model_id, firestation_id) VALUES \
        ('ABC123', 'Operational', '2023-08-01', 2000, 1, 1),\
        ('XYZ789', 'Under Maintenance', '2023-07-15', 3000, 2, 2),\
        ('HEL456', 'Operational', '2023-09-10', 1000, 3, 3),\
        ('AMB321', 'Operational', '2023-08-20', 5, 4, 4),\
        ('FTK102', 'Operational', '2023-07-20', 2500, 1, 5),\
        ('WTK303', 'Operational', '2023-06-25', 12000, 2, 6),\
        ('HEL789', 'Operational', '2023-08-14', 1500, 3, 7),\
        ('AMB654', 'Operational', '2023-09-05', 6, 4, 8),\
        ('FTK201', 'Operational', '2023-09-01', 2200, 5, 9),\
        ('WTK808', 'Operational', '2023-08-30', 13000, 6, 10),\
        ('HEL901', 'Operational', '2023-07-07', 1200, 7, 1),\
        ('AMB002', 'Operational', '2023-06-19', 5, 8, 2),\
        ('FTK300', 'Operational', '2023-09-14', 2300, 1, 3),\
        ('WTK404', 'Under Maintenance', '2023-08-01', 11000, 2, 4),\
        ('HEL123', 'Operational', '2023-09-22', 1400, 3, 5),\
        ('AMB999', 'Operational', '2023-10-03', 6, 4, 6),\
        ('FTK456', 'Operational', '2023-09-18', 2000, 5, 7),\
        ('WTK505', 'Operational', '2023-07-23', 12500, 6, 8),\
        ('HEL888', 'Operational', '2023-10-11', 1000, 7, 9),\
        ('AMB777', 'Operational', '2023-09-25', 7, 8, 10);\
        \
        INSERT INTO firefighters (code, name, rank, contact, status, starting_date, certifications, firestation_id) VALUES \
        (101, 'João Silva', 'Tenente', '555-1234', 'Ativo', '2021-06-15', 'Certificado em Suporte Básico de Vida', 1),\
        (102, 'Ana Santos', 'Capitão', '555-5678', 'Licença', '2019-03-22', 'Técnicas Avançadas de Combate a Incêndios', 2),\
        (103, 'Carlos Pereira', 'Sargento', '555-8765', 'Ativo', '2022-10-10', 'Certificado Hazmat', 3),\
        (104, 'Maria Oliveira', 'Soldado', '555-4321', 'Ativo', '2020-05-05','Certificado em Suporte Básico de Vida', 4),\
        (105, 'Miguel Costa', 'Sargento', '555-1122', 'Ativo', '2021-07-22', 'Primeiros Socorros', 5),\
        (106, 'Luciana Lopes', 'Capitão', '555-3344', 'Licença', '2018-12-18', 'Operações de Resgate', 6),\
        (107, 'Ricardo Fernandes', 'Tenente', '555-5566', 'Ativo', '2020-09-14', 'Resposta a Emergências', 7),\
        (108, 'Sofia Rodrigues', 'Soldado', '555-7788', 'Ativo', '2022-02-10', 'Certificado em Suporte Básico de Vida', 8),\
        (109, 'Diogo Martins', 'Sargento', '555-9900', 'Ativo', '2019-03-29', 'Suporte Avançado de Vida', 9),\
        (110, 'Sara Moreira', 'Capitão', '555-1111', 'Ativo', '2017-11-20', 'Liderança em Combate a Incêndios', 10),\
        (111, 'Pedro Almeida', 'Soldado', '555-2222', 'Ativo', '2023-01-15', 'Certificado em Suporte Básico de Vida', 1),\
        (112, 'Beatriz Ferreira', 'Tenente', '555-3333', 'Ativo', '2021-04-10', 'Certificado em Resgate Aquático', 2),\
        (113, 'Rui Carvalho', 'Sargento', '555-4444', 'Licença', '2019-08-17', 'Operações de Combate a Incêndios em Florestas', 3),\
        (114, 'Inês Gomes', 'Soldado', '555-5555', 'Ativo', '2022-05-01', 'Certificado em Suporte Básico de Vida', 4),\
        (115, 'Tiago Sousa', 'Capitão', '555-6666', 'Ativo', '2018-03-12', 'Combate a Incêndios Urbanos', 5),\
        (116, 'Patrícia Mendes', 'Tenente', '555-7777', 'Ativo', '2021-10-19', 'Certificação em Primeiros Socorros', 6),\
        (117, 'Hugo Nogueira', 'Sargento', '555-8888', 'Licença', '2020-12-09', 'Suporte Básico de Vida', 7),\
        (118, 'Catarina Costa', 'Soldado', '555-9999', 'Ativo', '2023-07-15', 'Certificado em Suporte Básico de Vida', 8),\
        (119, 'Vítor Figueiredo', 'Tenente', '555-0000', 'Ativo', '2019-09-25', 'Resgate em Altura', 9),\
        (120, 'Teresa Fonseca', 'Capitão', '555-1235', 'Ativo', '2016-11-11', 'Liderança em Operações de Emergência', 10);\
        \
        INSERT INTO fire_vehicle_assignment (code_SGIF, registration_plate, allocation_date) VALUES  \
        ('DM2111', 'ABC123', '2024-01-15'),\
        ('BL4112', 'XYZ789', '2024-02-05'),\
        ('DM3111', 'HEL456', '2024-03-10'),\
        ('BL2111', 'AMB321', '2024-04-20'),\
        ('DM2113', 'FTK102', '2024-04-10'),\
        ('BL3116', 'WTK303', '2024-05-15'),\
        ('BL2112', 'HEL789', '2024-06-18'),\
        ('BL4115', 'AMB654', '2024-07-11'),\
        ('RO2116', 'FTK201', '2024-08-22'),\
        ('BL3118', 'WTK808', '2024-09-05'); \
        INSERT INTO fire_firefighter_assignment (code_SGIF, firefighter_code) VALUES \
        ('DM2111', 101), \
        ('BL4112', 102),\
        ('DM3111', 103),\
        ('BL2111', 104),\
        ('DM2113', 105),\
        ('BL3116', 106),\
        ('BL2112', 107), \
        ('BL4115', 108), \
        ('RO2116', 109), \
        ('BL3118', 110);\
        \
        INSERT INTO firetruck (id, water_capacity, pump_capacity, hose_length) VALUES \
        (1, 5000, 1500, 30), \
        (5, 4500, 1400, 25); \
        \
        INSERT INTO watertank (id, water_capacity, pump_capacity, trayler_type) VALUES \
        (2, 10000, 2000, 'Large'), \
        (6, 12000, 2200, 'Medium'); \
        \
         INSERT INTO helicopter (id, water_capacity, max_altitude, flight_range) VALUES \
        (3, 1000, 3000, 500), \
        (7, 800, 2800, 450); \
        \
        INSERT INTO ambulance (id, medical_equipment) VALUES \
        (4, 'Defibrillator, Oxygen Tank, First Aid Kit'), \
        (8, 'Advanced First Aid Kit, Ventilator, Stretcher');"

cur.execute(query)
conn.commit()

# Fechar cursor e conexão
cur.close()
conn.close()

print("Data insertion completed.")
