  CREATE TABLE cause (
    cod INTEGER PRIMARY KEY CHECK(cod > 0),
    type VARCHAR NOT NULL,
    grp VARCHAR NOT NULL,
    description VARCHAR NOT NULL
  );

  CREATE TABLE parish (
    code_DTCCFR INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    municipality VARCHAR NOT NULL,
    district VARCHAR NOT NULL
  );

  CREATE TABLE fires (
    code_SGIF VARCHAR PRIMARY KEY,
    date_time TIMESTAMP NOT NULL,
    total_area_ha FLOAT NOT NULL,
    TIMESTAMP_alert TIMESTAMP,
    TIMESTAMP_first_intervention TIMESTAMP,
    TIMESTAMP_extinction TIMESTAMP,
    latitude FLOAT,
    longitude FLOAT,
    DSR FLOAT,
    FWI FLOAT,
    ISI FLOAT,
    DC FLOAT,
    DMC FLOAT,
    FFMC FLOAT,
    BUI FLOAT,
    alert_source VARCHAR,
    cod INT REFERENCES cause(cod) ON DELETE CASCADE,
    code_DTCCFR BIGINT REFERENCES parish(code_DTCCFR) ON DELETE CASCADE
  );

  CREATE TABLE model (
    id INTEGER PRIMARY KEY CHECK(id > 0),
    name VARCHAR NOT NULL,
    make VARCHAR NOT NULL
  );

  CREATE TABLE firestations (
    id INTEGER PRIMARY KEY CHECK(id > 0),
    name VARCHAR NOT NULL,
    capacity_vehicles INTEGER NOT NULL,
    capacity_firefighters INTEGER NOT NULL,
    address VARCHAR NOT NULL,
    code_DTCCFR INTEGER REFERENCES parish(code_DTCCFR) ON DELETE CASCADE
  );

  CREATE TABLE vehicles (
    registration_plate VARCHAR PRIMARY KEY,
    status VARCHAR NOT NULL,
    last_maintenance_date DATE,
    capacity INTEGER NOT NULL,
    model_id INTEGER NOT NULL REFERENCES model(id) ON DELETE CASCADE,
    firestation_id INTEGER REFERENCES firestations(id) ON DELETE CASCADE
  );

  CREATE TABLE firefighters (
    code INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    rank VARCHAR NOT NULL,
    contact VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    starting_date DATE NOT NULL,
    certifications VARCHAR,
    firestation_id INTEGER REFERENCES firestations(id) ON DELETE CASCADE
  );

  CREATE TABLE fire_vehicle_assignment (
    code_SGIF VARCHAR,
    registration_plate VARCHAR,
    allocation_date DATE NOT NULL,
    PRIMARY KEY (code_SGIF, registration_plate),
    FOREIGN KEY (code_SGIF) REFERENCES fires(code_SGIF) ON DELETE CASCADE,
    FOREIGN KEY (registration_plate) REFERENCES vehicles(registration_plate) ON DELETE CASCADE
  );

  CREATE TABLE fire_firefighter_assignment (
    code_SGIF VARCHAR,
    firefighter_code INTEGER,
    PRIMARY KEY (code_SGIF, firefighter_code),
    FOREIGN KEY (code_SGIF) REFERENCES fires(code_SGIF) ON DELETE CASCADE,
    FOREIGN KEY (firefighter_code) REFERENCES firefighters(code) ON DELETE CASCADE
  );

  CREATE TABLE firetruck (
    id INTEGER PRIMARY KEY,
    water_capacity FLOAT NOT NULL,
    pump_capacity FLOAT NOT NULL,
    hose_length FLOAT NOT NULL,
    FOREIGN KEY (id) REFERENCES model(id) ON DELETE CASCADE
  );

  CREATE TABLE watertank (
    id INTEGER PRIMARY KEY,
    water_capacity FLOAT NOT NULL,
    pump_capacity FLOAT NOT NULL,
    trayler_type VARCHAR NOT NULL,
    FOREIGN KEY (id) REFERENCES model(id) ON DELETE CASCADE
  );

  CREATE TABLE helicopter (
    id INTEGER PRIMARY KEY,
    water_capacity FLOAT NOT NULL,
    max_altitude INTEGER NOT NULL,
    flight_range INTEGER NOT NULL,
    FOREIGN KEY (id) REFERENCES model(id) ON DELETE CASCADE
  );

  CREATE TABLE ambulance (
    id INTEGER PRIMARY KEY,
    medical_equipment VARCHAR NOT NULL,
    FOREIGN KEY (id) REFERENCES model(id) ON DELETE CASCADE
  );
