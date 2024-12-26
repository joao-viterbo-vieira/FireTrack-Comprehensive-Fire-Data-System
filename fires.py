import psycopg
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

sns.set_theme(style="whitegrid")
wildfire_colors = ['#ff4e00', '#ff8000', '#ffaa00', '#cc6600', '#993d00']

# Database connection
def get_connection():  
    try:
        conn = psycopg.connect("""
          dbname=up202107689
          user=up202107689
          password=up202107689
          host=dbm.fe.up.pt
          port=5433
          options='-c search_path=fires'
          """)
        return conn
    except Exception as e:
        print("Error connecting to the database:", e)
        return None

# Menu Display
def display_menu():
    print("\n=== Fire Incident Management System ===")
    print("[1] Search Fire Incident")
    print("[2] Search Firefighter")
    print("[3] Add/Update and See Vehicle/Model Information")
    print("[4] Show Top Fire Stations")
    print("[5] Show Fire Incident Statistics")
    print("[6] Visualize Fire Incidents")
    print("[7] Manage Fire Incidents")
    print("[8] Prediction of Fire Area Size Based on DC")
    print("[9] Manage Firefighters and Firestations")
    print("[10] Export Filtered Tables to CSV")
    print("[0] Exit")

# Option 1: Search Fire Incident with Date Range Filtering
def search_fire_incident(conn):
    locality = input("\nIn which locality did the fire occur: ").strip()
    
    # Ask if the user wants to filter by date range
    print("\nWould you like to filter by date range?")
    print("[1] Yes")
    print("[2] No")
    date_filter_choice = input("Choose an option (1-2): ").strip()
    
    start_date = None
    end_date = None
    
    if date_filter_choice == '1':
        while True:
            start_date_str = input("Enter start date (YYYY-MM-DD): ").strip()
            end_date_str = input("Enter end date (YYYY-MM-DD): ").strip()
            try:
                start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
                end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
                if start_date > end_date:
                    print("Start date cannot be later than end date. Please try again.")
                    continue
                break
            except ValueError:
                print("Invalid date format. Please use YYYY-MM-DD.")
    
    try:
        with conn.cursor() as cur:
            if date_filter_choice == '1':
                query = """
                    SELECT code_SGIF, date_time, total_area_ha, 
                           EXTRACT(EPOCH FROM (TIMESTAMP_extinction - TIMESTAMP_alert))/3600 AS response_time_hours
                    FROM fires
                    JOIN parish ON fires.code_DTCCFR = parish.code_DTCCFR
                    WHERE parish.district ILIKE %s
                      AND date_time BETWEEN %s AND %s
                    ORDER BY date_time DESC
                    LIMIT 10;
                """
                cur.execute(query, (f"%{locality}%", start_date, end_date))
            else:
                query = """
                    SELECT code_SGIF, date_time, total_area_ha, 
                           EXTRACT(EPOCH FROM (TIMESTAMP_extinction - TIMESTAMP_alert))/3600 AS response_time_hours
                    FROM fires
                    JOIN parish ON fires.code_DTCCFR = parish.code_DTCCFR
                    WHERE parish.district ILIKE %s
                    ORDER BY date_time DESC
                    LIMIT 10;
                """
                cur.execute(query, (f"%{locality}%",))
            
            results = cur.fetchall()

            if not results:
                print("No fire incidents found in the specified locality and date range.")
                return
            print("\nHere are the first 10 fire incidents in the specified locality and date range:")
            for idx, row in enumerate(results, 1):
                code_SGIF, date_time, total_area, response_time = row
                
                # Formatar date_time
                if date_time:
                    date_time_str = date_time.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    date_time_str = "N/A"
                
                # Formatar total_area
                if total_area is not None:
                    total_area_str = f"{total_area} ha"
                else:
                    total_area_str = "N/A"
                
                # Formatar response_time
                if response_time is not None:
                    response_time_str = f"{response_time:.2f} hours"
                else:
                    response_time_str = "N/A"
                
                print(f"[{idx}] {date_time_str} [{total_area_str}] [{response_time_str}] (Code: {code_SGIF})")
            while True:
                choice = input("\nSelect the number of the fire incident to see more details, or 0 to go back: ")
                if choice == '0':
                    break
                elif choice.isdigit() and 1 <= int(choice) <= len(results):
                    fire_code = results[int(choice)-1][0]
                    show_fire_details(conn, fire_code)
                else:
                    print("Invalid choice. Please try again.")
    except Exception as e:
        print("Error fetching fire incidents:", e)
        conn.rollback()

# Show Detailed Fire Incident
def show_fire_details(conn, fire_code):
    try:
        with conn.cursor() as cur:
            query = """
                SELECT fires.code_SGIF, fires.date_time, fires.total_area_ha, fires.latitude, fires.longitude,
                       EXTRACT(EPOCH FROM (fires.TIMESTAMP_first_intervention - fires.TIMESTAMP_alert)) / 3600 AS response_time_hours,
                       cause.type, parish.name, parish.municipality, parish.district
                FROM fires
                JOIN cause ON fires.cod = cause.cod
                JOIN parish ON fires.code_DTCCFR = parish.code_DTCCFR
                WHERE fires.code_SGIF = %s;
            """
            cur.execute(query, (fire_code,))
            fire = cur.fetchone()
            if fire:
                (code_SGIF, date_time, total_area, latitude, longitude,
                 response_time, cause_type, parish_name, municipality, district) = fire
                
                print("\n--- Fire Incident Details ---")
                
                # Formatar Code
                code_SGIF_str = code_SGIF if code_SGIF else "N/A"
                print(f"Code: {code_SGIF_str}")
                
                # Formatar Date & Time
                if date_time:
                    date_time_str = date_time.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    date_time_str = "N/A"
                print(f"Date & Time: {date_time_str}")
                
                # Formatar Total Area
                if total_area is not None:
                    total_area_str = f"{total_area} ha"
                else:
                    total_area_str = "N/A"
                print(f"Total Area: {total_area_str}")
                
                # Formatar Localização
                if parish_name and municipality and district:
                    location_str = f"{parish_name}, {municipality}, {district}"
                else:
                    location_str = "N/A"
                print(f"Location: {location_str}")
                
                # Formatar Coordenadas
                if latitude is not None and longitude is not None:
                    coordinates_str = f"({latitude}, {longitude})"
                else:
                    coordinates_str = "N/A"
                print(f"Coordinates: {coordinates_str}")
                
                # Formatar Response Time
                if response_time is not None:
                    response_time_str = f"{response_time:.2f} hours"
                else:
                    response_time_str = "N/A"
                print(f"Response Time: {response_time_str}")
                
                # Formatar Cause Type
                cause_type_str = cause_type if cause_type else "N/A"
                print(f"Cause Type: {cause_type_str}")
                
                print("------------------------------")
            else:
                print("Fire incident not found.")
    except Exception as e:
        print("Error fetching fire details:", e)
        conn.rollback()

# Option 2: Search Firefighter
def search_firefighter(conn):
    search_term = input("\nEnter Firefighter Name or Code: ").strip()
    try:
        with conn.cursor() as cur:
            if search_term.isdigit():
                query = """
                    SELECT firefighters.code, firefighters.name, firefighters.rank, firefighters.contact, firefighters.status,
                           firefighters.starting_date, firefighters.certifications, firestations.name AS firestation_name
                    FROM firefighters
                    LEFT JOIN firestations ON firefighters.firestation_id = firestations.id
                    WHERE firefighters.code = %s;
                """
                cur.execute(query, (int(search_term),))
            else:
                query = """
                    SELECT firefighters.code, firefighters.name, firefighters.rank, firefighters.contact, firefighters.status,
                           firefighters.starting_date, firefighters.certifications, firestations.name AS firestation_name
                    FROM firefighters
                    LEFT JOIN firestations ON firefighters.firestation_id = firestations.id
                    WHERE firefighters.name ILIKE %s;
                """
                cur.execute(query, (f"%{search_term}%",))
            results = cur.fetchall()
            if not results:
                print("No firefighters found with that name or code.")
                return
            print("\n--- Firefighter(s) Found ---")
            for row in results:
                code, name, rank, contact, status, starting_date, certifications, firestation = row
                print(f"Code: {code}")
                print(f"Name: {name}")
                print(f"Rank: {rank}")
                print(f"Contact: {contact}")
                print(f"Status: {status}")
                print(f"Starting Date: {starting_date}")
                print(f"Certifications: {certifications}")
                print(f"Firestation: {firestation}")
                print("------------------------------")
    except Exception as e:
        print("Error searching firefighter:", e)
        conn.rollback()  # Reset transaction state for the connection

# Option 3: Add/Update Vehicle Information
def add_update_vehicle(conn):
    while True:
        print("\n--- Add/Update Vehicle Information ---")
        print("[1] Add New Vehicle")
        print("[2] Update Existing Vehicle")
        print("[3] Add New Model")
        print("[4] Update Existing Model")
        print("[5] See Tables")
        print("[6] Back to Main Menu")
        choice = input("Choose an option (1-6): ")
        if choice == '1':
            add_new_vehicle(conn)
        elif choice == '2':
            update_existing_vehicle(conn)
        elif choice == '3':
            add_new_model(conn)
        elif choice == '4':
            update_existing_model(conn)
        elif choice == '5':
            see_tables(conn)
        elif choice == '6':
            break
        else:
            print("Invalid choice.")

def see_tables(conn):
    while True:
        print("\n--- See Tables ---")
        print("[1] View Models")
        print("[2] View Ambulances")
        print("[3] View Firetrucks")
        print("[4] View Helicopters")
        print("[5] View Watertanks")
        print("[6] View Vehicles")
        print("[7] Back to Add/Update Vehicle Information")
        choice = input("Choose an option (1-7): ")
        if choice == '1':
            view_models(conn)
        elif choice == '2':
            view_subtypes(conn, 'ambulance')
        elif choice == '3':
            view_subtypes(conn, 'firetruck')
        elif choice == '4':
            view_subtypes(conn, 'helicopter')
        elif choice == '5':
            view_subtypes(conn, 'watertank')
        elif choice == '6':
            view_vehicles(conn)
        elif choice == '7':
            break
        else:
            print("Invalid choice.")

def add_new_vehicle(conn):
    try:
        registration_plate = input("Enter Registration Plate: ").strip()
        status = input("Enter Status: ").strip()
        last_maintenance_date = input("Enter Last Maintenance Date (YYYY-MM-DD) or leave blank: ").strip()
        capacity = int(input("Enter Capacity: ").strip())
        model_id = int(input("Enter Model ID (ex: 1-8): ").strip())
        firestation_id = input("Enter Firestation ID or leave blank: ").strip()
        firestation_id = int(firestation_id) if firestation_id else None

        with conn.cursor() as cur:
            # Check if model_id exists
            cur.execute("SELECT id FROM model WHERE id = %s;", (model_id,))
            if not cur.fetchone():
                print("Model ID does not exist. Please add the model first.")
                return

            # Check if registration_plate already exists
            cur.execute("SELECT registration_plate FROM vehicles WHERE registration_plate = %s;", (registration_plate,))
            if cur.fetchone():
                print("Registration plate already exists. Please use a different plate.")
                return

            query = """
                INSERT INTO vehicles (registration_plate, status, last_maintenance_date, capacity, model_id, firestation_id)
                VALUES (%s, %s, %s, %s, %s, %s);
            """
            cur.execute(query, (
                registration_plate, status,
                last_maintenance_date if last_maintenance_date else None,
                capacity, model_id,
                firestation_id
            ))
            conn.commit()
            print("Vehicle added successfully.")
    except ValueError:
        print("Error: Capacity and IDs must be numbers.")
    except Exception as e:
        conn.rollback()
        print("Error adding vehicle:", e)

def update_existing_vehicle(conn):
    try:
        registration_plate = input("Enter Registration Plate of the vehicle to update: ").strip()
        print("Enter new values (leave blank to keep current value):")
        status = input("Enter New Status: ").strip()
        last_maintenance_date = input("Enter New Last Maintenance Date (YYYY-MM-DD): ").strip()
        capacity = input("Enter New Capacity: ").strip()
        model_id = input("Enter New Model ID (ex: 1-8): ").strip()
        firestation_id = input("Enter New Firestation ID: ").strip()

        with conn.cursor() as cur:
            # Fetch current values
            cur.execute("SELECT * FROM vehicles WHERE registration_plate = %s;", (registration_plate,))
            vehicle = cur.fetchone()
            if not vehicle:
                print("Vehicle not found.")
                return

            # Prepare new values
            new_status = status if status else vehicle[1]
            new_last_maintenance_date = last_maintenance_date if last_maintenance_date else vehicle[2]
            new_capacity = int(capacity) if capacity else vehicle[3]
            new_model_id = int(model_id) if model_id else vehicle[4]
            new_firestation_id = int(firestation_id) if firestation_id else vehicle[5]

            # Check if the new model_id exists
            cur.execute("SELECT id FROM model WHERE id = %s;", (new_model_id,))
            if not cur.fetchone():
                print("New Model ID does not exist. Please add the model first.")
                return

            # Update the vehicle
            update_query = """
                UPDATE vehicles
                SET status = %s,
                    last_maintenance_date = %s,
                    capacity = %s,
                    model_id = %s,
                    firestation_id = %s
                WHERE registration_plate = %s;
            """
            cur.execute(update_query, (
                new_status, new_last_maintenance_date,
                new_capacity, new_model_id,
                new_firestation_id,
                registration_plate
            ))
            conn.commit()
            print("Vehicle updated successfully.")
    except ValueError:
        print("Error: Capacity and IDs must be numbers.")
    except Exception as e:
        conn.rollback()
        print("Error updating vehicle:", e)

def add_new_model(conn):
    try:
        print("\n--- Add New Model ---")
        model_id = int(input("Enter Model ID (ex: 9): ").strip())
        name = input("Enter Model Name: ").strip()
        make = input("Enter Model Make: ").strip()
        model_type = input("Enter Model Type (ambulance/firetruck/helicopter/watertank): ").strip().lower()

        with conn.cursor() as cur:
            # Check if model_id already exists
            cur.execute("SELECT id FROM model WHERE id = %s;", (model_id,))
            if cur.fetchone():
                print("Model ID already exists. Please choose a different ID.")
                return

            # Insert into model table
            insert_model_query = """
                INSERT INTO model (id, name, make)
                VALUES (%s, %s, %s);
            """
            cur.execute(insert_model_query, (model_id, name, make))

            # Insert into specific subtype table
            if model_type == 'ambulance':
                medical_equipment = input("Enter Medical Equipment: ").strip()
                insert_subtype_query = """
                    INSERT INTO ambulance (id, medical_equipment)
                    VALUES (%s, %s);
                """
                cur.execute(insert_subtype_query, (model_id, medical_equipment))
            elif model_type == 'firetruck':
                water_capacity = float(input("Enter Water Capacity (liters): ").strip())
                pump_capacity = float(input("Enter Pump Capacity (liters per minute): ").strip())
                hose_length = float(input("Enter Hose Length (meters): ").strip())
                insert_subtype_query = """
                    INSERT INTO firetruck (id, water_capacity, pump_capacity, hose_length)
                    VALUES (%s, %s, %s, %s);
                """
                cur.execute(insert_subtype_query, (model_id, water_capacity, pump_capacity, hose_length))
            elif model_type == 'helicopter':
                water_capacity = float(input("Enter Water Capacity (liters): ").strip())
                max_altitude = int(input("Enter Maximum Altitude (meters): ").strip())
                flight_range = int(input("Enter Flight Range (kilometers): ").strip())
                insert_subtype_query = """
                    INSERT INTO helicopter (id, water_capacity, max_altitude, flight_range)
                    VALUES (%s, %s, %s, %s);
                """
                cur.execute(insert_subtype_query, (model_id, water_capacity, max_altitude, flight_range))
            elif model_type == 'watertank':
                water_capacity = float(input("Enter Water Capacity (liters): ").strip())
                pump_capacity = float(input("Enter Pump Capacity (liters per minute): ").strip())
                trayler_type = input("Enter Trailer Type: ").strip()
                insert_subtype_query = """
                    INSERT INTO watertank (id, water_capacity, pump_capacity, trayler_type)
                    VALUES (%s, %s, %s, %s);
                """
                cur.execute(insert_subtype_query, (model_id, water_capacity, pump_capacity, trayler_type))
            else:
                print("Unknown model type. Only ambulance, firetruck, helicopter, and watertank are supported.")
                conn.rollback()
                return

            conn.commit()
            print("Model added successfully.")
    except ValueError:
        print("Error: IDs and capacities must be numbers.")
    except Exception as e:
        conn.rollback()
        print("Error adding model:", e)

def update_existing_model(conn):
    try:
        print("\n--- Update Existing Model ---")
        model_id = int(input("Enter Model ID to update: ").strip())

        with conn.cursor() as cur:
            # Check if model exists
            cur.execute("SELECT * FROM model WHERE id = %s;", (model_id,))
            model = cur.fetchone()
            if not model:
                print("Model not found.")
                return

            name = input(f"Enter New Model Name (current: {model[1]}) or leave blank: ").strip()
            make = input(f"Enter New Model Make (current: {model[2]}) or leave blank: ").strip()

            # Prepare new values
            new_name = name if name else model[1]
            new_make = make if make else model[2]

            # Update model table
            update_model_query = """
                UPDATE model
                SET name = %s,
                    make = %s
                WHERE id = %s;
            """
            cur.execute(update_model_query, (new_name, new_make, model_id))

            # Determine the current subtype
            subtype = None
            subtype_tables = ['ambulance', 'firetruck', 'helicopter', 'watertank']
            for table in subtype_tables:
                cur.execute(f"SELECT * FROM {table} WHERE id = %s;", (model_id,))
                if cur.fetchone():
                    subtype = table
                    break

            if not subtype:
                print("Subtype information not found for this model.")
                conn.rollback()
                return

            # Update subtype-specific information
            if subtype == 'ambulance':
                cur.execute("SELECT medical_equipment FROM ambulance WHERE id = %s;", (model_id,))
                ambulance = cur.fetchone()
                current_medical_equipment = ambulance[0]

                medical_equipment = input(f"Enter New Medical Equipment (current: {current_medical_equipment}) or leave blank: ").strip()
                if medical_equipment:
                    update_subtype_query = """
                        UPDATE ambulance
                        SET medical_equipment = %s
                        WHERE id = %s;
                    """
                    cur.execute(update_subtype_query, (medical_equipment, model_id))
            elif subtype == 'firetruck':
                cur.execute("SELECT water_capacity, pump_capacity, hose_length FROM firetruck WHERE id = %s;", (model_id,))
                firetruck = cur.fetchone()
                current_water_capacity, current_pump_capacity, current_hose_length = firetruck

                print("Enter new values for Firetruck (leave blank to keep current values):")
                water_capacity = input(f"Water Capacity (current: {current_water_capacity} liters): ").strip()
                pump_capacity = input(f"Pump Capacity (current: {current_pump_capacity} liters per minute): ").strip()
                hose_length = input(f"Hose Length (current: {current_hose_length} meters): ").strip()

                new_water_capacity = float(water_capacity) if water_capacity else current_water_capacity
                new_pump_capacity = float(pump_capacity) if pump_capacity else current_pump_capacity
                new_hose_length = float(hose_length) if hose_length else current_hose_length

                update_subtype_query = """
                    UPDATE firetruck
                    SET water_capacity = %s,
                        pump_capacity = %s,
                        hose_length = %s
                    WHERE id = %s;
                """
                cur.execute(update_subtype_query, (new_water_capacity, new_pump_capacity, new_hose_length, model_id))
            elif subtype == 'helicopter':
                cur.execute("SELECT water_capacity, max_altitude, flight_range FROM helicopter WHERE id = %s;", (model_id,))
                helicopter = cur.fetchone()
                current_water_capacity, current_max_altitude, current_flight_range = helicopter

                print("Enter new values for Helicopter (leave blank to keep current values):")
                water_capacity = input(f"Water Capacity (current: {current_water_capacity} liters): ").strip()
                max_altitude = input(f"Maximum Altitude (current: {current_max_altitude} meters): ").strip()
                flight_range = input(f"Flight Range (current: {current_flight_range} kilometers): ").strip()

                new_water_capacity = float(water_capacity) if water_capacity else current_water_capacity
                new_max_altitude = int(max_altitude) if max_altitude else current_max_altitude
                new_flight_range = int(flight_range) if flight_range else current_flight_range

                update_subtype_query = """
                    UPDATE helicopter
                    SET water_capacity = %s,
                        max_altitude = %s,
                        flight_range = %s
                    WHERE id = %s;
                """
                cur.execute(update_subtype_query, (new_water_capacity, new_max_altitude, new_flight_range, model_id))
            elif subtype == 'watertank':
                cur.execute("SELECT water_capacity, pump_capacity, trayler_type FROM watertank WHERE id = %s;", (model_id,))
                watertank = cur.fetchone()
                current_water_capacity, current_pump_capacity, current_trayler_type = watertank

                print("Enter new values for Watertank (leave blank to keep current values):")
                water_capacity = input(f"Water Capacity (current: {current_water_capacity} liters): ").strip()
                pump_capacity = input(f"Pump Capacity (current: {current_pump_capacity} liters per minute): ").strip()
                trayler_type = input(f"Trailer Type (current: {current_trayler_type}): ").strip()

                new_water_capacity = float(water_capacity) if water_capacity else current_water_capacity
                new_pump_capacity = float(pump_capacity) if pump_capacity else current_pump_capacity
                new_trayler_type = trayler_type if trayler_type else current_trayler_type

                update_subtype_query = """
                    UPDATE watertank
                    SET water_capacity = %s,
                        pump_capacity = %s,
                        trayler_type = %s
                    WHERE id = %s;
                """
                cur.execute(update_subtype_query, (new_water_capacity, new_pump_capacity, new_trayler_type, model_id))

            conn.commit()
            print("Model updated successfully.")
    except ValueError:
        print("Error: IDs and capacities must be numbers.")
    except Exception as e:
        conn.rollback()
        print("Error updating model:", e)

def view_models(conn):
    try:
        with conn.cursor() as cur:
            query = "SELECT * FROM model ORDER BY id;"
            cur.execute(query)
            models = cur.fetchall()
            if not models:
                print("No models found.")
                return
            print("\n--- Models ---")
            print("{:<5} {:<20} {:<20}".format("ID", "Name", "Make"))
            for model in models:
                print("{:<5} {:<20} {:<20}".format(model[0], model[1], model[2]))
    except Exception as e:
        print("Error viewing models:", e)

def view_subtypes(conn, subtype):
    try:
        with conn.cursor() as cur:
            if subtype == 'ambulance':
                query = """
                    SELECT m.id, m.name, m.make, a.medical_equipment
                    FROM model m
                    JOIN ambulance a ON m.id = a.id
                    ORDER BY m.id;
                """
                print("\n--- Ambulances ---")
                print("{:<5} {:<20} {:<20} {:<30}".format("ID", "Name", "Make", "Medical Equipment"))
                cur.execute(query)
                records = cur.fetchall()
                for record in records:
                    print("{:<5} {:<20} {:<20} {:<30}".format(record[0], record[1], record[2], record[3]))
            elif subtype == 'firetruck':
                query = """
                    SELECT m.id, m.name, m.make, f.water_capacity, f.pump_capacity, f.hose_length
                    FROM model m
                    JOIN firetruck f ON m.id = f.id
                    ORDER BY m.id;
                """
                print("\n--- Firetrucks ---")
                print("{:<5} {:<20} {:<20} {:<15} {:<15} {:<15}".format(
                    "ID", "Name", "Make", "Water Capacity", "Pump Capacity", "Hose Length"))
                cur.execute(query)
                records = cur.fetchall()
                for record in records:
                    print("{:<5} {:<20} {:<20} {:<15} {:<15} {:<15}".format(
                        record[0], record[1], record[2],
                        record[3], record[4], record[5]
                    ))
            elif subtype == 'helicopter':
                query = """
                    SELECT m.id, m.name, m.make, h.water_capacity, h.max_altitude, h.flight_range
                    FROM model m
                    JOIN helicopter h ON m.id = h.id
                    ORDER BY m.id;
                """
                print("\n--- Helicopters ---")
                print("{:<5} {:<20} {:<20} {:<15} {:<15} {:<15}".format(
                    "ID", "Name", "Make", "Water Capacity", "Max Altitude", "Flight Range"))
                cur.execute(query)
                records = cur.fetchall()
                for record in records:
                    print("{:<5} {:<20} {:<20} {:<15} {:<15} {:<15}".format(
                        record[0], record[1], record[2],
                        record[3], record[4], record[5]
                    ))
            elif subtype == 'watertank':
                query = """
                    SELECT m.id, m.name, m.make, w.water_capacity, w.pump_capacity, w.trayler_type
                    FROM model m
                    JOIN watertank w ON m.id = w.id
                    ORDER BY m.id;
                """
                print("\n--- Watertanks ---")
                print("{:<5} {:<20} {:<20} {:<15} {:<15} {:<15}".format(
                    "ID", "Name", "Make", "Water Capacity", "Pump Capacity", "Trailer Type"))
                cur.execute(query)
                records = cur.fetchall()
                for record in records:
                    print("{:<5} {:<20} {:<20} {:<15} {:<15} {:<15}".format(
                        record[0], record[1], record[2],
                        record[3], record[4], record[5]
                    ))
            else:
                print("Unknown subtype.")
    except Exception as e:
        conn.rollback()
        print(f"Error viewing {subtype}s:", e)

def view_vehicles(conn):
    try:
        with conn.cursor() as cur:
            query = """
                SELECT v.registration_plate, v.status, m.name, m.make,
                    CASE
                        WHEN a.id IS NOT NULL THEN 'Ambulance'
                        WHEN f.id IS NOT NULL THEN 'Firetruck'
                        WHEN h.id IS NOT NULL THEN 'Helicopter'
                        WHEN w.id IS NOT NULL THEN 'Watertank'
                        ELSE 'Unknown'
                    END AS vehicle_type
                FROM vehicles v
                JOIN model m ON v.model_id = m.id
                LEFT JOIN ambulance a ON m.id = a.id
                LEFT JOIN firetruck f ON m.id = f.id
                LEFT JOIN helicopter h ON m.id = h.id
                LEFT JOIN watertank w ON m.id = w.id
                ORDER BY v.registration_plate;
            """
            cur.execute(query)
            vehicles = cur.fetchall()
            if not vehicles:
                print("No vehicles found.")
                return
            print("\n--- Vehicles ---")
            print("{:<15} {:<15} {:<20} {:<20} {:<15}".format("Plate", "Status", "Model Name", "Model Make", "Type"))
            for vehicle in vehicles:
                print("{:<15} {:<15} {:<20} {:<20} {:<15}".format(
                    vehicle[0], vehicle[1], vehicle[2], vehicle[3], vehicle[4]
                ))
    except Exception as e:
        conn.rollback()
        print("Error viewing vehicles:", e)

# Option 4: Show Top Fire Stations
def show_top_fire_stations(conn):
    try:
        with conn.cursor() as cur:
            query = """
                SELECT firestations.name, COUNT(fires.code_SGIF) AS incident_count
                FROM firestations
                JOIN firefighters ON firestations.id = firefighters.firestation_id
                JOIN fire_firefighter_assignment ON firefighters.code = fire_firefighter_assignment.firefighter_code
                JOIN fires ON fire_firefighter_assignment.code_SGIF = fires.code_SGIF
                GROUP BY firestations.name
                ORDER BY incident_count DESC
                LIMIT 5;
            """
            cur.execute(query)
            results = cur.fetchall()
            if not results:
                print("No data available.")
                return
            print("\n--- Top 5 Fire Stations ---")
            for idx, row in enumerate(results, 1):
                name, count = row
                print(f"[{idx}] {name} - {count} incidents")
    except Exception as e:
        print("Error fetching top fire stations:", e)
        conn.rollback()

# Option 5: Show Fire Incident Statistics
def show_fire_incident_statistics(conn):
    try:
        with conn.cursor() as cur:
            # Main query for incident statistics with added counts
            query = """
                SELECT 
                    AVG(EXTRACT(EPOCH FROM (TIMESTAMP_extinction - TIMESTAMP_alert))/3600) AS avg_response_time,
                    AVG(total_area_ha) AS avg_area,
                    COUNT(*) AS total_incidents,
                    (SELECT COUNT(*) FROM vehicles) AS total_vehicles,
                    (SELECT COUNT(*) FROM firefighters) AS total_firefighters,
                    (SELECT COUNT(*) FROM firestations) AS total_firestations,
                    (SELECT COUNT(DISTINCT registration_plate) FROM fire_vehicle_assignment) AS vehicles_assigned,
                    (SELECT COUNT(DISTINCT firefighter_code) FROM fire_firefighter_assignment) AS firefighters_assigned
                FROM fires;
            """
            cur.execute(query)
            stats = cur.fetchone()
            
            # Display results if stats are available
            if stats:
                avg_response_time, avg_area, total_incidents, total_vehicles, total_firefighters, total_firestations, vehicles_assigned, firefighters_assigned = stats
                print("\n--- Fire Incident Statistics ---")
                print(f"Total Incidents: {total_incidents}")
                print(f"Average Response Time: {avg_response_time:.2f} minutes")
                print(f"Average Area Affected: {avg_area:.2f} ha")
                print(f"Total Vehicles: {total_vehicles}")
                print(f"Total Firefighters: {total_firefighters}")
                print(f"Total Fire Stations: {total_firestations}")
                print(f"Vehicles Assigned to Incidents: {vehicles_assigned}")
                print(f"Firefighters Assigned to Incidents: {firefighters_assigned}")
                print("----------------------------------")
            else:
                print("No statistics available.")
    except Exception as e:
        print("Error fetching statistics:", e)
        conn.rollback()

# Option 6: Visualize Fire Incidents
def visualize_fire_incidents(conn):
    print("\n--- Fire Incident Visualizations ---")
    print("[1] Histogram of Fire Incidents by District")
    print("[2] Evolution of Fire Incidents Over Years")
    print("[3] Area vs. Response Time")
    print("[0] Back to Main Menu")
    choice = input("Choose an option: ")
    if choice == '1':
        histogram_by_district(conn)
    elif choice == '2':
        evolution_over_years(conn)
    elif choice == '3':
        area_vs_response_time(conn)
    elif choice == '0':
        return
    else:
        print("Invalid choice.")

def histogram_by_district(conn):
    try:
        query = """
            SELECT parish.district, COUNT(fires.code_SGIF) AS incident_count
            FROM fires
            JOIN parish ON fires.code_DTCCFR = parish.code_DTCCFR
            GROUP BY parish.district
            ORDER BY incident_count DESC;
        """
        df = pd.read_sql(query, conn)
        
        plt.figure(figsize=(12, 7))
        ax = plt.gca() 
        bar_plot = df.plot(kind='bar', x='district', y='incident_count', legend=False, color=wildfire_colors[0], ax=ax)
        
        # Customizing the plot
        plt.xlabel('District', fontsize=14, fontweight='bold', color="#2f2f2f")
        plt.ylabel('Number of Incidents', fontsize=14, fontweight='bold', color="#2f2f2f")
        plt.title('Fire Incidents by District', fontsize=16, fontweight='bold', color="#8b0000")
        plt.xticks(rotation=45, ha='right', fontsize=12, color="#4a4a4a")
        plt.yticks(fontsize=12, color="#4a4a4a")
        plt.grid(axis='y', color='#d4a373', linestyle='--', alpha=0.7)

        plt.tight_layout()
        plt.show()
    except Exception as e:
        print("Error generating histogram:", e)
        conn.rollback()

def evolution_over_years(conn):
    try:
        query = """
            SELECT DATE_PART('year', date_time) AS year, COUNT(code_SGIF) AS incidents
            FROM fires
            GROUP BY year
            ORDER BY year;
        """
        df = pd.read_sql(query, conn)
        
        plt.figure(figsize=(12, 7))
        plt.plot(df['year'], df['incidents'], marker='o', color=wildfire_colors[2], markersize=8, linewidth=2)

        # Customizing the plot
        plt.xlabel('Year', fontsize=14, fontweight='bold', color="#2f2f2f")
        plt.ylabel('Number of Incidents', fontsize=14, fontweight='bold', color="#2f2f2f")
        plt.title('Evolution of Fire Incidents Over Years', fontsize=16, fontweight='bold', color="#8b0000")
        plt.xticks(fontsize=12, color="#4a4a4a")
        plt.yticks(fontsize=12, color="#4a4a4a")
        plt.grid(True, color='#d4a373', linestyle='--', alpha=0.7)

        plt.tight_layout()
        plt.show()
    except Exception as e:
        print("Error generating evolution plot:", e)
        conn.rollback()

def area_vs_response_time(conn):
    try:
        query = """
            SELECT total_area_ha, EXTRACT(EPOCH FROM (TIMESTAMP_extinction - TIMESTAMP_alert))/3600 AS response_time_hours
            FROM fires
            WHERE total_area_ha IS NOT NULL AND TIMESTAMP_extinction IS NOT NULL AND TIMESTAMP_alert IS NOT NULL;
        """
        df = pd.read_sql(query, conn)
        
        plt.figure(figsize=(12, 7))
        plt.scatter(df['total_area_ha'], df['response_time_hours'], alpha=0.5, color=wildfire_colors[3], edgecolor='darkred')

        # Customizing the plot
        plt.xlabel('Total Area (ha)', fontsize=14, fontweight='bold', color="#2f2f2f")
        plt.ylabel('Response Time (hours)', fontsize=14, fontweight='bold', color="#2f2f2f")
        plt.title('Area vs. Response Time', fontsize=16, fontweight='bold', color="#8b0000")
        plt.xticks(fontsize=12, color="#4a4a4a")
        plt.yticks(fontsize=12, color="#4a4a4a")
        plt.grid(True, color='#d4a373', linestyle='--', alpha=0.7)

        plt.tight_layout()
        plt.show()
    except Exception as e:
        print("Error generating scatter plot:", e)
        conn.rollback()

# Option 7: Manage Fire Incidents Submenu
def manage_fire_incidents(conn):
    while True:
        print("\n--- Manage Fire Incidents ---")
        print("[1] Create New Fire Incident and Assignments")
        print("[2] Assign Vehicles to Existing Fire Incident")
        print("[3] Assign Firefighters to Existing Fire Incident")
        #print("[4] Assign Other Resources (Optional)")
        print("[0] Back to Main Menu")
        choice = input("Choose an option: ")

        if choice == '1':
            create_fire_incident(conn)
        elif choice == '2':
            assign_vehicle_to_fire(conn)
        elif choice == '3':
            assign_firefighter_to_fire(conn)
        #elif choice == '4':
            #assign_other_resources(conn)   Implement as needed
        elif choice == '0':
            break
        else:
            print("Invalid choice. Please try again.")

def create_fire_incident(conn):
    try:
        with conn.cursor() as cur:
            print("\n--- Create New Fire Incident ---")
            
            # Collect mandatory fire incident information
            code_SGIF = input("Enter SGIF Code for the Fire Incident: ").strip()
            date_time_str = input("Enter Date and Time of Fire (YYYY-MM-DD HH:MM:SS): ").strip()
            total_area_ha_str = input("Enter Total Area Affected (ha): ").strip()
            
            # Validate mandatory fields
            if not code_SGIF or not date_time_str or not total_area_ha_str:
                print("Error: The fields SGIF Code, Date & Time, and Total Area are mandatory.")
                return
            
            # Convert total area to a float and validate
            try:
                total_area_ha = float(total_area_ha_str)
            except ValueError:
                print("Error: Total Area must be a valid number.")
                return

            # Convert mandatory date and time
            try:
                date_time = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                print("Error: Date and Time must be in the format YYYY-MM-DD HH:MM:SS.")
                return
            
            # Optional timestamps
            timestamp_alert, timestamp_first_intervention, timestamp_extinction = None, None, None

            timestamp_alert_str = input("Enter Alert Timestamp (YYYY-MM-DD HH:MM:SS) or leave blank: ").strip()
            if timestamp_alert_str:
                try:
                    timestamp_alert = datetime.strptime(timestamp_alert_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    print("Error: Alert Timestamp must be in the format YYYY-MM-DD HH:MM:SS.")
                    return

            timestamp_first_intervention_str = input("Enter First Intervention Timestamp (YYYY-MM-DD HH:MM:SS) or leave blank: ").strip()
            if timestamp_first_intervention_str:
                try:
                    timestamp_first_intervention = datetime.strptime(timestamp_first_intervention_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    print("Error: First Intervention Timestamp must be in the format YYYY-MM-DD HH:MM:SS.")
                    return

            timestamp_extinction_str = input("Enter Extinction Timestamp (YYYY-MM-DD HH:MM:SS) or leave blank: ").strip()
            if timestamp_extinction_str:
                try:
                    timestamp_extinction = datetime.strptime(timestamp_extinction_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    print("Error: Extinction Timestamp must be in the format YYYY-MM-DD HH:MM:SS.")
                    return
            
            # Location (optional)
            latitude, longitude = None, None

            latitude_str = input("Enter Latitude of the Fire: ").strip()
            if latitude_str:
                try:
                    latitude = float(latitude_str)
                except ValueError:
                    print("Error: Latitude must be a valid number.")
                    return

            longitude_str = input("Enter Longitude of the Fire: ").strip()
            if longitude_str:
                try:
                    longitude = float(longitude_str)
                except ValueError:
                    print("Error: Longitude must be a valid number.")
                    return

            # SQL to insert data
            sql = """
                INSERT INTO fires (
                    code_SGIF, date_time, total_area_ha, timestamp_alert,
                    timestamp_first_intervention, timestamp_extinction, latitude, longitude
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                code_SGIF, date_time, total_area_ha, timestamp_alert,
                timestamp_first_intervention, timestamp_extinction, latitude, longitude
            )
            cur.execute(sql, values)
            conn.commit()
            print("New fire incident created successfully.")
            
    except Exception as e:
        print(f"An error occurred: {e}")

    # Helper Function: Assign Resources After Creating Fire
    def assign_resources_after_creation(conn, code_SGIF):
        try:
            with conn.cursor() as cur:
                # Assign Vehicles
                while True:
                    assign_vehicle = input("Do you want to assign a vehicle to this fire incident? (y/n): ").strip().lower()
                    if assign_vehicle == 'y':
                        registration_plate = input("Enter Vehicle Registration Plate: ").strip()
                        
                        # Check if the vehicle exists
                        cur.execute("SELECT registration_plate FROM vehicles WHERE registration_plate = %s;", (registration_plate,))
                        if not cur.fetchone():
                            print("Vehicle not found. Please check the registration plate and try again.")
                            continue
                        
                        allocation_date_str = input("Enter Allocation Date (YYYY-MM-DD) or leave blank for today: ").strip()
                        allocation_date = datetime.today().date() if not allocation_date_str else datetime.strptime(allocation_date_str, "%Y-%m-%d").date()
                        
                        # Insert into fire_vehicle_assignment table with ON CONFLICT specifying the columns
                        assign_vehicle_query = """
                            INSERT INTO fire_vehicle_assignment (code_SGIF, registration_plate, allocation_date)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (code_SGIF, registration_plate) DO NOTHING;
                        """
                        cur.execute(assign_vehicle_query, (code_SGIF, registration_plate, allocation_date))
                        conn.commit()
                        print(f"Vehicle {registration_plate} successfully assigned to fire {code_SGIF}.")
                    elif assign_vehicle == 'n':
                        break
                    else:
                        print("Invalid option. Please enter 'y' or 'n'.")

                # Assign Firefighters
                while True:
                    assign_firefighter = input("Do you want to assign a firefighter to this fire incident? (y/n): ").strip().lower()
                    if assign_firefighter == 'y':
                        firefighter_code = input("Enter Firefighter Code: ").strip()
                        
                        # Check if the firefighter exists
                        if not firefighter_code.isdigit():
                            print("Invalid code. It must be numeric.")
                            continue
                        cur.execute("SELECT code FROM firefighters WHERE code = %s;", (int(firefighter_code),))
                        if not cur.fetchone():
                            print("Firefighter not found. Please check the code and try again.")
                            continue
                        
                        # Insert into fire_firefighter_assignment table with ON CONFLICT specifying the columns
                        assign_firefighter_query = """
                            INSERT INTO fire_firefighter_assignment (code_SGIF, firefighter_code)
                            VALUES (%s, %s)
                            ON CONFLICT (code_SGIF, firefighter_code) DO NOTHING;
                        """
                        cur.execute(assign_firefighter_query, (code_SGIF, int(firefighter_code)))
                        conn.commit()
                        print(f"Firefighter {firefighter_code} successfully assigned to fire {code_SGIF}.")
                    elif assign_firefighter == 'n':
                        break
                    else:
                        print("Invalid option. Please enter 'y' or 'n'.")
        except Exception as e:
            print("Error assigning resources:", e)
            conn.rollback()

# Option 7.2: Assign Vehicles to Existing Fire (Alternative for older PostgreSQL versions)
def assign_vehicle_to_fire(conn):
    try:
        with conn.cursor() as cur:
            print("\n--- Assign Vehicle to Existing Fire Incident ---")
            code_SGIF = input("Enter SGIF Code of the Fire Incident (Ex: DM2111): ").strip()
            
            # Verify if the fire incident exists
            cur.execute("SELECT code_SGIF FROM fires WHERE code_SGIF = %s;", (code_SGIF,))
            if not cur.fetchone():
                print("Fire incident not found. Please check the SGIF code and try again.")
                return
            
            registration_plate = input("Enter Vehicle Registration Plate: (Ex: XYZ789, AMB654) ").strip()
            
            # Verify if the vehicle exists
            cur.execute("SELECT registration_plate FROM vehicles WHERE registration_plate = %s;", (registration_plate,))
            if not cur.fetchone():
                print("Vehicle not found. Please check the registration plate and try again.")
                return
            
            # Check if the assignment already exists
            cur.execute("""
                SELECT 1 FROM fire_vehicle_assignment
                WHERE code_SGIF = %s AND registration_plate = %s;
            """, (code_SGIF, registration_plate))
            if cur.fetchone():
                print("This vehicle is already assigned to the specified fire incident.")
                return
            
            allocation_date_str = input("Enter Allocation Date (YYYY-MM-DD) or leave blank for today: ").strip()
            allocation_date = datetime.today().date() if not allocation_date_str else datetime.strptime(allocation_date_str, "%Y-%m-%d").date()
            
            # Insert into fire_vehicle_assignment table
            insert_query = """
                INSERT INTO fire_vehicle_assignment (code_SGIF, registration_plate, allocation_date)
                VALUES (%s, %s, %s);
            """
            cur.execute(insert_query, (code_SGIF, registration_plate, allocation_date))
            conn.commit()
            print(f"Vehicle {registration_plate} successfully assigned to fire {code_SGIF}.")
    except Exception as e:
        print("Error assigning vehicle to fire:", e)
        conn.rollback()

# Option 7.3: Assign Firefighters to Existing Fire
def assign_firefighter_to_fire(conn):
    try:
        with conn.cursor() as cur:
            print("\n--- Assign Firefighter to Existing Fire Incident ---")
            code_SGIF = input("Enter SGIF Code of the Fire Incident (Ex: DM2111): ").strip()
            
            # Verify if the fire incident exists
            cur.execute("SELECT code_SGIF FROM fires WHERE code_SGIF = %s;", (code_SGIF,))
            if not cur.fetchone():
                print("Fire incident not found. Please check the SGIF code and try again.")
                return
            
            firefighter_code = input("Enter Firefighter Code (Ex:101-120): ").strip()
            
            # Verify if the firefighter code is numeric
            if not firefighter_code.isdigit():
                print("Invalid code. It must be numeric.")
                return
            
            firefighter_code_int = int(firefighter_code)
            
            # Verify if the firefighter exists
            cur.execute("SELECT code FROM firefighters WHERE code = %s;", (firefighter_code_int,))
            if not cur.fetchone():
                print("Firefighter not found. Please check the code and try again.")
                return
            
            
            # Insert into fire_firefighter_assignment table with ON CONFLICT specifying the columns
            assign_firefighter_query = """
                INSERT INTO fire_firefighter_assignment (code_SGIF, firefighter_code)
                VALUES (%s, %s);
            """
            cur.execute(assign_firefighter_query, (code_SGIF, firefighter_code_int))
            conn.commit()
            print(f"Firefighter {firefighter_code_int} successfully assigned to fire {code_SGIF}.")
    except Exception as e:
        print("Error assigning firefighter to fire:", e)
        conn.rollback()

# Option 7.4: Assign Other Resources (Optional)
def assign_other_resources(conn):
    try:
        with conn.cursor() as cur:
            print("\n--- Assign Other Resources to Fire Incident ---")
            # You can add other assignments based on your tables and requirements.
            # For example, assigning special equipment, additional vehicles, etc.
            # Since the current database only has two assignment tables, this is left as an example.

            print("No additional assignment options available.")
            # Implement as needed.
    except Exception as e:
        print("Error assigning other resources:", e)
        conn.rollback()

# Option 8: Prediction of Fire Area Size Based on DC
def predict_area_by_dsr(conn):

    try:
        input_dsr_str = input("Input current DC for prediction of fire area size: ").strip()
        input_dsr = float(input_dsr_str)
    except ValueError:
        print("Error: DC must be a valid number.")
        return

    try:
         # Query data from the database
        query = """
            SELECT dc, total_area_ha 
            FROM fires 
            WHERE DC IS NOT NULL AND total_area_ha IS NOT NULL;
        """
        df = pd.read_sql(query, conn)
        
        
        # Ensure the column data is numeric
        df['dc'] = pd.to_numeric(df['dc'], errors='coerce')
        df['total_area_ha'] = pd.to_numeric(df['total_area_ha'], errors='coerce')

        # Drop rows with missing values
        df = df.dropna(subset=['dc', 'total_area_ha'])
        
        if df.empty:
            print("No data available for prediction.")
            return
        
        # Define features and target
        X = df[['dc']]
        y = df['total_area_ha']
        
        # Split data into training and test sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Train the model
        model = LinearRegression()
        model.fit(X_train, y_train)
        
        # Predict the area for the input DSR value
        predicted_area = model.predict([[input_dsr]])
        print(f"Predicted area for DC {input_dsr}: {predicted_area[0]:.2f} ha")
        
        return predicted_area[0]
        
    except Exception as e:
        print("Error in predicting area by DC:", e)
        conn.rollback()

# Option 9: Manage Firefighters and Firestations
def manage_firefighters_firestations(conn):
    while True:
        print("\n--- Manage Firefighters and Firestations ---")
        print("[1] Add New Firefighter")
        print("[2] Update Existing Firefighter")
        print("[3] Add New Firestation")
        print("[4] Update Existing Firestation")
        print("[5] View Firefighters with Firestation Name")
        print("[6] View Firestations")
        print("[7] Back to Main Menu")
        choice = input("Choose an option (1-7): ")

        if choice == '1':
            add_new_firefighter(conn)
        elif choice == '2':
            update_existing_firefighter(conn)
        elif choice == '3':
            add_new_firestation(conn)
        elif choice == '4':
            update_existing_firestation(conn)
        elif choice == '5':
            view_firefighters_with_firestation(conn)
        elif choice == '6':
            view_firestations(conn)
        elif choice == '7':
            break
        else:
            print("Invalid choice. Please try again.")

def add_new_firefighter(conn):
    try:
        with conn.cursor() as cur:
            print("\n--- Add New Firefighter ---")
            code_str = input("Enter Firefighter Code: ").strip()
            if not code_str.isdigit():
                print("Error: Firefighter Code must be a number.")
                return
            code = int(code_str)
            name = input("Enter Name: ").strip()
            rank = input("Enter Rank: ").strip()
            contact = input("Enter Contact: ").strip()
            status = input("Enter Status: ").strip()
            starting_date_str = input("Enter Starting Date (YYYY-MM-DD): ").strip()
            certifications = input("Enter Certifications: ").strip()
            firestation_id_str = input("Enter Firestation ID or leave blank: ").strip()
            firestation_id = int(firestation_id_str) if firestation_id_str.isdigit() else None

            # Check if firefighter code already exists
            cur.execute("SELECT code FROM firefighters WHERE code = %s;", (code,))
            if cur.fetchone():
                print("Firefighter code already exists. Please choose a different code.")
                return

            # Check if firestation_id exists
            if firestation_id:
                cur.execute("SELECT id FROM firestations WHERE id = %s;", (firestation_id,))
                if not cur.fetchone():
                    print("Firestation ID does not exist. Please add the firestation first.")
                    return

            # Convert starting_date_str to date
            try:
                starting_date = datetime.strptime(starting_date_str, "%Y-%m-%d").date()
            except ValueError:
                print("Error: Starting Date must be in the format YYYY-MM-DD.")
                return

            # Insert into firefighters table
            insert_query = """
                INSERT INTO firefighters (code, name, rank, contact, status, starting_date, certifications, firestation_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
            cur.execute(insert_query, (
                code, name, rank, contact, status, starting_date,
                certifications if certifications else None, firestation_id
            ))
            conn.commit()
            print("Firefighter added successfully.")
    except ValueError:
        print("Error: Codes and IDs must be numbers.")
    except Exception as e:
        conn.rollback()
        print("Error adding firefighter:", e)

def update_existing_firefighter(conn):
    try:
        with conn.cursor() as cur:
            print("\n--- Update Existing Firefighter ---")
            code_str = input("Enter Firefighter Code to update: ").strip()
            if not code_str.isdigit():
                print("Error: Firefighter Code must be a number.")
                return
            code = int(code_str)

            # Fetch current values
            cur.execute("SELECT * FROM firefighters WHERE code = %s;", (code,))
            firefighter = cur.fetchone()
            if not firefighter:
                print("Firefighter not found.")
                return

            print("Enter new values (leave blank to keep current value):")
            name = input(f"Name (current: {firefighter[1]}): ").strip()
            rank = input(f"Rank (current: {firefighter[2]}): ").strip()
            contact = input(f"Contact (current: {firefighter[3]}): ").strip()
            status = input(f"Status (current: {firefighter[4]}): ").strip()
            starting_date_str = input(f"Starting Date (current: {firefighter[5]}): ").strip()
            certifications = input(f"Certifications (current: {firefighter[6]}): ").strip()
            firestation_id_str = input(f"Firestation ID (current: {firefighter[7]}): ").strip()

            # Prepare new values
            new_name = name if name else firefighter[1]
            new_rank = rank if rank else firefighter[2]
            new_contact = contact if contact else firefighter[3]
            new_status = status if status else firefighter[4]
            new_starting_date = starting_date_str if starting_date_str else firefighter[5]
            new_certifications = certifications if certifications else firefighter[6]
            new_firestation_id = int(firestation_id_str) if firestation_id_str.isdigit() else firefighter[7]

            # Convert starting_date_str to date
            if starting_date_str:
                try:
                    new_starting_date = datetime.strptime(starting_date_str, "%Y-%m-%d").date()
                except ValueError:
                    print("Error: Starting Date must be in the format YYYY-MM-DD.")
                    return

            # Check if firestation_id exists
            if firestation_id_str:
                cur.execute("SELECT id FROM firestations WHERE id = %s;", (new_firestation_id,))
                if not cur.fetchone():
                    print("Firestation ID does not exist. Please add the firestation first.")
                    return

            # Update firefighters table
            update_query = """
                UPDATE firefighters
                SET name = %s,
                    rank = %s,
                    contact = %s,
                    status = %s,
                    starting_date = %s,
                    certifications = %s,
                    firestation_id = %s
                WHERE code = %s;
            """
            cur.execute(update_query, (
                new_name, new_rank, new_contact, new_status, new_starting_date,
                new_certifications, new_firestation_id, code
            ))
            conn.commit()
            print("Firefighter updated successfully.")
    except ValueError:
        print("Error: Codes and IDs must be numbers.")
    except Exception as e:
        conn.rollback()
        print("Error updating firefighter:", e)

def add_new_firestation(conn):
    try:
        with conn.cursor() as cur:
            print("\n--- Add New Firestation ---")
            id_str = input("Enter Firestation ID: ").strip()
            if not id_str.isdigit():
                print("Error: Firestation ID must be a number.")
                return
            id = int(id_str)
            name = input("Enter Name: ").strip()
            capacity_vehicles_str = input("Enter Capacity for Vehicles: ").strip()
            capacity_firefighters_str = input("Enter Capacity for Firefighters: ").strip()
            address = input("Enter Address: ").strip()
            code_DTCCFR_str = input("Enter Parish Code (code_DTCCFR) or leave blank: ").strip()
            code_DTCCFR = int(code_DTCCFR_str) if code_DTCCFR_str.isdigit() else None

            # Check if firestation ID already exists
            cur.execute("SELECT id FROM firestations WHERE id = %s;", (id,))
            if cur.fetchone():
                print("Firestation ID already exists. Please choose a different ID.")
                return

            # Check if code_DTCCFR exists
            if code_DTCCFR:
                cur.execute("SELECT code_DTCCFR FROM parish WHERE code_DTCCFR = %s;", (code_DTCCFR,))
                if not cur.fetchone():
                    print("Parish code_DTCCFR does not exist. Please add the parish first.")
                    return

            # Convert capacities to integers
            try:
                capacity_vehicles = int(capacity_vehicles_str)
                capacity_firefighters = int(capacity_firefighters_str)
            except ValueError:
                print("Error: Capacities must be numbers.")
                return

            # Insert into firestations table
            insert_query = """
                INSERT INTO firestations (id, name, capacity_vehicles, capacity_firefighters, address, code_DTCCFR)
                VALUES (%s, %s, %s, %s, %s, %s);
            """
            cur.execute(insert_query, (
                id, name, capacity_vehicles, capacity_firefighters, address, code_DTCCFR
            ))
            conn.commit()
            print("Firestation added successfully.")
    except ValueError:
        print("Error: IDs and capacities must be numbers.")
    except Exception as e:
        conn.rollback()
        print("Error adding firestation:", e)

def update_existing_firestation(conn):
    try:
        with conn.cursor() as cur:
            print("\n--- Update Existing Firestation ---")
            id_str = input("Enter Firestation ID to update: ").strip()
            if not id_str.isdigit():
                print("Error: Firestation ID must be a number.")
                return
            id = int(id_str)

            # Fetch current values
            cur.execute("SELECT * FROM firestations WHERE id = %s;", (id,))
            firestation = cur.fetchone()
            if not firestation:
                print("Firestation not found.")
                return

            print("Enter new values (leave blank to keep current value):")
            name = input(f"Name (current: {firestation[1]}): ").strip()
            capacity_vehicles_str = input(f"Capacity for Vehicles (current: {firestation[2]}): ").strip()
            capacity_firefighters_str = input(f"Capacity for Firefighters (current: {firestation[3]}): ").strip()
            address = input(f"Address (current: {firestation[4]}): ").strip()
            code_DTCCFR_str = input(f"Parish Code (current: {firestation[5]}): ").strip()

            # Prepare new values
            new_name = name if name else firestation[1]
            new_capacity_vehicles = int(capacity_vehicles_str) if capacity_vehicles_str.isdigit() else firestation[2]
            new_capacity_firefighters = int(capacity_firefighters_str) if capacity_firefighters_str.isdigit() else firestation[3]
            new_address = address if address else firestation[4]
            new_code_DTCCFR = int(code_DTCCFR_str) if code_DTCCFR_str.isdigit() else firestation[5]

            # Check if code_DTCCFR exists
            if code_DTCCFR_str:
                cur.execute("SELECT code_DTCCFR FROM parish WHERE code_DTCCFR = %s;", (new_code_DTCCFR,))
                if not cur.fetchone():
                    print("Parish code_DTCCFR does not exist. Please add the parish first.")
                    return

            # Update firestations table
            update_query = """
                UPDATE firestations
                SET name = %s,
                    capacity_vehicles = %s,
                    capacity_firefighters = %s,
                    address = %s,
                    code_DTCCFR = %s
                WHERE id = %s;
            """
            cur.execute(update_query, (
                new_name, new_capacity_vehicles, new_capacity_firefighters,
                new_address, new_code_DTCCFR, id
            ))
            conn.commit()
            print("Firestation updated successfully.")
    except ValueError:
        print("Error: IDs and capacities must be numbers.")
    except Exception as e:
        conn.rollback()
        print("Error updating firestation:", e)

def view_firefighters_with_firestation(conn):
    try:
        with conn.cursor() as cur:
            query = """
                SELECT f.code, f.name, f.rank, f.contact, f.status, f.starting_date, f.certifications, fs.name AS firestation_name
                FROM firefighters f
                LEFT JOIN firestations fs ON f.firestation_id = fs.id
                ORDER BY f.code;
            """
            cur.execute(query)
            firefighters = cur.fetchall()
            if not firefighters:
                print("No firefighters found.")
                return
            print("\n--- Firefighters ---")
            print("{:<5} {:<20} {:<10} {:<15} {:<10} {:<12} {:<20} {:<20}".format(
                "Code", "Name", "Rank", "Contact", "Status", "Start Date", "Certifications", "Firestation"
            ))
            for ff in firefighters:
                print("{:<5} {:<20} {:<10} {:<15} {:<10} {:<12} {:<20} {:<20}".format(
                    ff[0], ff[1], ff[2], ff[3], ff[4], ff[5], ff[6], ff[7]
                ))
    except Exception as e:
        print("Error viewing firefighters:", e)

def view_firestations(conn):
    try:
        with conn.cursor() as cur:
            query = """
                SELECT id, name, capacity_vehicles, capacity_firefighters, address, code_DTCCFR
                FROM firestations
                ORDER BY id;
            """
            cur.execute(query)
            firestations = cur.fetchall()
            if not firestations:
                print("No firestations found.")
                return
            print("\n--- Firestations ---")
            print("{:<5} {:<20} {:<10} {:<15} {:<30} {:<15}".format(
                "ID", "Name", "Veh Cap", "FF Cap", "Address", "Parish Code"
            ))
            for fs in firestations:
                print("{:<5} {:<20} {:<10} {:<15} {:<30} {:<15}".format(
                    fs[0], fs[1], fs[2], fs[3], fs[4], fs[5]
                ))
    except Exception as e:
        print("Error viewing firestations:", e)

# Option 10: Export All Fire Incidents to CSV
def export_fire_incidents_to_csv(conn):
    try:
        print("\n--- Export All Fire Incidents to CSV ---")
        # Ask for date range filtering
        print("Would you like to filter by date range?")
        print("[1] Yes")
        print("[2] No")
        date_filter_choice = input("Choose an option (1-2): ").strip()
        
        start_date = None
        end_date = None
        
        if date_filter_choice == '1':
            while True:
                start_date_str = input("Enter start date (YYYY-MM-DD): ").strip()
                end_date_str = input("Enter end date (YYYY-MM-DD): ").strip()
                try:
                    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
                    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
                    if start_date > end_date:
                        print("Start date cannot be later than end date. Please try again.")
                        continue
                    break
                except ValueError:
                    print("Invalid date format. Please use YYYY-MM-DD.")
        
        # Ask for locality filter
        locality = input("Enter locality to filter by district (leave blank for all): ").strip()

        # Construct the SQL query with joins
        query = """
            SELECT 
                fires.code_SGIF,
                fires.date_time,
                fires.total_area_ha,
                fires.TIMESTAMP_alert,
                fires.TIMESTAMP_first_intervention,
                fires.TIMESTAMP_extinction,
                fires.latitude,
                fires.longitude,
                fires.DSR,
                fires.FWI,
                fires.ISI,
                fires.DC,
                fires.DMC,
                fires.FFMC,
                fires.BUI,
                fires.alert_source,
                cause.type AS cause_type,
                cause.grp AS cause_grp,
                cause.description AS cause_description,
                parish.name AS parish_name,
                parish.municipality,
                parish.district,
                model.name AS model_name,
                model.make AS model_make,
                firestations.name AS firestation_name,
                firestations.capacity_vehicles,
                firestations.capacity_firefighters,
                firestations.address,
                firestations.code_DTCCFR,
                vehicles.registration_plate,
                vehicles.status AS vehicle_status,
                vehicles.last_maintenance_date,
                vehicles.capacity AS vehicle_capacity,
                vehicles.firestation_id AS vehicle_firestation_id,
                firefighters.code AS firefighter_code,
                firefighters.name AS firefighter_name,
                firefighters.rank AS firefighter_rank,
                firefighters.contact,
                firefighters.status AS firefighter_status,
                firefighters.starting_date,
                firefighters.certifications,
                firefighters.firestation_id AS firefighter_firestation_id,
                fire_vehicle_assignment.allocation_date AS vehicle_allocation_date,
                fire_firefighter_assignment.firefighter_code AS assigned_firefighter_code,
                firetruck.water_capacity AS firetruck_water_capacity,
                firetruck.pump_capacity AS firetruck_pump_capacity,
                firetruck.hose_length,
                watertank.water_capacity AS watertank_water_capacity,
                watertank.pump_capacity AS watertank_pump_capacity,
                watertank.trayler_type,
                helicopter.water_capacity AS helicopter_water_capacity,
                helicopter.max_altitude,
                helicopter.flight_range,
                ambulance.medical_equipment
            FROM fires
            LEFT JOIN cause ON fires.cod = cause.cod
            LEFT JOIN parish ON fires.code_DTCCFR = parish.code_DTCCFR
            LEFT JOIN fire_firefighter_assignment ON fires.code_SGIF = fire_firefighter_assignment.code_SGIF
            LEFT JOIN firefighters ON fire_firefighter_assignment.firefighter_code = firefighters.code
            LEFT JOIN fire_vehicle_assignment ON fires.code_SGIF = fire_vehicle_assignment.code_SGIF
            LEFT JOIN vehicles ON fire_vehicle_assignment.registration_plate = vehicles.registration_plate
            LEFT JOIN model ON vehicles.model_id = model.id
            LEFT JOIN firestations ON vehicles.firestation_id = firestations.id
            LEFT JOIN firetruck ON model.id = firetruck.id
            LEFT JOIN watertank ON model.id = watertank.id
            LEFT JOIN helicopter ON model.id = helicopter.id
            LEFT JOIN ambulance ON model.id = ambulance.id
        """

        # Add filters
        filters = []
        params = []
        if locality:
            filters.append("parish.district ILIKE %s")
            params.append(f"%{locality}%")
        if date_filter_choice == '1':
            filters.append("fires.date_time BETWEEN %s AND %s")
            params.extend([start_date, end_date])

        if filters:
            query += " WHERE " + " AND ".join(filters)

        query += " ORDER BY fires.date_time DESC;"

        # Execute the query and fetch data into a DataFrame
        df = pd.read_sql(query, conn, params=params)

        if df.empty:
            print("No fire incidents found with the specified filters.")
            return

        # Ask user where to save the CSV
        csv_filename = input("Enter the filename for the CSV (e.g., fire_incidents.csv): ").strip()
        if not csv_filename.endswith('.csv'):
            csv_filename += '.csv'

        # Export DataFrame to CSV
        try:
            df.to_csv(csv_filename, index=False)
            print(f"Fire incidents exported successfully to {csv_filename}.")
        except Exception as e:
            print("Error exporting to CSV:", e)

    except Exception as e:
        print("Error exporting fire incidents to CSV:", e)
        conn.rollback()

# Main Function
def main():
    conn = get_connection()
    if not conn:
        return
    while True:
        display_menu()
        choice = input("What’s your choice? ")
        if choice == '1':
            search_fire_incident(conn)
        elif choice == '2':
            search_firefighter(conn)
        elif choice == '3':
            add_update_vehicle(conn)
        elif choice == '4':
            show_top_fire_stations(conn)
        elif choice == '5':
            show_fire_incident_statistics(conn)
        elif choice == '6':
            visualize_fire_incidents(conn)
        elif choice == '7':
            manage_fire_incidents(conn) 
        elif choice == '8':
            predict_area_by_dsr(conn)  # Call the prediction function
        elif choice == '9':
            manage_firefighters_firestations(conn)
        elif choice == '10':
            export_fire_incidents_to_csv(conn)
        elif choice == '0':
            print("Thanks for your time. Goodbye!")
            break
        else:
            print("Invalid choice. Please try again.")
    conn.close()

if __name__ == "__main__":
    main()