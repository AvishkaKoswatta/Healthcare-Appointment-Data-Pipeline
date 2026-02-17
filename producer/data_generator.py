# import psycopg2
# from faker import Faker
# import random
# import time
# import threading
# from datetime import datetime
# import os
# from dotenv import load_dotenv

# load_dotenv()


# fake = Faker()

# # PostgreSQL connection
# conn = psycopg2.connect(
#     host=os.getenv("POSTGRES_HOST"),
#     port=os.getenv("POSTGRES_PORT"),
#     dbname=os.getenv("POSTGRES_DB"),
#     user=os.getenv("POSTGRES_USER"),
#     password=os.getenv("POSTGRES_PASSWORD"),
# )
# cur = conn.cursor()

# # --- Low-frequency / static tables ---

# NUM_HOSPITALS = 6
# NUM_DOCTORS = 40
# NUM_PATIENTS = 100

# hospital_ids = []
# doctor_ids = []
# patient_ids = []

# # Generate hospitals
# for i in range(1, NUM_HOSPITALS + 1):
#     hospital_id = f"H{i:03}"
#     hospital_name = f"{fake.city()} Hospital"
#     city = fake.city()
#     country = fake.country()
#     bed_capacity = random.randint(50, 500)
#     created_at = datetime.now()
    
#     cur.execute("""
#         INSERT INTO hospital (hospital_id, hospital_name, city, country, bed_capacity, created_at)
#         VALUES (%s, %s, %s, %s, %s, %s)
#         ON CONFLICT (hospital_id) DO NOTHING
#     """, (hospital_id, hospital_name, city, country, bed_capacity, created_at))
    
#     hospital_ids.append(hospital_id)

# # Generate doctors
# for i in range(1, NUM_DOCTORS + 1):
#     doctor_id = f"D{i:03}"
#     hospital_id = random.choice(hospital_ids)
#     doctor_name = fake.name()
#     specialty = random.choice(["Cardiology", "Neurology", "Oncology", "Orthopedics", "Emergency"])
#     experience_years = random.randint(1, 30)
#     created_at = datetime.now()
    
#     cur.execute("""
#         INSERT INTO doctor (doctor_id, hospital_id, doctor_name, specialty, experience_years, created_at)
#         VALUES (%s, %s, %s, %s, %s, %s)
#         ON CONFLICT (doctor_id) DO NOTHING
#     """, (doctor_id, hospital_id, doctor_name, specialty, experience_years, created_at))
    
#     doctor_ids.append(doctor_id)

# # Generate patients
# for i in range(1, NUM_PATIENTS + 1):
#     patient_id = f"P{i:03}"
#     patient_name = fake.name()
#     gender = random.choice(["Male", "Female", "Other"])
#     date_of_birth = fake.date_of_birth(minimum_age=1, maximum_age=90)
#     city = fake.city()
#     created_at = datetime.now()
    
#     cur.execute("""
#         INSERT INTO patient (patient_id, patient_name, gender, date_of_birth, city, created_at)
#         VALUES (%s, %s, %s, %s, %s, %s)
#         ON CONFLICT (patient_id) DO NOTHING
#     """, (patient_id, patient_name, gender, date_of_birth, city, created_at))
    
#     patient_ids.append(patient_id)

# conn.commit()
# print("Static tables populated successfully!")

# # --- High-frequency streaming: appointment_events ---
# streaming = True

# def stream_appointments():
#     global streaming
#     event_counter = 1
#     while streaming:
#         appointment_id = f"A{event_counter:05}"
#         patient_id = random.choice(patient_ids)
#         doctor_id = random.choice(doctor_ids)
#         hospital_id = random.choice(hospital_ids)
#         event_type = random.choice(["BOOKED", "CONFIRMED", "CANCELLED", "COMPLETED"])
#         appointment_time = fake.date_time_between(start_date='-1d', end_date='+30d')
#         event_timestamp = datetime.now()
#         consultation_fee = round(random.uniform(50, 500), 2)
        
#         cur.execute("""
#             INSERT INTO appointment_events (event_id, appointment_id, patient_id, doctor_id, hospital_id, event_type, appointment_time, event_timestamp, consultation_fee)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """, (
#             f"E{event_counter:06}",
#             appointment_id,
#             patient_id,
#             doctor_id,
#             hospital_id,
#             event_type,
#             appointment_time,
#             event_timestamp,
#             consultation_fee
#         ))
#         conn.commit()
#         event_counter += 1
#         time.sleep(random.uniform(0.5, 2))  # 0.1 to 1 second for frequent streaming

# # Start streaming in a separate thread
# stream_thread = threading.Thread(target=stream_appointments)
# stream_thread.start()

# print("Streaming appointments... Press Ctrl+C to stop or call stop_streaming()")

# # Function to stop streaming
# def stop_streaming():
#     global streaming
#     streaming = False
#     stream_thread.join()
#     print("Streaming stopped.")





import psycopg2
from faker import Faker
import random
import time
import threading
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

fake = Faker()

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
)
cur = conn.cursor()

NUM_HOSPITALS = 6
NUM_DOCTORS = 40
NUM_PATIENTS = 100

hospital_ids = []
doctor_ids = []
patient_ids = []

for i in range(1, NUM_HOSPITALS + 1):
    hospital_id = f"H{i:03}"
    hospital_name = f"{fake.city()} Hospital"
    city = fake.city()
    country = fake.country()
    bed_capacity = random.randint(50, 500)
    created_at = datetime.now()

    cur.execute("""
        INSERT INTO hospital (hospital_id, hospital_name, city, country, bed_capacity, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (hospital_id) DO NOTHING
    """, (hospital_id, hospital_name, city, country, bed_capacity, created_at))

    hospital_ids.append(hospital_id)

for i in range(1, NUM_DOCTORS + 1):
    doctor_id = f"D{i:03}"
    hospital_id = random.choice(hospital_ids)
    doctor_name = fake.name()
    specialty = random.choice(["Cardiology", "Neurology", "Oncology", "Orthopedics", "Emergency"])
    experience_years = random.randint(1, 30)
    created_at = datetime.now()

    cur.execute("""
        INSERT INTO doctor (doctor_id, hospital_id, doctor_name, specialty, experience_years, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (doctor_id) DO NOTHING
    """, (doctor_id, hospital_id, doctor_name, specialty, experience_years, created_at))

    doctor_ids.append(doctor_id)


for i in range(1, NUM_PATIENTS + 1):
    patient_id = f"P{i:03}"
    patient_name = fake.name()
    gender = random.choice(["Male", "Female", "Other"])
    date_of_birth = fake.date_of_birth(minimum_age=1, maximum_age=90)
    city = fake.city()
    created_at = datetime.now()

    cur.execute("""
        INSERT INTO patient (patient_id, patient_name, gender, date_of_birth, city, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (patient_id) DO NOTHING
    """, (patient_id, patient_name, gender, date_of_birth, city, created_at))

    patient_ids.append(patient_id)

conn.commit()
print("Static tables populated successfully!")

streaming = True

cur.execute("SELECT MAX(event_id) FROM appointment_events")
last_id = cur.fetchone()[0]

if last_id:
    
    event_counter = int(last_id[1:]) + 1
else:
    event_counter = 1


def stream_appointments():
    global streaming, event_counter
    while streaming:
        appointment_id = f"A{event_counter:05}"
        patient_id = random.choice(patient_ids)
        doctor_id = random.choice(doctor_ids)
        hospital_id = random.choice(hospital_ids)
        event_type = random.choice(["BOOKED", "CONFIRMED", "CANCELLED", "COMPLETED"])
        appointment_time = fake.date_time_between(start_date='-1d', end_date='+30d')
        event_timestamp = datetime.now()
        consultation_fee = round(random.uniform(50, 500), 2)

        cur.execute("""
            INSERT INTO appointment_events (event_id, appointment_id, patient_id, doctor_id, hospital_id, event_type, appointment_time, event_timestamp, consultation_fee)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            f"E{event_counter:06}",
            appointment_id,
            patient_id,
            doctor_id,
            hospital_id,
            event_type,
            appointment_time,
            event_timestamp,
            consultation_fee
        ))
        conn.commit()
        event_counter += 1

       
        time.sleep(random.uniform(0.5, 2))


stream_thread = threading.Thread(target=stream_appointments)
stream_thread.start()

print("Streaming appointments... Press Ctrl+C to stop or call stop_streaming()")


def stop_streaming():
    global streaming
    streaming = False
    stream_thread.join()
    print("Streaming stopped.")
