# from flask import Flask, render_template_string
# import snowflake.connector
# import pandas as pd
# import os

# app = Flask(__name__)

# def get_connection():
#     return snowflake.connector.connect(
#         user=os.getenv("SNOWFLAKE_USER"),
#         password=os.getenv("SNOWFLAKE_PASSWORD"),
#         account=os.getenv("SNOWFLAKE_ACCOUNT"),
#         warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
#         database=os.getenv("SNOWFLAKE_DB"),
#         schema=os.getenv("SNOWFLAKE_SCHEMA"),
#     )

# @app.route("/")
# def dashboard():
#     conn = get_connection()
    
#     # Today total appointments
#     today_appointments = pd.read_sql("""
#         SELECT COUNT(*) AS CNT
#         FROM appointment_events_fact
#         WHERE CAST(appointment_time AS DATE) = CURRENT_DATE()
#     """, conn).iloc[0]["CNT"]

#     # Total patients
#     total_patients = pd.read_sql("""
#         SELECT COUNT(DISTINCT patient_id) AS CNT
#         FROM patient_dim
#     """, conn).iloc[0]["CNT"]

#     # Total doctors
#     total_doctors = pd.read_sql("""
#         SELECT COUNT(DISTINCT doctor_id) AS CNT
#         FROM doctor_dim
#     """, conn).iloc[0]["CNT"]

#     # Total hospitals
#     total_hospitals = pd.read_sql("""
#         SELECT COUNT(DISTINCT hospital_id) AS CNT
#         FROM hospital_dim
#     """, conn).iloc[0]["CNT"]

#     # Active appointments (event_type = 'confirmed')
#     active_appointments = pd.read_sql("""
#         SELECT COUNT(*) AS CNT
#         FROM appointment_events_fact
#         WHERE EVENT_TYPE = 'confirmed'
#         AND CAST(appointment_time AS DATE) = CURRENT_DATE()
#     """, conn).iloc[0]["CNT"]

#     # Recent appointments
#     recent_appointments = pd.read_sql("""
#         SELECT appointment_time,doctor_name, event_type FROM appointment_events_fact 
#                               ORDER BY appointment_time ASC LIMIT 5;
#     """, conn)

#     conn.close()

#     return render_template_string("""
#     <html>
#     <head>
#         <title>Healthcare Live Dashboard</title>
#         <meta http-equiv="refresh" content="10">
#         <style>
#             body { font-family: Arial; padding: 40px; }
#             .card { padding:20px; margin:10px; display:inline-block; 
#                     border-radius:10px; background:#f4f4f4; }
#             table { border-collapse: collapse; margin-top:20px; }
#             th, td { padding:10px; border:1px solid #ccc; text-align:center; }
#         </style>
#     </head>
#     <body>
#         <h1>🏥 Healthcare Appointment Live Dashboard</h1>

#         <div class="card">
#             <h2>Total Patients</h2>
#             <h1>{{total_patients}}</h1>
#         </div>

#         <div class="card">
#             <h2>Total Doctors</h2>
#             <h1>{{total_doctors}}</h1>
#         </div>

#         <div class="card">
#             <h2>Total Hospitals</h2>
#             <h1>{{total_hospitals}}</h1>
#         </div>

#         <div class="card">
#             <h2>Today's Appointments</h2>
#             <h1>{{today_appointments}}</h1>
#         </div>

#         <div class="card">
#             <h2>Active Appointments</h2>
#             <h1>{{active_appointments}}</h1>
#         </div>

#         <h2>Recent Appointments</h2>                                
#         <table>
#             <tr><th>Appointment Time</th><th>Doctor Name</th><th>Event Type</th></tr>
#             {% for row in recent_appointments %}
#                 <tr>
#                     <td>{{row.appointment_time}}</td>
#                     <td>{{row.doctor_name}}</td>
#                     <td>{{row.event_type}}</td>
#                 </tr>
#             {% endfor %}
#         </table>

#         <p>Auto refresh every 10 seconds</p>
#     </body>
#     </html>
#     """,
#     total_patients=total_patients,
#     total_doctors=total_doctors,
#     total_hospitals=total_hospitals,
#     today_appointments=today_appointments,
#     active_appointments=active_appointments,
#     recent_appointments=recent_appointments.to_dict(orient="records")
#     )

# if __name__ == "__main__":
#     app.run(host="0.0.0.0", port=5000, debug=True)


from flask import Flask, render_template, request, redirect, url_for, flash
import snowflake.connector
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "healthcare-secret-key")


# ═══════════════════════════════════════════════════════════════════════
# CONNECTION HELPERS
#   get_snowflake_conn()  →  dashboard reads  (Snowflake *_dim / *_fact)
#   get_pg_conn()         →  all CRUD forms   (PostgreSQL source tables)
# ═══════════════════════════════════════════════════════════════════════

def get_snowflake_conn():
    """Read-only.  Powers the live dashboard from the Snowflake warehouse."""
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


def get_pg_conn():
    """Read/Write.  Powers all management forms from the PostgreSQL source DB."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


# ═══════════════════════════════════════════════════════════════════════
# DASHBOARD  ←  Snowflake
#   Tables: appointment_events_fact, patient_dim, doctor_dim, hospital_dim
# ═══════════════════════════════════════════════════════════════════════

@app.route("/")
def dashboard():
    conn = get_snowflake_conn()

    today_appointments = pd.read_sql("""
        SELECT COUNT(*) AS CNT
        FROM appointment_events_fact
        WHERE CAST(appointment_time AS DATE) = CURRENT_DATE()
    """, conn).iloc[0]["CNT"]

    total_patients = pd.read_sql("""
        SELECT COUNT(DISTINCT patient_id) AS CNT FROM patient_dim
    """, conn).iloc[0]["CNT"]

    total_doctors = pd.read_sql("""
        SELECT COUNT(DISTINCT doctor_id) AS CNT FROM doctor_dim
    """, conn).iloc[0]["CNT"]

    total_hospitals = pd.read_sql("""
        SELECT COUNT(DISTINCT hospital_id) AS CNT FROM hospital_dim
    """, conn).iloc[0]["CNT"]

    active_appointments = pd.read_sql("""
        SELECT COUNT(*) AS CNT
        FROM appointment_events_fact
        WHERE EVENT_TYPE = 'CONFIRMED'
        AND CAST(appointment_time AS DATE) = CURRENT_DATE()
    """, conn).iloc[0]["CNT"]

    recent_appointments = pd.read_sql("""
        SELECT appointment_time, doctor_name, patient_id, event_type
        FROM appointment_events_fact
        ORDER BY appointment_time DESC
        LIMIT 10
    """, conn)
    recent_appointments.columns = [c.lower() for c in recent_appointments.columns]

    specialty_stats = pd.read_sql("""
        SELECT d.specialty, COUNT(*) AS cnt
        FROM appointment_events_fact f
        JOIN doctor_dim d ON f.doctor_id = d.doctor_id
        GROUP BY d.specialty
        ORDER BY cnt DESC
    """, conn)
    specialty_stats.columns = specialty_stats.columns.str.lower()

    conn.close()

    return render_template(
        "dashboard.html",
        total_patients=total_patients,
        total_doctors=total_doctors,
        total_hospitals=total_hospitals,
        today_appointments=today_appointments,
        active_appointments=active_appointments,
        recent_appointments=recent_appointments.to_dict(orient="records"),
        specialty_stats=specialty_stats.to_dict(orient="records"),
    )


# ═══════════════════════════════════════════════════════════════════════
# HOSPITALS CRUD  ←  PostgreSQL  →  table: hospital
# ═══════════════════════════════════════════════════════════════════════

@app.route("/manage/hospitals")
def hospitals():
    conn = get_pg_conn()
    rows = pd.read_sql(
        "SELECT hospital_id, hospital_name, city, country, bed_capacity "
        "FROM hospital ORDER BY hospital_id",
        conn,
    )
    conn.close()
    return render_template("hospitals.html", hospitals=rows.to_dict(orient="records"))


@app.route("/manage/hospitals/add", methods=["POST"])
def add_hospital():
    data = request.form
    hid = data["hospital_id"].strip().upper()
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO hospital (hospital_id, hospital_name, city, country, bed_capacity, created_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (hospital_id) DO NOTHING
        """,
        (hid, data["hospital_name"], data["city"], data["country"], int(data["bed_capacity"])),
    )
    conn.commit()
    cur.close()
    conn.close()
    flash(f"Hospital {hid} added successfully.", "success")
    return redirect(url_for("hospitals"))


@app.route("/manage/hospitals/update", methods=["POST"])
def update_hospital():
    data = request.form
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE hospital
        SET hospital_name=%s, city=%s, country=%s, bed_capacity=%s
        WHERE hospital_id=%s
        """,
        (data["hospital_name"], data["city"], data["country"],
         int(data["bed_capacity"]), data["hospital_id"]),
    )
    conn.commit()
    cur.close()
    conn.close()
    flash(f"Hospital {data['hospital_id']} updated.", "success")
    return redirect(url_for("hospitals"))


@app.route("/manage/hospitals/delete", methods=["POST"])
def delete_hospital():
    hid = request.form["hospital_id"]
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM hospital WHERE hospital_id=%s", (hid,))
    conn.commit()
    cur.close()
    conn.close()
    flash(f"Hospital {hid} deleted.", "warning")
    return redirect(url_for("hospitals"))


# ═══════════════════════════════════════════════════════════════════════
# DOCTORS CRUD  ←  PostgreSQL  →  table: doctor
# ═══════════════════════════════════════════════════════════════════════

@app.route("/manage/doctors")
def doctors():
    conn = get_pg_conn()
    rows = pd.read_sql(
        "SELECT doctor_id, hospital_id, doctor_name, specialty, experience_years "
        "FROM doctor ORDER BY doctor_id",
        conn,
    )
    hosp = pd.read_sql(
        "SELECT hospital_id, hospital_name FROM hospital ORDER BY hospital_id", conn
    )
    conn.close()
    return render_template(
        "doctors.html",
        doctors=rows.to_dict(orient="records"),
        hospitals=hosp.to_dict(orient="records"),
    )


@app.route("/manage/doctors/add", methods=["POST"])
def add_doctor():
    data = request.form
    did = data["doctor_id"].strip().upper()
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO doctor (doctor_id, hospital_id, doctor_name, specialty, experience_years, created_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (doctor_id) DO NOTHING
        """,
        (did, data["hospital_id"], data["doctor_name"],
         data["specialty"], int(data["experience_years"])),
    )
    conn.commit()
    cur.close()
    conn.close()
    flash(f"Doctor {did} added successfully.", "success")
    return redirect(url_for("doctors"))


@app.route("/manage/doctors/update", methods=["POST"])
def update_doctor():
    data = request.form
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE doctor
        SET hospital_id=%s, doctor_name=%s, specialty=%s, experience_years=%s
        WHERE doctor_id=%s
        """,
        (data["hospital_id"], data["doctor_name"],
         data["specialty"], int(data["experience_years"]), data["doctor_id"]),
    )
    conn.commit()
    cur.close()
    conn.close()
    flash(f"Doctor {data['doctor_id']} updated.", "success")
    return redirect(url_for("doctors"))


@app.route("/manage/doctors/delete", methods=["POST"])
def delete_doctor():
    did = request.form["doctor_id"]
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM doctor WHERE doctor_id=%s", (did,))
    conn.commit()
    cur.close()
    conn.close()
    flash(f"Doctor {did} deleted.", "warning")
    return redirect(url_for("doctors"))


# ═══════════════════════════════════════════════════════════════════════
# PATIENTS CRUD  ←  PostgreSQL  →  table: patient
# ═══════════════════════════════════════════════════════════════════════

@app.route("/manage/patients")
def patients():
    conn = get_pg_conn()
    rows = pd.read_sql(
        "SELECT patient_id, patient_name, gender, date_of_birth, city "
        "FROM patient ORDER BY patient_id",
        conn,
    )
    conn.close()
    return render_template("patients.html", patients=rows.to_dict(orient="records"))


@app.route("/manage/patients/add", methods=["POST"])
def add_patient():
    data = request.form
    pid = data["patient_id"].strip().upper()
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO patient (patient_id, patient_name, gender, date_of_birth, city, created_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (patient_id) DO NOTHING
        """,
        (pid, data["patient_name"], data["gender"], data["date_of_birth"], data["city"]),
    )
    conn.commit()
    cur.close()
    conn.close()
    flash(f"Patient {pid} added successfully.", "success")
    return redirect(url_for("patients"))


@app.route("/manage/patients/update", methods=["POST"])
def update_patient():
    data = request.form
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE patient
        SET patient_name=%s, gender=%s, date_of_birth=%s, city=%s
        WHERE patient_id=%s
        """,
        (data["patient_name"], data["gender"],
         data["date_of_birth"], data["city"], data["patient_id"]),
    )
    conn.commit()
    cur.close()
    conn.close()
    flash(f"Patient {data['patient_id']} updated.", "success")
    return redirect(url_for("patients"))


@app.route("/manage/patients/delete", methods=["POST"])
def delete_patient():
    pid = request.form["patient_id"]
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM patient WHERE patient_id=%s", (pid,))
    conn.commit()
    cur.close()
    conn.close()
    flash(f"Patient {pid} deleted.", "warning")
    return redirect(url_for("patients"))


# ═══════════════════════════════════════════════════════════════════════
# APPOINTMENTS CRUD  ←  PostgreSQL  →  table: appointment_events
# ═══════════════════════════════════════════════════════════════════════

@app.route("/manage/appointments")
def appointments():
    conn = get_pg_conn()
    rows = pd.read_sql(
        """
        SELECT event_id, appointment_id, patient_id, doctor_id, hospital_id,
               event_type, appointment_time, event_timestamp, consultation_fee
        FROM appointment_events
        ORDER BY event_timestamp DESC
        LIMIT 100
        """,
        conn,
    )
    patients_list  = pd.read_sql("SELECT patient_id,  patient_name  FROM patient  ORDER BY patient_id",  conn)
    doctors_list   = pd.read_sql("SELECT doctor_id,   doctor_name   FROM doctor   ORDER BY doctor_id",   conn)
    hospitals_list = pd.read_sql("SELECT hospital_id, hospital_name FROM hospital ORDER BY hospital_id", conn)
    conn.close()
    return render_template(
        "appointments.html",
        appointments=rows.to_dict(orient="records"),
        patients=patients_list.to_dict(orient="records"),
        doctors=doctors_list.to_dict(orient="records"),
        hospitals=hospitals_list.to_dict(orient="records"),
    )


@app.route("/manage/appointments/add", methods=["POST"])
def add_appointment():
    data = request.form
    conn = get_pg_conn()
    cur = conn.cursor()

    # Mirror the faker script: derive next ID from current MAX
    cur.execute("SELECT MAX(event_id) FROM appointment_events")
    last_event = cur.fetchone()[0]
    next_num = (int(last_event[1:]) + 1) if last_event else 1
    event_id       = f"E{next_num:06}"
    appointment_id = f"A{next_num:05}"

    cur.execute(
        """
        INSERT INTO appointment_events
            (event_id, appointment_id, patient_id, doctor_id, hospital_id,
             event_type, appointment_time, event_timestamp, consultation_fee)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s)
        """,
        (
            event_id, appointment_id,
            data["patient_id"], data["doctor_id"], data["hospital_id"],
            data["event_type"], data["appointment_time"],
            float(data["consultation_fee"]),
        ),
    )
    conn.commit()
    cur.close()
    conn.close()
    flash(f"Appointment {appointment_id} (event {event_id}) added.", "success")
    return redirect(url_for("appointments"))


@app.route("/manage/appointments/update", methods=["POST"])
def update_appointment():
    data = request.form
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE appointment_events
        SET event_type=%s, appointment_time=%s, consultation_fee=%s
        WHERE event_id=%s
        """,
        (data["event_type"], data["appointment_time"],
         float(data["consultation_fee"]), data["event_id"]),
    )
    conn.commit()
    cur.close()
    conn.close()
    flash(f"Event {data['event_id']} updated.", "success")
    return redirect(url_for("appointments"))


@app.route("/manage/appointments/delete", methods=["POST"])
def delete_appointment():
    eid = request.form["event_id"]
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM appointment_events WHERE event_id=%s", (eid,))
    conn.commit()
    cur.close()
    conn.close()
    flash(f"Event {eid} deleted.", "warning")
    return redirect(url_for("appointments"))


# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)