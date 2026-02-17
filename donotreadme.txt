docker exec -it appointment-data-project-postgres-1 psql -U postgres -d healthcare_db

INSERT INTO appointment_events (
    event_id,
    appointment_id,
    patient_id,
    doctor_id,
    hospital_id,
    event_type,
    appointment_time,
    event_timestamp,
    consultation_fee
)
VALUES (
    'E000134',
    'A00134',
;    'P024',
    'D008',
    'H003',
    'CONFIRMED',
    NOW(),
    NOW(),
    320.75
    
);

UPDATE patient
SET city = 'Galle',
    created_at = NOW()
WHERE patient_id = 'P004';

docker exec -it kafka bash

docker compose up -d

docker compose down

sudo service postgresql stop