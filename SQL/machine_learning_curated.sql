WITH accelerometer_trusted_columns AS (
    SELECT email,
        timestamp,
        x,
        y,
        z
    FROM accelerometer_trusted
)
SELECT *
FROM customer_curated c
INNER JOIN step_trainer_landing s
    ON c.serialnumber = s.serialnumber
INNER JOIN accelerometer_trusted_columns a
    ON s.sensorreadingtime = a.timestamp;