SELECT *
FROM customer_curated c
INNER JOIN step_trainer_landing s
    ON c.serialnumber = s.serialnumber;