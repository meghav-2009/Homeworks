CREATE TABLE animaldimension (
    animalkey INT PRIMARY KEY,
    animal_id VARCHAR, 
    animal_name VARCHAR,
    dob DATE,
    age VARCHAR,
    animal_type VARCHAR,
    breed VARCHAR,
    color VARCHAR,
    repro_status VARCHAR,
    gender VARCHAR
);

CREATE TABLE timedimension (
    timekey INT PRIMARY KEY,
    ts TIMESTAMP,
    month VARCHAR,
    year INT
);

CREATE TABLE outcomedimension (
    outcomekey INT PRIMARY KEY, 
    outcome_type VARCHAR,
    outcome_subtype VARCHAR
);

CREATE TABLE animalfact (
    main_pk SERIAL PRIMARY KEY,
    animalkey INT REFERENCES animaldimension(animalkey),
    outcomekey INT REFERENCES outcomedimension(outcomekey),
    timekey INT REFERENCES timedimension(timekey)
);
