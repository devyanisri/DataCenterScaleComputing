DROP TABLE IF EXISTS animaldim;
CREATE TABLE animaldim(
   animal_dim_key SERIAL PRIMARY KEY,
   animal_id VARCHAR,
   animal_type VARCHAR,
   animal_name VARCHAR,
   dob DATE,
   breed VARCHAR,
   color VARCHAR,
   reprod VARCHAR,
   gender VARCHAR,
   timestmp TIMESTAMP
);

DROP TABLE IF EXISTS timingdim;
CREATE TABLE timingdim(
   time_dim_key SERIAL PRIMARY KEY,
   monthh VARCHAR,
   yearr INT
);

DROP TABLE IF EXISTS outcomedim;
CREATE TABLE outcomedimension(
   outcome_dim_key SERIAL PRIMARY KEY,
   outcome_type VARCHAR,
   outcome_subtype VARCHAR
);

DROP TABLE IF EXISTS outcomesfact;
CREATE TABLE outcomesfact(
   outcomesfact_key SERIAL PRIMARY KEY,
   outcome_dim_key INT REFERENCES outcomedimension(outcome_dim_key),
   animal_dim_key INT REFERENCES animaldimension(animal_dim_key),
   time_dim_key INT REFERENCES timingdimension(time_dim_key)
);

DROP TABLE IF EXISTS dummy_table;
CREATE TABLE dummy_table(
   temp_key SERIAL PRIMARY KEY,
   animal_id VARCHAR,
   animal_type VARCHAR,
   animal_name VARCHAR,
   dob DATE,
   breed VARCHAR,
   color VARCHAR,
   reprod VARCHAR,
   gender VARCHAR,
   timestmp TIMESTAMP,
   month_h VARCHAR,
   year_r INT,
   outcome_type VARCHAR,
   outcome_subtype VARCHAR
);
