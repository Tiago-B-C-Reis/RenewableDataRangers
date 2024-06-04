-- Step 1: Create the transaction table if it doesn't exist
CREATE TABLE IF NOT EXISTS gold.transaction (
    code VARCHAR(255) PRIMARY KEY,
    transaction VARCHAR(255)
);

-- Step 2: Create the commodity table if it doesn't exist
CREATE TABLE IF NOT EXISTS gold.commodity (
    code VARCHAR(255) PRIMARY KEY,
    commodity VARCHAR(255)
);

-- Step 3: Create the electricity_complete table if it doesn't exist
CREATE TABLE IF NOT EXISTS gold.electricity_complete (
    ID SERIAL PRIMARY KEY,
    freq VARCHAR(255),
    Country_Name VARCHAR(255),
    Country_Codes VARCHAR(255),
    commodity_code VARCHAR(255),
    commodity_name VARCHAR(255),
    transaction_code INTEGER,
    transaction_name VARCHAR(255),
    unit_measure VARCHAR(255),
    time_period INTEGER,
    value DOUBLE PRECISION,
    obs_status VARCHAR(255)
);

-- Step 4: Create the electricity_per_year table if it doesn't exist
CREATE TABLE IF NOT EXISTS gold.electricity_per_year (
    ID SERIAL PRIMARY KEY,
    time_period INTEGER,
    total_energy_per_year DOUBLE PRECISION,
    max_energy_per_year DOUBLE PRECISION,
    min_energy_per_year DOUBLE PRECISION,
    avg_energy_per_year DOUBLE PRECISION
);

-- Step 5: Create the electricity_per_country table if it doesn't exist
CREATE TABLE IF NOT EXISTS gold.electricity_per_country (
    ID SERIAL PRIMARY KEY,
    Country_Codes VARCHAR(255),
    total_energy_per_country DOUBLE PRECISION,
    max_energy_per_country DOUBLE PRECISION,
    min_energy_per_country DOUBLE PRECISION,
    avg_energy_per_country DOUBLE PRECISION
);

-- Step 6: Create the electricity_per_transaction table if it doesn't exist
CREATE TABLE IF NOT EXISTS gold.electricity_per_commodity (
    ID SERIAL PRIMARY KEY,
    commodity_name VARCHAR(255),
    energy_per_commodity DOUBLE PRECISION,
    max_energy_per_commodity DOUBLE PRECISION,
    min_energy_per_commodity DOUBLE PRECISION,
    avg_energy_per_commodity DOUBLE PRECISION
);


TRUNCATE TABLE gold.commodity;
TRUNCATE TABLE gold.electricity;
TRUNCATE TABLE gold.electricity_complete;
TRUNCATE TABLE gold.electricity_per_commodity;
TRUNCATE TABLE gold.electricity_per_country;
TRUNCATE TABLE gold.electricity_per_year;
TRUNCATE TABLE gold.transaction;