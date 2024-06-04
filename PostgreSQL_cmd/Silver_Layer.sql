-- Step 1: Create the CountryCodes table if it doesn't exist
CREATE TABLE IF NOT EXISTS silver.CountryCodes (
    M49Code INTEGER PRIMARY KEY,
    CountryOrArea VARCHAR(255),
    ISOAlpha3Code VARCHAR(3)
);

-- Step 2: Create the Commodity table if it doesn't exist
CREATE TABLE IF NOT EXISTS silver.commodity (
    -- Id INTEGER PRIMARY KEY,
    code VARCHAR(255),
    commodity VARCHAR(255)
);

-- Step 3: Create the Transaction table if it doesn't exist
CREATE TABLE IF NOT EXISTS silver.transaction (
    -- Id INTEGER PRIMARY KEY,
    code VARCHAR(255),
    transaction VARCHAR(255)
);

-- Step 4: Modify the Electricity table and add a foreign key constraint
CREATE TABLE IF NOT EXISTS silver.Electricity (
    ID SERIAL PRIMARY KEY,
    freq VARCHAR(255),
    ref_area int,
    commodity VARCHAR(255),
    transaction INTEGER,
    unit_measure VARCHAR(255),
    time_period INTEGER,
    value DOUBLE PRECISION,
    unit_mult INTEGER,
    obs_status VARCHAR(255),
    conversion_factor DOUBLE PRECISION,
	-- FOREIGN KEY (ref_area) REFERENCES dbo.CountryCodes(M49Code) -- Foreign key constraint
);