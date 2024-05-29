-- Step 1: Create the CountryCodes table if it doesn't exist
CREATE TABLE dbo.CountryCodes (
    M49Code INTEGER PRIMARY KEY,
    CountryOrArea VARCHAR(255),
    ISOAlpha3Code VARCHAR(3)
);

-- Step 2: Modify the Electricity table and add a foreign key constraint
CREATE TABLE dbo.Electricity (
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
	FOREIGN KEY (ref_area) REFERENCES dbo.CountryCodes(M49Code) -- Foreign key constraint
);