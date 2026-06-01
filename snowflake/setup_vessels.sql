CREATE OR REPLACE STAGE vessels_s3_stage
  URL = 's3://marisight-staging-layer-121913093195-us-east-1-an/vessels/'
  CREDENTIALS = (
    AWS_KEY_ID = 'YOUR_AWS_KEY_HERE'
    AWS_SECRET_KEY = 'YOUR_AWS_SECRET_KEY_HERE'
  );
list @vessels_s3_stage

CREATE TABLE IF NOT EXISTS project_db.dbo.vessel (
    name                     STRING,
    type                     STRING,
    year_built               STRING,
    gross_tonnage            STRING,
    deadweight               STRING,
    "length(m)"              STRING,
    "beam(m)"                STRING,
    detail_link              STRING,
    departure_date           STRING,
    last_port_country        STRING,
    last_port_name           STRING,
    arrival_date             STRING,
    destination_port_country STRING,
    destination_port_name    STRING,
    destination_port_lat     STRING,
    destination_port_lon     STRING,
    reported_status          STRING,
    report_date              STRING
);
SELECT * FROM PROJECT_DB.DBO.vessel;


select count(*) from vessel


