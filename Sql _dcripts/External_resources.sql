CREATE MASTER KEY ENCRYPTION BY PASSWORD = '*********'

----------------------------------------
--- DATABASE SCOPED CREDENTIAL
----------------------------------------
CREATE DATABASE SCOPED CREDENTIAL azure_dwh_creds
WITH IDENTITY = 'Managed Identity'

----- EXTERNAL DATA SOURCE -------

CREATE EXTERNAL DATA SOURCE silver_source
WITH (
    LOCATION = 'https://azuredwhdl.dfs.core.windows.net/silver/',
    CREDENTIAL = azure_dwh_creds
)

CREATE EXTERNAL DATA SOURCE gold_source
WITH (
    LOCATION = 'https://azuredwhdl.dfs.core.windows.net/gold/',
    CREDENTIAL = azure_dwh_creds
)


--- EXTERNAL FILE FORMAT

CREATE EXTERNAL FILE FORMAT parquet_format
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)

--- FOR DELTA FILE FORMAT
CREATE EXTERNAL FILE FORMAT delta_format
WITH (
    FORMAT_TYPE = DELTA
    )
