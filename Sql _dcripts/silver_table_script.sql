use salesDWH;

CREATE SCHEMA silver;

CREATE SCHEMA gold;


IF NOT EXISTS(
    select * from 
    sys.tables t 
    join sys.schemas s 
    on t.schema_id = s.schema_id
    where t.name = 'SilverTable' and s.name = 'silver')
    CREATE EXTERNAL TABLE silver.SilverTable(
        OrderID VARCHAR(100),
        OrderDate DATE,
        CustomerID VARCHAR(100),
        CustomerName VARCHAR(100),
        CustomerEmail VARCHAR(100),
        ProductID VARCHAR(100),
        ProductName VARCHAR(100),
        ProductCategory VARCHAR(100),
        RegionID VARCHAR(100),
        RegionName VARCHAR(100),
        Country VARCHAR(100),
        Quantity VARCHAR(100),
        UnitPrice INT,
        TotalAmount INT,
        Domain VARCHAR(100)
    )
    WITH (
        LOCATION = 'enrichedsales',
        DATA_SOURCE = silver_source,
        FILE_FORMAT = parquet_format
        )
