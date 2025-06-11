--- Dimension Customer
IF NOT EXISTS (
    select * from sys.tables t
    join
    sys.schemas s 
    on t.schema_id = s.schema_id
    where t.name = 'DimCustomer' and s.name = 'gold'
)
CREATE EXTERNAL TABLE gold.DimCustomer
WITH (
        LOCATION = 'DimCustomer',
        DATA_SOURCE = gold_source,
        FILE_FORMAT = parquet_format
        )
AS
SELECT ROW_NUMBER() OVER (ORDER BY CustomerID) DimCustomerKey,
CustomerID,
CustomerName,
CustomerEmail,
Domain
FROM 
(SELECT DISTINCT(CustomerID), 
CustomerName,
CustomerEmail, 
Domain
FROM silver.SilverTable) t


---- DIM PRODUCT
IF NOT EXISTS (select * FROM
sys.tables t join sys.schemas s 
on t.schema_id = s.schema_id
where t.name = 'DimProduct' and s.name = 'gold')
CREATE EXTERNAL TABLE gold.DimProduct
WITH (
    LOCATION = 'DimProduct',
    DATA_SOURCE = gold_source,
    FILE_FORMAT = parquet_format
)
AS
Select ROW_NUMBER() over (order by ProductID) as DimProductKey, *
FROM
(SELECT
DISTINCT ProductID,
ProductName,
ProductCategory
from silver.SilverTable) t

---- Dim Geography
IF NOT EXISTS (select 
* from sys.tables t 
join sys.schemas s
on t.schema_id = s.schema_id
where t.name = 'DimGeography' and s.name = 'gold')
CREATE EXTERNAL TABLE gold.DimGeography
WITH (
LOCATION = 'DimGeography',
DATA_SOURCE = gold_source,
FILE_FORMAT = parquet_format
)
AS 
SELECT ROW_NUMBER() over (order by RegionID) as DimGeographyKey, *
FROM 
(SELECT 
DISTINCT RegionID,
RegionName,
Country
from silver.SilverTable) t

----- DIM ORDER -----
IF NOT EXISTS (
    select * from sys.tables t
    join sys.schemas s
    on t.schema_id = s.schema_id
    where t.name = 'DimOrders' and s.name = 'gold'
)
CREATE EXTERNAL TABLE gold.DimOrders
WITH(
    LOCATION = 'DimOrders',
    DATA_SOURCE = gold_source,
    FILE_FORMAT = parquet_format
)
AS 
SELECT 
DISTINCT OrderID,
OrderDate,
CustomerID,
CustomerName,
CustomerEmail, 
ProductID,
ProductName,
ProductCategory,
RegionID,
RegionName,
Country,
Domain,
ROW_NUMBER() OVER (ORDER BY CustomerID) as DimOrdersKey
FROM silver.SilverTable
order by OrderID

---- FACT TABLE
IF NOT EXISTS (
    select * from sys.tables t
    join sys.schemas s
    on t.schema_id = s.schema_id
    where t.name = 'FactOrders' and s.name = 'gold'
)
CREATE EXTERNAL TABLE gold.FactOrders
WITH(
    LOCATION = 'FactOrders',
    DATA_SOURCE = gold_source,
    FILE_FORMAT = parquet_format
)
AS
SELECT 
do.DimOrdersKey,
dc.DimCustomerKey,
dp.DimProductKey,
dg.DimGeographyKey,
s.Quantity,
s.UnitPrice, 
s.TotalAmount
from silver.SilverTable s
left join gold.DimOrders do
on s.OrderID = do.OrderID
left join gold.DimCustomer dc
on s.CustomerID = dc.CustomerID
left join gold.DimProduct dp
on s.ProductID = dp.ProductID 
left join gold.DimGeography dg
on s.RegionID = dg.RegionID and dg.Country = s.Country
