
# Azure E‑commerce Data Warehouse with Synapse Analytics

This project demonstrates an end‑to‑end e‑commerce order data warehouse on Azure Synapse Analytics. Leveraging Azure Data Lake Storage Gen2, Synapse Pipelines, Mapping Data Flows, and Serverless SQL Pools, it implements a medallion architecture:

- **Bronze**: Raw CSV files ingested via Synapse Copy activity and stored as Parquet.  
- **Silver**: Enriched and conformed Parquet datasets, exposed via external tables for fast querying.  
- **Gold**: Star‑schema model (dimensions + fact) built atop the Silver layer using SQL external tables.  

---

## Solution Components

### Storage Layers

- **Source**:  
  - Container holds the original CSV order files.  
- **Bronze**:  
  - Parquet‑converted raw data.  
- **Silver**:  
  - Curated and cleaned Parquet files  
- **Gold**:  
  - Parquet files for each dimension and fact under `gold/DimCustomer`, `gold/DimProduct`, `gold/DimGeography`, `gold/FactOrders`, etc.

### Pipelines and Data Flows

1. **Raw Ingestion Pipeline**  
   - **Copy activity**  
     - **Source**: CSV in `source/orders/`  
     - **Sink**: Parquet in `bronze/orders/`  

2. **Silver Enrichment Pipeline**  
   - **Mapping Data Flow**  
     - Reads Bronze Parquet files  
     - Applies cleansing and type conversions  
     - Writes enriched output to `silver/enrichedsales/` in Parquet  

3. **Gold Modeling**  
   - Execute SQL scripts in the Synapse SQL pool to create external tables for:  
     - **DimCustomer**, **DimProduct**, **DimGeography**, **DimOrders**  
     - **FactOrders**, joining the Silver zone to all dimensions  

