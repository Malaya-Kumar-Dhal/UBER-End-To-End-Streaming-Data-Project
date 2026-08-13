# **UBER REAL-TIME DATA ENGINEERING PROJECT**

# 🚗 Uber Data Engineering Project

An end-to-end **Uber Data Engineering pipeline** built using Microsoft Azure, Azure Data Factory, Azure Data Lake Storage Gen2, Databricks, Apache Spark, Delta Lake, and Azure Event Hubs.

The project demonstrates how raw Uber ride data can be ingested, stored, transformed, enriched, and processed using a modern cloud data engineering architecture.

---

## 🏗️ Architecture

```text
                    ┌──────────────────┐
                    │   GitHub / JSON  │
                    │    Source Data   │
                    └────────┬─────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │   Azure Data        │
                  │     Factory         │
                  └─────────┬───────────┘
                            │
                            ▼
                  ┌─────────────────────┐
                  │     ADLS Gen2       │
                  │  Raw / Ingestion    │
                  └─────────┬───────────┘
                            │
                            ▼
                  ┌─────────────────────┐
                  │     Databricks      │
                  │      + Spark        │
                  └─────────┬───────────┘
                            │
                            ▼
                  ┌─────────────────────┐
                  │    Bronze Layer     │
                  │    Delta Tables     │
                  └─────────┬───────────┘
                            │
                            ▼
                  ┌─────────────────────┐
                  │ Streaming /         │
                  │ Transformations     │
                  └─────────┬───────────┘
                            │
                            ▼
                  ┌─────────────────────┐
                  │     Silver OBT      │
                  │ Enriched Ride Data  │
                  └─────────┬───────────┘
                            │
                            ▼
                  ┌─────────────────────┐
                  │ Analytics / Gold    │
                  │       Layer         │
                  └─────────────────────┘

             Azure Event Hubs
                    │
                    ▼
             Event Streaming
```

---

## 🎯 Project Objective

The objective of this project is to build a scalable data engineering pipeline that processes Uber ride data from raw JSON files into structured and enriched Delta tables.

The pipeline demonstrates:

* Automated data ingestion
* Cloud data lake storage
* Batch and streaming processing
* Data transformation
* Data enrichment using lookup tables
* Delta Lake table management
* Medallion architecture
* Azure-based data engineering

---

## 🛠️ Technologies Used

| Technology                       | Purpose                                    |
| -------------------------------- | ------------------------------------------ |
| **Azure Data Factory**           | Data ingestion and pipeline orchestration  |
| **Azure Data Lake Storage Gen2** | Cloud data lake storage                    |
| **Azure Databricks**             | Data processing and transformation         |
| **PySpark**                      | Distributed data processing                |
| **Spark SQL**                    | SQL-based transformations                  |
| **Delta Lake**                   | Reliable data storage and table management |
| **Azure Event Hubs**             | Real-time event streaming                  |
| **Python**                       | Data ingestion and processing              |
| **GitHub**                       | Source data and version control            |

---

## 📂 Source Data

The project uses JSON datasets containing Uber-related information.

### Main datasets

* `bulk_rides.json`
* `map_cities.json`
* `map_cancellation_reasons.json`
* `map_payment_methods.json`
* `map_ride_statuses.json`
* `map_vehicle_makes.json`
* `map_vehicle_types.json`

---

## 🔄 Data Pipeline

### 1. Data Ingestion

Raw JSON files are retrieved from the source repository using **Azure Data Factory**.

ADF dynamically processes multiple files using a `ForEach` activity.

The pipeline uses dynamic expressions to construct file paths:

```text
@{concat(item().file, '.json')}
```

This allows the same pipeline to process multiple datasets without creating separate activities for every file.

---

### 2. Raw Data Storage

The ingested files are stored in **Azure Data Lake Storage Gen2**.

Example structure:

```text
raw/
└── ingestion/
    ├── map_cities.json
    ├── map_cancellation_reasons.json
    ├── bulk_rides.json
    ├── map_payment_methods.json
    ├── map_ride_statuses.json
    ├── map_vehicle_makes.json
    └── map_vehicle_types.json
```

---

### 3. Bronze Layer

The raw JSON data is converted into Spark DataFrames and stored as Delta tables in the Bronze layer.

Example:

```text
uber.bronze.bulk_rides
uber.bronze.map_cities
uber.bronze.map_vehicle_makes
uber.bronze.map_vehicle_types
```

The Bronze layer preserves the ingested data in a structured and queryable format.

---

## 🥉 Bronze Layer

The Bronze layer contains the initial structured representation of the raw data.

Example tables:

```text
uber.bronze.stg_rides
uber.bronze.map_cities
uber.bronze.map_cancellation_reasons
uber.bronze.map_payment_methods
uber.bronze.map_ride_statuses
uber.bronze.map_vehicle_makes
uber.bronze.map_vehicle_types
```

---

## 🥈 Silver Layer

The Silver layer performs data enrichment and transformation.

The project creates an enriched streaming table:

```text
silver_obt
```

The Silver OBT combines ride data with lookup information such as:

* Vehicle make
* Vehicle type
* Vehicle description
* Base rate
* Per-mile rate
* Per-minute rate

Example transformation:

```sql
CREATE OR REFRESH STREAMING TABLE silver_obt
AS
SELECT
    stg_rides.ride_id,
    stg_rides.confirmation_number,
    stg_rides.passenger_id,
    stg_rides.driver_id,
    stg_rides.vehicle_id,
    stg_rides.pickup_location_id,
    stg_rides.dropoff_location_id,
    stg_rides.vehicle_type_id,
    stg_rides.vehicle_make_id,
    stg_rides.payment_method_id,
    stg_rides.ride_status_id,
    stg_rides.pickup_city_id,
    stg_rides.dropoff_city_id,
    stg_rides.cancellation_reason_id,
    stg_rides.passenger_name,
    stg_rides.passenger_email,
    stg_rides.passenger_phone,
    stg_rides.driver_name,
    stg_rides.driver_rating,
    stg_rides.driver_phone,
    stg_rides.driver_license,
    stg_rides.vehicle_model,
    stg_rides.vehicle_color,
    stg_rides.license_plate,
    stg_rides.pickup_address,
    stg_rides.pickup_latitude,
    stg_rides.pickup_longitude,
    stg_rides.dropoff_address,
    stg_rides.dropoff_latitude,
    stg_rides.dropoff_longitude,
    stg_rides.distance_miles,
    stg_rides.duration_minutes,
    stg_rides.booking_timestamp,
    stg_rides.pickup_timestamp,
    stg_rides.dropoff_timestamp,
    stg_rides.base_fare,
    stg_rides.distance_fare,
    stg_rides.time_fare,
    stg_rides.surge_multiplier,
    stg_rides.subtotal,
    stg_rides.tip_amount,
    stg_rides.total_fare,
    stg_rides.rating,
    map_vehicle_makes.vehicle_make,
    map_vehicle_types.vehicle_type,
    map_vehicle_types.description,
    map_vehicle_types.base_rate,
    map_vehicle_types.per_mile,
    map_vehicle_types.per_minute
FROM STREAM(uber.bronze.stg_rides) AS stg_rides

WATERMARK booking_timestamp
DELAY OF INTERVAL 3 MINUTES

LEFT JOIN uber.bronze.map_vehicle_makes AS map_vehicle_makes
    ON stg_rides.vehicle_make_id =
       map_vehicle_makes.vehicle_make_id

LEFT JOIN uber.bronze.map_vehicle_types AS map_vehicle_types
    ON stg_rides.vehicle_type_id =
       map_vehicle_types.vehicle_type_id;
```

---

## ⚡ Streaming Processing

The project uses **Spark Structured Streaming / streaming tables** to process ride data incrementally.

A watermark is applied to the booking timestamp:

```text
WATERMARK booking_timestamp
DELAY OF INTERVAL 3 MINUTES
```

This helps manage late-arriving events during streaming processing.

---

## 📡 Azure Event Hubs

Azure Event Hubs is used for event-driven data ingestion and real-time processing.

The architecture allows ride events to be published and consumed as streaming data.

```text
Uber Ride Event
      ↓
Azure Event Hubs
      ↓
Databricks / Spark
      ↓
Delta Tables
```

---

## 🧩 Dynamic Pipeline Processing

Instead of creating a separate ADF activity for every JSON file, the project uses a dynamic `ForEach` pipeline.

The file list is generated dynamically:

```json
[
    {"file": "map_cities"},
    {"file": "map_cancellation_reasons"},
    {"file": "bulk_rides"},
    {"file": "map_payment_methods"},
    {"file": "map_ride_statuses"},
    {"file": "map_vehicle_makes"},
    {"file": "map_vehicle_types"}
]
```

The pipeline then dynamically creates the filename:

```text
@{concat(item().file, '.json')}
```

This makes the ingestion process reusable and scalable.

---

## 🗂️ Medallion Architecture

The project follows the **Medallion Architecture**:

```text
             RAW DATA
                │
                ▼
        ┌───────────────┐
        │    BRONZE     │
        │ Raw / Initial │
        │  Delta Data   │
        └───────┬───────┘
                │
                ▼
        ┌───────────────┐
        │    SILVER     │
        │ Cleaned +     │
        │ Enriched Data │
        └───────┬───────┘
                │
                ▼
        ┌───────────────┐
        │     GOLD      │
        │ Analytics /   │
        │ Business Data │
        └───────────────┘
```

---

## 🚀 Key Features

* End-to-end Azure data engineering pipeline
* Dynamic file ingestion using ADF
* ADLS Gen2 data lake
* Delta Lake storage
* Bronze/Silver architecture
* Spark SQL transformations
* PySpark processing
* Streaming tables
* Watermark-based streaming processing
* Lookup table enrichment
* Azure Event Hubs integration
* Parameterized and reusable pipelines
* GitHub-based source data ingestion

---

## 📊 Example Data Enrichment

Raw ride data contains IDs such as:

```text
vehicle_make_id
vehicle_type_id
```

The Silver layer enriches these IDs using lookup tables:

```text
vehicle_make_id
       │
       ▼
map_vehicle_makes
       │
       ▼
vehicle_make
```

and:

```text
vehicle_type_id
       │
       ▼
map_vehicle_types
       │
       ├── vehicle_type
       ├── description
       ├── base_rate
       ├── per_mile
       └── per_minute
```

---

## 🔐 Security

Credentials and SAS tokens should **not** be committed to GitHub.

Use:

* Azure Key Vault
* Managed Identity
* Environment variables
* Databricks secrets
* ADF linked-service authentication

for production deployments.

---

## 📁 Project Structure

```text
Uber_Data_Engineer_Project/
│
├── Data/
│   ├── bulk_rides.json
│   ├── map_cities.json
│   ├── map_cancellation_reasons.json
│   ├── map_payment_methods.json
│   ├── map_ride_statuses.json
│   ├── map_vehicle_makes.json
│   └── map_vehicle_types.json
│
├── ADF/
│   └── Data Factory pipelines
│
├── Databricks/
│   ├── Bronze
│   ├── Silver
│   └── Gold
│
├── Python/
│   └── ingestion scripts
│
└── README.md
```

---

## 🧠 What I Learned

Through this project, I worked with:

* Cloud-based data ingestion
* Azure Data Factory pipelines
* ADLS Gen2
* Databricks
* PySpark
* Spark SQL
* Delta Lake
* Streaming data processing
* Watermarking
* Data enrichment
* Medallion architecture
* Dynamic pipeline expressions
* Azure Event Hubs
* End-to-end data engineering workflows

---

## 👨‍💻 Author

**Malaya Kumar Dhal**

GitHub:
https://github.com/Malaya-Kumar-Dhal

---

## ⭐ Future Improvements

* Add a Gold layer for business analytics
* Add Power BI dashboards
* Implement automated data quality checks
* Add CI/CD using GitHub Actions
* Implement Azure Key Vault integration
* Add monitoring and alerting
* Improve streaming scalability
* Add automated testing

---

## 📌 Project Status

**Completed — End-to-End Data Engineering Pipeline**

The project successfully demonstrates ingestion from source data → Azure Data Factory → ADLS Gen2 → Databricks → Delta Bronze → Streaming Silver transformations.
