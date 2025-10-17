# Project Plan: Zenith Online Analytics Pipeline

## ⚠️ Consistency Note: Use `variables.py` for Paths and Names

To ensure consistency across all scripts and notebooks (including the data generator, ETL pipeline, and analytics queries), **import and use the variables defined in `variables.py`**. This file contains all catalog, schema, and volume names, as well as key data paths, as Python variables.
**Do not hardcode catalog, schema, or path names**—always reference the variables, for example:

```python
from variables import (
    CATALOG_NAME,
    LANDING_SCHEMA,
    BRONZE_SCHEMA,
    RAW_STREAMING_PATH,
    RAW_BATCH_CUSTOMERS_PATH,
    RAW_BATCH_PRODUCTS_PATH,
    CHECKPOINT_BASE_PATH,
    SCHEMA_BASE_PATH,
)
```

This approach ensures that any changes to naming conventions or paths only need to be made in one place (`variables.py`), and all components of the pipeline will remain in sync.

---

## I. Setup your Databricks environment
1. **Create a Databricks Account**

   - Sign up for a [Databricks Free Edition account](https://www.databricks.com/learn/free-edition) if you don’t already have one.
   - Familiarize yourself with the workspace, clusters, and notebook interface.

2. **Import this repository to Databricks**

   - In Databricks, go to the Workspace sidebar and click the "Repos" section, click "Add Repo".
     - Alternatively, go to your personal folder, click "create" and select "git folder".
   - Paste the GitHub URL for this repository.
   - Authenticate with GitHub if prompted, and select the main branch.
   - The repo will appear as a folder in your workspace, allowing you to edit, run notebooks, and manage files directly from Databricks.
   - For more details, see the official Databricks documentation: [Repos in Databricks](https://docs.databricks.com/repos/index.html).

## II. Configure Unity Catalog

To run this project end-to-end, complete the following setup steps in your Databricks workspace.

This can be done by running the `Setup Environment.ipynb` notebook (recommended) or manually:

> **Tip:** When creating catalogs, schemas, and volumes manually, refer to the variable names in `variables.py` for consistency.

> For example, use `CATALOG_NAME`, `LANDING_SCHEMA`, `SYSTEM_SCHEMA`, etc., to match the naming in your scripts and notebooks.

1. **Create a Catalog**

   - In the Databricks Data Explorer, create a new catalog named:
     ```
     zenith_online
     ```

2. **Create Schemas (Databases)**

   - Under the `zenith_online` catalog, create the following five schemas:
     - `_system` (for storing checkpoints and other metadata)
     - `00_landing` (for raw data volumes)
     - `01_bronze`
     - `02_silver`
     - `03_gold`

3. **Create Volumes and Directories for Raw Data**

   - In the `00_landing` schema, create two volumes:
     - `batch`
     - `streaming`
   - Create "customers" and "products" directories under the "batch" volume, and "user_events" directory under the streaming volume.
   - ```
     /Volumes/zenith_online/00_landing/batch/customers
     /Volumes/zenith_online/00_landing/batch/products
     /Volumes/zenith_online/00_landing/streaming/user_events
     ```

4. **Create Volumes for Pipeline Metadata**

   - In the `_system` schema, create two volumes for storing pipeline metadata:
     - `checkpoints`
     - `schemas`

## III. Run the syntetic data generator

1. Run the data_generator.py script in databricks (it relies on the variables.py file for using paths and schema names)
2. **Important:** The data generator uses the following variables for output paths:

   1. `RAW_STREAMING_PATH`
   2. `RAW_BATCH_CUSTOMERS_PATH`
   3. `RAW_BATCH_PRODUCTS_PATH`

   Make sure any scripts or notebooks that read this data also use these variables from `variables.py`.

## IV. Check schema of the synthetic data
Go to [DataGenerator.md](DataGenerator.md) and familarize yourself with the schema!

## V. ETL Pipeline Construction Checklist

This checklist details every transformation, data quality check, and business aggregation required to build the pipeline.

You can use the "Your_Code" notebook for writing the code.

⚠️ If you ever get stuck or need inspiration, please check the final pipeline under the "final_code" folder.

### ✅ Bronze Layer: Raw Data Ingestion

- **Goal:** Ingest raw data from landing zone volumes into managed Delta tables with minimal transformation, adding necessary metadata for traceability.
- **Input:** Raw JSON, CSV, and Parquet files.
- **Output:**

  1. **`bronze_user_events` (from streaming JSON)**
  2. **`bronze_customer_profiles` (from CSV)**
  3. **`bronze_product_details` (from Parquet)**

- **Target Schema:** 01_bronze (use `BRONZE_SCHEMA` from `variables.py`)

---

- **Task 1: Create `bronze_user_events` Streaming Table**

  - Tips
    - _Why:_ Establishes a governed, append-only raw fact table capturing every user interaction with minimal transformation so downstream layers can always trace back to the original event. Using Auto Loader plus an explicit schema guarantees schema drift control and reliable incremental ingestion even as new files arrive.
    - _End Goal:_ A durable Delta table (`bronze_user_events`) in the Bronze schema that faithfully mirrors the raw JSON feed with added timestamps for lineage, enabling reproducible enrichment and time-aware processing in Silver.
  - Tasks
    - [ ] Define and enforce an explicit schema for the incoming JSON data to prevent schema inference issues. The schema should include `event_id`, `event_timestamp`, `user_id`, `event_type`, `product_id`, and `session_id`.
    - [ ] Use Auto Loader (`cloudFiles`) to read the streaming JSON files from the `/Volumes/zenith_online/00_landing/streaming/user_events` path (use `RAW_STREAMING_PATH` from `variables.py`.)
    - [ ] Add an `ingestion_timestamp` column to record when the data was processed.
    - [ ] Convert the string-based `event_timestamp` into a proper timestamp column named `event_dt`.
    - [ ] Configure a checkpoint location for this stream within the `_system` volume ()
    - [ ] Write the resulting stream to a Delta table named `bronze_user_events` using the `availableNow` trigger to process all available data in a single batch (use `BRONZE_SCHEMA` variable from variables.py)

---

- **Task 2: Create `bronze_customer_profiles` Batch Table**
  - Tips
    - _Why:_ Converts a periodically refreshed dimension snapshot (customers) into a managed Delta table so it can be efficiently joined with streaming facts. Enforcing a schema up front prevents accidental type widening (e.g., inferring customer_id as string) that would later break joins.
    - _End Goal:_ A clean, type-consistent customer dimension (`bronze_customer_profiles`) ready for enrichment joins that preserves referential integrity with events.
  - Tasks
    - [ ] Define and apply a schema to ensure `customer_id` is an integer and other columns are strings.
    - [ ] Read the batch CSV file from the `/Volumes/zenith_online/00_landing/batch/customers` path (use `RAW_BATCH_CUSTOMERS_PATH` from variables)
    - [ ] Ensure the read operation uses the headers from the CSV file.
    - [ ] Overwrite the data into a Delta table named `bronze_customer_profiles`.

---

- **Task 3: Create `bronze_product_details` Batch Table**
  - Tips
    - _Why:_ Provides a governed product dimension (names, categories, prices) required to translate low-level product\*id references in events into analytics-friendly attributes and monetary values.
    - _End Goal:_ A Delta product dimension (`bronze_product_details`) enabling category / revenue calculations and later skew-mitigated aggregations.
  - Tasks
    - [ ] Read the batch Parquet file from the `/Volumes/zenith_online/00_landing/batch/products` path (use `RAW_BATCH_PRODUCTS_PATH` from variables)
    - [ ] Overwrite the data into a Delta table named `bronze_product_details`.

---

### ✅ Silver Layer: Cleansed, Enriched & Conformed Data

- **Goal:** Combine raw data sources, clean the data, remove duplicates, and create an enriched, queryable table for deeper analysis.
- **Input:** Bronze layer tables.
- **Output:** A central, enriched table: `silver_sessionized_activity`.
- **Target Schema:** `02_silver` (use `SILVER_SCHEMA` from `variables.py`)

---

- **Task 4: Create a Pandas UDF for Region Categorization**
  - Tips
    - _Why:_ Normalizes granular location strings into a smaller, business-recognized region taxonomy to simplify segmentation, reduce cardinality in aggregations, and accelerate downstream queries.
    - _End Goal:_ A reusable function (`categorize_region`) producing a standardized region column used for customer and revenue rollups in Silver and Gold tables.
  - Tasks
    - [ ] Develop a Pandas UDF named `categorize_region`.
    - [ ] The UDF must accept a pandas Series of customer locations (strings).
    - [ ] It should return a pandas Series with a corresponding region ('East Coast', 'West Coast', or 'Central') based on the input location. (Note - the data generator provides only the following regions - ['New York', 'Philadelphia'] (both east coast) and ['Los Angeles', 'San Diego'] (both west coast)

---

- **Task 5: Create `silver_sessionized_activity` Streaming Table**
  - Tips
    - _Why:_ Transforms noisy raw events into a de-duplicated, enriched activity fact set combining product, customer, temporal, and derived region context—shaping data into a form suited for consistent business aggregation without re-implementing joins each time.
    - _End Goal:_ A single conformed Silver table (`silver_sessionized_activity`) that is the authoritative behavioral dataset powering all Gold-layer KPIs.
  - Tasks
    - [ ] Read the `bronze_user_events` table as a stream.
    - [ ] Read the `bronze_customer_profiles` and `bronze_product_details` tables as static DataFrames for enrichment.
    - [ ] **Apply Watermarking & Deduplication:**
      - Set a 3-minute watermark on the `event_dt` column to handle late-arriving data.
      - Drop duplicate events based on the `event_id` column.
    - [ ] **Enrich the Data via Joins:**
      - Perform an `inner` join with the products table on `product_id`.
      - Perform a `left` join with the customers table on `user_id`.
      - **Apply Broadcast Join:** The smaller products DataFrame should be broadcast to optimize the join performance.
    - [ ] **Apply Column Transformations:**
      - Use the `categorize_region` Pandas UDF to add a `region` column based on the customer's `location`.
      - Rename the `event_type` column to `action`.
      - Create a new `event_date` column by casting the `event_dt` timestamp to a date.
    - [ ] **Select and Finalize Schema:** Select a final set of columns in a logical order to form the silver table schema.
    - [ ] Write the enriched stream to a Delta table named `silver_sessionized_activity` using the `availableNow` trigger.
    - [ ] Configure a checkpoint location for this silver stream (use `CHECKPOINT_BASE_PATH` from `variables.py`.)

---

### ✅ Gold Layer: Business-Ready Analytics & Aggregations

- **Goal:** Build aggregated, denormalized tables that directly answer key business questions and are optimized for BI tools.
- **Target Schema:** `03_gold`
- **Input:** Silver layer table (`silver_sessionized_activity`).
- **Output:**

  - `gold_daily_product_performance`
  - `gold_customer_purchase_summary`

---

- **Task 6: Create `gold_daily_product_performance` Aggregate Table**
  - Tips
    - _Why:_ Produces daily product performance metrics (views, funnel progression, revenue) optimized for BI dashboards and trend analyses. Salting mitigates impact of skewed hot products, ensuring stable performance and accurate ranking calculations.
    - _End Goal:_ A partitioned, query-ready Delta table (`gold_daily_product_performance`) enabling rapid insights into product engagement and monetization by day and category with embedded revenue ranking.
  - Tasks
    - [ ] Read the `silver_sessionized_activity` table as a batch DataFrame.
    - [ ] **Handle Data Skew:**
      - Add a temporary "salt" column containing a random integer between 0 and 4. This will help distribute the aggregation workload for popular products across more Spark tasks.
      - Perform a preliminary aggregation grouped by `event_date`, `product_id`, `product_name`, `category`, and the `salt` column. Calculate views, adds-to-cart, purchases, and revenue.
    - [ ] **Final Aggregation:**
      - Perform a second aggregation on the salted results. Group by `event_date`, `product_id`, `product_name`, and `category` to sum the salted sub-totals into final totals.
    - [ ] **Add Product Ranking:**
      - Use a window function to calculate a `revenue_rank` for each product. The rank should be partitioned by `event_date` and `category`, and ordered by total revenue in descending order.
    - [ ] **Write the Gold Table:**
      - Overwrite the final DataFrame into a Delta table named `gold_daily_product_performance`.
      - Partition the table by `event_date` to optimize queries that filter by date.

---

- **Task 7: Create `customer_purchase_summary` Aggregate Table**
  - Tips
    - _Why:_ Consolidates purchase behavior per customer to support LTV-style analysis, top customer identification, and targeted marketing segmentation. The null region count surfaces data completeness issues early.
    - _End Goal:_ A Gold table (`customer_purchase_summary`) listing monetization KPIs per customer, sorted for immediate consumption by business stakeholders.
  - Tasks
    - [ ] Read the `silver_sessionized_activity` table as a batch DataFrame.
    - [ ] **Perform Data Quality Check:** Calculate and print the count of events where the customer `region` is `NULL`.
    - [ ] **Filter and Aggregate:**
      - Filter the data to include only `purchase` actions.
      - Group by `user_id` and `region`.
      - Calculate the following metrics for each customer: `total_purchase_value`, `total_purchases`, `distinct_products_purchased` (using an approximate count), and `last_purchase_timestamp`.
    - [ ] **Sort and Write:**
      - Order the resulting DataFrame by `total_purchase_value` descending.
      - Overwrite the data into a Delta table named `customer_purchase_summary`.

---

### ✅ Analytics: Answering Business Questions

- **Goal:** Use Spark SQL to query the Gold tables and provide answers to business stakeholders.

---

- **Task 8: Query Top 5 Products**
  - Tips
    - _Why:_ Identifies current best-selling products, informing inventory prioritization and promotional decisions.
    - _End Goal:_ A reproducible query pattern that stakeholders can adapt for daily merchandising reports.
  - Tasks
    - [ ] Write a SQL query against the `gold_daily_product_performance` table to find the top 5 products with the highest number of purchases on the most recent day.

---

- **Task 9: Query Daily Revenue by Category**
  - Tips
    - _Why:_ Tracks category-level revenue trends to spot growth, seasonality, or emerging declines, guiding assortment strategy.
    - _End Goal:_ A concise revenue-by-category time series supporting dashboards and forecasting inputs.
  - Tasks
    - [ ] Write a SQL query against the `gold_daily_product_performance` table to calculate the total daily revenue for each product category.

---

- **Task 10: Query Top 10 Customers**
  - Tips
    - _Why:_ Surfaces high-value customers for retention campaigns, VIP programs, and churn risk monitoring.
    - _End Goal:_ A ranked customer list enabling marketing and customer success teams to act on highest ROI opportunities.
  - Tasks
    - [ ] Write a SQL query against the `customer_purchase_summary` table to identify the top 10 customers by total purchase value.
