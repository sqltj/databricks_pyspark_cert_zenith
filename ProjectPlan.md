# Project Plan: Zenith Online Analytics Pipeline

## Databricks Configuration Requirements

To run this project end-to-end, complete the following setup steps in your Databricks workspace. This can be done by running the `utils/Setup Environment.ipynb` notebook or manually:

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
   - The setup script will automatically create the necessary subdirectories inside these volumes. The final paths for the data generator will be:
     ```
     /Volumes/zenith_online/00_landing/batch/customers
     /Volumes/zenith_online/00_landing/batch/products
     /Volumes/zenith_online/00_landing/streaming/user_events
     ```

4. **Create Volumes for Pipeline Metadata**

   - In the `_system` schema, create two volumes for storing pipeline metadata:
     - `checkpoints`
     - `schemas`

## ETL Pipeline Reconstruction Checklist

This checklist details every transformation, data quality check, and business aggregation required to build the final `ELT.py` pipeline.

### ✅ Bronze Layer: Raw Data Ingestion

- **Goal:** Ingest raw data from landing zone volumes into managed Delta tables with minimal transformation, adding necessary metadata for traceability.
- **Input:** Raw JSON, CSV, and Parquet files.
- **Output:**

  1. **`bronze_user_events` (from streaming JSON)**
  2. **`bronze_customer_profiles` (from CSV)**
  3. **`bronze_product_details` (from Parquet)**

- **Target Schema:** `01_bronze`
- **Task 1: Create `bronze_user_events` Streaming Table**

  - [ ] Use Auto Loader (`cloudFiles`) to read the streaming JSON files from the `/Volumes/zenith_online/00_landing/streaming/user_events` path.
  - [ ] Define and enforce an explicit schema for the incoming JSON data to prevent schema inference issues. The schema should include `event_id`, `event_timestamp`, `user_id`, `event_type`, `product_id`, and `session_id`.
  - [ ] Add an `ingestion_timestamp` column to record when the data was processed.
  - [ ] Convert the string-based `event_timestamp` into a proper timestamp column named `event_dt`.
  - [ ] Write the resulting stream to a Delta table named `bronze_user_events` using the `availableNow` trigger to process all available data in a single batch.
  - [ ] Configure a checkpoint location for this stream within the `_system` volume.

- **Task 2: Create `bronze_customer_profiles` Batch Table**

  - [ ] Read the batch CSV file from the `/Volumes/zenith_online/00_landing/batch/customers` path.
  - [ ] Ensure the read operation uses the headers from the CSV file.
  - [ ] Define and apply a schema to ensure `customer_id` is an integer and other columns are strings.
  - [ ] Overwrite the data into a Delta table named `bronze_customer_profiles`.

- **Task 3: Create `bronze_product_details` Batch Table**

  - [ ] Read the batch Parquet file from the `/Volumes/zenith_online/00_landing/batch/products` path.
  - [ ] Overwrite the data into a Delta table named `bronze_product_details`.

---

### ✅ Silver Layer: Cleansed, Enriched & Conformed Data

- **Goal:** Combine raw data sources, clean the data, remove duplicates, and create an enriched, queryable table for deeper analysis.
- **Input:** Bronze layer tables.
- **Output:** A central, enriched table: `silver_sessionized_activity`.
- **Target Schema:** `02_silver`
- **Task 4: Create a Pandas UDF for Region Categorization**

  - [ ] Develop a Pandas UDF named `categorize_region`.
  - [ ] The UDF must accept a pandas Series of customer locations (strings).
  - [ ] It should return a pandas Series with a corresponding region ('East Coast', 'West Coast', or 'Central') based on the input location.

- **Task 5: Create `silver_sessionized_activity` Streaming Table**

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
  - [ ] Configure a checkpoint location for this silver stream.

---

### ✅ Gold Layer: Business-Ready Analytics & Aggregations

- **Goal:** Build aggregated, denormalized tables that directly answer key business questions and are optimized for BI tools.
- **Target Schema:** `03_gold`
- **Input:** Silver layer table (`silver_sessionized_activity`).
- **Output:**

  - `gold_daily_product_performance`
  - `gold_customer_purchase_summary`

- **Task 6: Create `gold_daily_product_performance` Aggregate Table**

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

- **Task 7: Create `customer_purchase_summary` Aggregate Table**

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
- **Task 8: Query Top 5 Products**

  - [ ] Write a SQL query against the `gold_daily_product_performance` table to find the top 5 products with the highest number of purchases on the most recent day.

- **Task 9: Query Daily Revenue by Category**

  - [ ] Write a SQL query against the `gold_daily_product_performance` table to calculate the total daily revenue for each product category.

- **Task 10: Query Top 10 Customers**

  - [ ] Write a SQL query against the `customer_purchase_summary` table to identify the top 10 customers by total purchase value.
