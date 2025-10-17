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

## I. Clone the repository to Databricks

## II. Databricks Configuration Requirements

To run this project end-to-end, complete the following setup steps in your Databricks workspace.

This can be done by running the `utils/Setup Environment.ipynb` notebook (recommended) or manually:

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

1. Go to "utils"
2. Run the data_generator.py script in databricks (it relies on the variables.py file for using paths and schema names)
3. **Important:** The data generator uses the following variables for output paths:

   1. `RAW_STREAMING_PATH`
   2. `RAW_BATCH_CUSTOMERS_PATH`
   3. `RAW_BATCH_PRODUCTS_PATH`

   Make sure any scripts or notebooks that read this data also use these variables from `variables.py`.
