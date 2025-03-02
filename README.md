# Databricks Health Insurance Rate Analysis Pipeline

## Overview
This project implements a data pipeline in Databricks to process, transform, and analyze health insurance rate data efficiently. The pipeline follows a Medallion architecture (Bronze, Silver, Gold) to optimize data storage, transformation, and querying.

## Data Scources
This project utilizes publicly available health insurance and economic data from two sources:

- **Health Insurance Exchange Public Use Files (Exchange PUFs) (2018-2024)** : 
Provided by the Centers for Medicare & Medicaid Services (CMS), these datasets include plan-level details on premiums, copayments, deductibles, and other attributes for Qualified Health Plans (QHPs) and Stand-Alone Dental Plans (SADPs) offered through the Health Insurance Exchanges. The data covers states using the Federally Facilitated Exchanges (FFE) and those with State-based Exchanges on the federal platform.
  - Rate PUF: Contains plan rates based on factors such as age, tobacco use, and location.
  - Plan Attributes PUF: Includes information on deductibles, out-of-pocket maximums, and other key plan characteristics.

- **Gross Domestic Product (GDP) by State (2005 - 2024)** :
Sourced from the Bureau of Economic Analysis (BEA), this dataset provides state-level GDP changes, reflecting economic growth trends across the U.S.

## Governance with Unity Catalog
This project implements data governance using Databricks Unity Catalog, ensuring secure and efficient management of data access, lineage, and compliance.

## Data Workflow

### 1. Bronze Layer (Raw Data Ingestion with Initial Preprocessing)
The Bronze tables serve as the foundation of the pipeline, storing raw data ingested from external sources. Due to schema inconsistencies and formatting issues, some preprocessing was applied before storing data in these tables:

- **plans_bronze**:
  - Data ingested from external sources contained inconsistent column headers and formatting issues
  - Applied transformations to rename columns for consistency and readability
  - Converted relevant fields to appropriate data types
  - Dropped unnecessary columns such as DentalOnlyPlan and non-standard plan types
  - Stored results in a Delta table with 156,076 rows

- **rates_bronze**:
  - The largest dataset in the pipeline, containing 16,138,589 rows
  - Standardized BusinessYear to an integer type
  - Filtered out null or invalid values in key columns like PlanId and Rate
  - Ensured all age values were properly formatted

- **state_gdp_bronze**:
  - The smallest dataset in the pipeline
  - Separates non-quarter columns (e.g., metadata fields) from quarter columns, which contain time-based financial data in the format "year:quarter"
  - Reshapes the dataset from a wide format (multiple quarter columns) into a long format 
  - Filters records to include only rows where Unit is "Millions of current dollars"

### 2. Silver Layer (Refined Data with Enhanced Structure)
Once the raw data was cleaned in the Bronze layer, the Silver layer transformed it further to enhance usability and enforce data integrity.

- **plans_silver**:
  - Removed dental-only plans and indemnity plans
  - Standardized the metal_level field by merging similar categories (e.g., Expanded Bronze → Bronze)
  - Converted the new_plan field to lowercase for uniformity
  - Introduced a pandemic_era column to differentiate pre-COVID and post-COVID plans
  - Eliminated duplicate records by grouping on unique identifiers
  - Stored in Delta format for easier downstream integration

- **rates_silver**:
  - Standardized the age column by converting textual values into appropriate integer ranges
  - Introduced an age_category column to categorize rates into meaningful groups
  - Filtered out rows with Family Option, as they were not relevant for individual plan pricing
  - Applied a window function to detect and remove plans with improperly set rates

### 3. Gold Layer (Optimized for Analysis & Efficient Joins)
The Gold layer was carefully designed to balance storage efficiency and query performance by reducing row count while maintaining analytical power.

- **rates_fact** (Cumulative Fact Table):
  - Rates were grouped by PlanId and StateCode, then stored as an array of historical rate values
  - The array stored bucketed rates for different age groups, as well as the year
  - This approach followed cumulative table design, looping through each years data and appending to the array, with each row in the final year containing all plans with the cumulated rate data
  - Reduced row count from 221,826 to 15,200 while preserving time-series analysis capabilities, a reduction of 93.15 rows
  - Each row contains an array of rate values per age group over multiple years

- **plans_dim_scd** (Slowly Changing Dimension Type 2 for Plans):
  - Tracks plan attributes that change over time (e.g., MetalLevel, PlanType)
  - Includes start_year and end_year columns denoting the period of validity for each row
  - Supports historical plan attribute analysis without excessive duplication
 
- **state_gdp_dim** :
  - Contains data related to the average GDP of different states over various business years
  - Serves as a reference for analyzing the economic performance of states over time
  - The table includes information on state codes, business years, and the average GDP values
  - Essential for tracking and comparing the economic growth of states within a specific timeframe

- **Gold Layer ERD** :
![Image Alt](https://github.com/Rabago85/databricks_project/blob/896e31c6d5163bce7ca2e26614cda32576a816a5/erd.jpg)

### 4. Pipeline DAG
The pipeline was run using Databricks workflows.
![Image Alt](https://github.com/Rabago85/databricks_project/blob/188ec05ce09d1b1d34f8cf990e3b9fbfb7b9f952/dag.jpg)

## SQL Analysis & Results
Several SQL queries were executed to analyze trends in health insurance rates, leveraging the efficient Gold-layer design.  In order to use the cumulative rates_fact table, the sql queries had to handle explode and unpivot logic.

### Key Insights:

1. **Plan Type Trends**:
   - PPO plans consistently had the highest average rates, followed by POS, EPO, and HMO plans
   - PPO plans saw the most significant increase in rates post-2021
   - HMO plans remained the lowest-cost option across all years
![Image Alt](https://github.com/Rabago85/databricks_project/blob/896e31c6d5163bce7ca2e26614cda32576a816a5/y_o_y_rate_by_plan_type.jpg)

2. **New vs. Existing Plans**:
   - New plans initially had competitive rates but increased at a steeper rate over time
   - New plans overtook existing plan costs by 2024
   - Suggests new plans might start with lower rates to attract enrollees
![Image Alt](https://github.com/Rabago85/databricks_project/blob/896e31c6d5163bce7ca2e26614cda32576a816a5/y_o_y_rate_by_new_or_existing.jpg)

3. **Pandemic Impact**:
   - Clear upward shift in rates post-pandemic (2021 and beyond)
   - Most pronounced increase between 2022 and 2024
   - Reinforces impact of post-pandemic economic conditions on premium hikes
![Image Alt](https://github.com/Rabago85/databricks_project/blob/896e31c6d5163bce7ca2e26614cda32576a816a5/y_o_y_rate_by_pandemic_era.jpg)

4. **Fairness Index by State**:
   - Significant disparities across states
   - Some states exhibit consistently high fairness scores, while others fluctuate
   - Low fairness scores indicate potential regulatory inconsistencies or market-driven pricing disparities
![Image Alt](https://github.com/Rabago85/databricks_project/blob/896e31c6d5163bce7ca2e26614cda32576a816a5/y_o_y_fairness_index.jpg)

## Areas for Improvement

1. **Optimizing Queries Using Spark**
   - Implement partitioning, bucketing, and indexing to improve SQL query performance

2. **Better Modeling in the Gold Layer**
   - Investigate alternative ways to model the gold layer that could simplify SQL queries.  The analysis queries used more CTE's than anticipated, so investigting more refined modeling could help for simplified downstream queries

3. **Using Views for Analysis**
   - Introduce views in the analysis layer to abstract away complex SQL logic for easier analyst access

## Technologies Used

- Databricks (ETL, Delta Tables, Spark SQL)
- Apache Spark (Data Processing)
- Python (PySpark) (ETL Pipelines)
- SQL (Data Analysis)
- Delta Lake (Storage & Transaction Management)

## Conclusion
This pipeline is designed to keep storage efficient and queries fast by using structured data modeling and smart ETL practices. By implementing cumulative fact tables and SCD2 dimensions, we've cut down on data volume without losing important historical insights. Moving forward, the focus will be on fine-tuning Spark queries, improving the Gold-layer model, and making it easier for end-users to analyze the data.
