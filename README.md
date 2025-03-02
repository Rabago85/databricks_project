# Databricks Health Insurance Rate Analysis Pipeline

## Overview
This project implements a data pipeline in Databricks to process, transform, and analyze health insurance rate data efficiently. The pipeline follows a Medallion architecture (Bronze, Silver, Gold) to optimize data storage, transformation, and querying.

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
  - Reduced row count from 221,826 to 15,200 while preserving time-series analysis capabilities
  - Each row contains an array of rate values per age group over multiple years

- **plans_dim_scd** (Slowly Changing Dimension Type 2 for Plans):
  - Tracks plan attributes that change over time (e.g., MetalLevel, PlanType)
  - Includes start_year and end_year columns denoting the period of validity for each row
  - Supports historical plan attribute analysis without excessive duplication

## SQL Analysis & Results
Several SQL queries were executed to analyze trends in health insurance rates, leveraging the efficient Gold-layer design.

### Key Insights:

1. **Plan Type Trends**:
   - PPO plans consistently had the highest average rates, followed by POS, EPO, and HMO plans
   - PPO plans saw the most significant increase in rates post-2021
   - HMO plans remained the lowest-cost option across all years

2. **New vs. Existing Plans**:
   - New plans initially had competitive rates but increased at a steeper rate over time
   - New plans overtook existing plan costs by 2024
   - Suggests new plans might start with lower rates to attract enrollees

3. **Pandemic Impact**:
   - Clear upward shift in rates post-pandemic (2021 and beyond)
   - Most pronounced increase between 2022 and 2024
   - Reinforces impact of post-pandemic economic conditions on premium hikes

4. **Fairness Index by State**:
   - Significant disparities across states
   - Some states exhibit consistently high fairness scores, while others fluctuate
   - Low fairness scores indicate potential regulatory inconsistencies or market-driven pricing disparities

## Areas for Improvement

1. **Optimizing Queries Using Spark**
   - Implement partitioning, bucketing, and indexing to improve SQL query performance

2. **Better Modeling in the Gold Layer**
   - Investigate alternative ways to model the gold layer that could simplify SQL queries

3. **Using Views for Analysis**
   - Introduce views to abstract away complex SQL logic for easier analyst access

## Technologies Used

- Databricks (ETL, Delta Tables, Spark SQL)
- Apache Spark (Data Processing)
- Python (PySpark) (ETL Pipelines)
- SQL (Data Analysis)
- Delta Lake (Storage & Transaction Management)

## Conclusion
By leveraging structured data modeling and efficient ETL practices, this pipeline optimizes storage and query performance. The use of cumulative fact tables and SCD2 dimensions has significantly reduced data volume while maintaining analytical integrity. Future improvements will focus on further optimizing Spark queries, refining Gold-layer modeling, and simplifying analysis workflows for end-users.
