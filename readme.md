

#  Insurance Policy Data Engineering Pipeline (Hackathon Submission)

## 1. Project Overview

This project implements a complete **end-to-end Data Engineering Pipeline** for ABC Insurance based on the provided problem statement.

Each day, ABC Insurance receives **four regional datasets**:

* East
* West
* North
* South

Each region sends daily CSV files containing **customer**, **policy**, **address**, and **premium transaction** information.

### **Hackathon Goal**

To build a professional Data Engineering solution that:

* Ingests and merges all regional + daily files
* Cleans and standardizes data into a canonical structure
* Builds a **Star Schema Data Warehouse**
* Implements **SCD Type-2** to maintain historical changes
* Generates required analytic outputs for **queries (b–f)**

---

# 2. Step 1 — Data Ingestion (Merging 4 Regional ZIP Files)

We ingest the following ZIP files:

```
Insurance_details_US_East_day.zip
Insurance_details_US_West_day.zip
Insurance_details_US_North_day.zip
Insurance_details_US_South_day.zip
```

### Processing Steps:

1. Automatically detect ZIP files
2. Extract all CSVs
3. Identify region from ZIP file name
4. Extract **snapshot day** from CSV file name
5. Add metadata columns:

| Column            | Description                         |
| ----------------- | ----------------------------------- |
| `src_region_file` | Region derived from ZIP name        |
| `snapshot_day`    | Day number extracted from file name |
| `src_file`        | Actual CSV file name                |

This produces a single consolidated **multi-region, multi-day dataset**.

---

# 3. Step 2 — Data Cleaning & Standardization

Raw files differ by region, structure, delimiter and naming.
We standardize everything into **26 canonical columns** as required.

### Major Cleaning Steps

| Cleaning Step                | Purpose                                          |
| ---------------------------- | ------------------------------------------------ |
| Auto delimiter detection     | Handle inconsistent separators                   |
| Column normalization         | Removes extra spaces, fixes naming variations    |
| Canonical column alignment   | Ensures exact 26 required fields                 |
| Customer Name reconstruction | Build Full Name if only First/Middle/Last exist  |
| Date parsing                 | Convert to clean datetime format                 |
| Numeric parsing              | Convert premium + amount fields to numeric       |
| Missing column creation      | Add missing canonical columns with nulls         |
| Row validation               | Remove rows missing `Customer ID` or `Policy ID` |
| Deduplication                | Remove duplicates across regions/days            |

### Result:

A **clean, uniform, validated dataset** that is ready for Data Warehouse loading.

---

# 4. Step 3 — Data Warehouse (Star Schema)

The core of the project is the **Star Schema**, designed to support analytical queries efficiently and professionally.

Below is the structure:

---

## Fact Table + Four Dimensions

```
                dim_customer (SCD2)
                       |
                       |
dim_policy_type -- fact_transactions -- dim_policy (SCD2)
                       |
                       |
                 dim_address (Hash NK)
```

---

# 5. Dimension Tables (Detailed)

## 5.1 dim_customer (SCD Type-2)

Tracks historical changes in:

* customer_name
* customer_segment
* marital_status
* gender
* dob

### Columns:

| Column             | Description                               |
| ------------------ | ----------------------------------------- |
| customer_sk        | Surrogate key (unique version ID)         |
| customer_id        | Natural ID from source system             |
| Tracked attributes | Name, segment, marital status, etc.       |
| start_day          | Version start snapshot                    |
| end_day            | Version end snapshot                      |
| is_current         | Whether this record is the latest version |

### Logic:

A hash is generated from tracked fields.
If today’s hash ≠ yesterday’s hash → customer attributes changed → new SCD2 version created.

---

## 5.2 dim_policy (SCD Type-2)

Policy attributes may change across snapshot days, such as:

* policy_term
* policy_name
* policy_type
* premium_amt
* start/end dates

### Natural Key:

```
policy_nk = src_region_file + "|" + policy_id
```

(prevents collisions across regions)

### SCD Logic:

Same hashing technique as dim_customer.

---

## 5.3 dim_policy_type (Static Dimension)

Stores:

* policy_type_id
* policy_type
* policy_type_desc

Referenced from both `dim_policy` and `fact_transactions`.

---

## 5.4 dim_address (Hash-Based Natural Key)

Address consists of multiple fields:

```
customer_id  
country  
region  
state_or_province  
city  
postal_code
```

We compute:

```
address_nk = MD5(customer_id|country|region|state|city|postal_code)
```

Then generate:

```
address_sk = surrogate key
```

This method guarantees:

* Uniqueness
* Stability
* Efficient joins

---

# 6. Fact Table — fact_transactions

This table represents the **daily snapshot** of every active policy.

### Columns:

| Column                    | Purpose                   |
| ------------------------- | ------------------------- |
| fact_sk                   | Primary key               |
| snapshot_day              | Day number for daily view |
| src_region_file           | Source region             |
| customer_sk               | Linked to dim_customer    |
| policy_sk                 | Linked to dim_policy      |
| policy_type_sk            | Linked to dim_policy_type |
| address_sk                | Linked to dim_address     |
| total_policy_amt          | Measures                  |
| premium_amt               | Measures                  |
| premium_amt_paid_tilldate | Measures                  |
| next_premium_dt           | Date measure              |
| actual_premium_paid_dt    | Date measure              |

---

# 7. Step 4 — Running Analytical Queries (b–f)

Using the Star Schema, the required business queries are easily solved.

---

## (b) Customers Who Changed Policy Type

Uses:

* dim_policy (SCD2)
* fact_transactions (policy snapshots)

Logic:

* For each customer, determine the latest policy type
* Scan historical snapshots backward
* Identify the **most recent different** policy type
* Produce **previous vs current** policy type report

---

## (c) Total Policy Amount by All Customers

Using:

```
fact_transactions.total_policy_amt
GROUP BY customer_sk
```

Outputs customer-level total policy exposure.

---

## (d) Total Policy Amount for Auto Policy Type

Filter:

```
policy_type = 'Auto'
```

Then run same aggregation as (c).

---

## (e) East & West Customers, Quarterly Policies, Year 2012

Filters:

* dim_address.region IN (East, West)
* dim_policy.policy_term = 'Quarterly'
* dim_policy.policy_start_dt.year = 2012

Outputs:

* Earliest policy start date
* Sum of total_policy_amt

---

## (f) Customers Whose Marital Status Changed

Using SCD2 in dim_customer:

* Each SCD version has a start_day, end_day
* When marital_status changes → new version
* Last version ends at 2099-12-31

Outputs full timeline of marital status changes.

---

# 8. Output Files

### Data Warehouse Outputs

```
dim_customer.csv
dim_address.csv
dim_policy.csv
dim_policy_type.csv
fact_transactions.csv
merged_clean_26cols.csv
```

### Analytical Query Outputs (b–f)

```
q2_b_policy_type_changed.csv
q2_c_total_policy_amt_all.csv
q2_d_total_policy_amt_auto.csv
q2_e_east_west_quarterly_2012.csv
q2_f_marital_status_changed.csv
```

---

# 9. Final Hackathon Summary 

**“We ingested four regional ZIPs, standardized all daily files into a clean 26-column schema, applied rigorous data quality rules, and built a full Star Schema Data Warehouse.
Using surrogate keys, MD5 hash keys, and SCD Type-2, we preserved history for both customers and policies.
From this warehouse, we delivered all business query outputs (b–f) accurately and efficiently.”**

