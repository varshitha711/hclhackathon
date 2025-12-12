
 **Insurance Policy Data Engineering Pipeline — Hackathon Submission**

 **1. Project Overview**

This project implements an end-to-end **Data Engineering Pipeline** for ABC Insurance.
Each day, 4 regional ZIP files (East, West, North, South) are provided with customer, policy, address, and transaction data.

Our objective:

* **Combine all regions & all days into a unified data model**
* **Clean and standardize the raw data**
* **Build a proper Data Warehouse with SCD Type-2 dimensions**
* **Use surrogate keys & hashing for record tracking**
* **Produce analytical outputs (Queries b–f)** exactly as required in the problem statement

---

 **2. Step 1 — Data Ingestion (Merging the 4 Region ZIP Files)**

We ingest the following ZIP files:

```
Insurance_details_US_East_day.zip
Insurance_details_US_West_day.zip
Insurance_details_US_North_day.zip
Insurance_details_US_South_day.zip
```
 **Process:**

* Automatically detect these ZIP files from `/content`.
* Extract all CSVs inside them.
* Detect the **region** and **snapshot day** from each filename.
* Combine everything into a single dataset with metadata:

| Column            | Purpose                                 |
| ----------------- | --------------------------------------- |
| `src_region_file` | Region of the record based on file name |
| `snapshot_day`    | Day number extracted from file name     |
| `src_file`        | Specific CSV file name                  |

This results in a single consolidated **multi-region, multi-day dataset**, ready for cleaning.

---

**3. Step 2 — Data Cleaning & Standardization**

Since each region may have different column names, formats, or delimiters, **cleaning is critical**.

 ✔ **Delimiter Detection**

Each CSV may use:
`,`, `|`, `;`, `\t`

We auto-detect the correct separator.

### ✔ **Column Standardization to 26 Canonical Columns**

We map and create all required 26 fields, including:

* Customer: ID, Name, Segment, Marital Status, Gender, DOB
* Policy: ID, Type, Term, Start/End Dates, Premium
* Address: Country, Region, State, City, Postal Code
* Financial: Total Policy Amount, Premium Paid Till Date

### ✔ **Data Cleaning Performed**

| Cleaning Step                    | Purpose                                                                            |
| -------------------------------- | ---------------------------------------------------------------------------------- |
| **Whitespace normalization**     | Removes inconsistent spaces/tabs from text fields                                  |
| **Case standardization**         | Lowercase/trim for uniformity                                                      |
| **Date parsing**                 | Convert all date fields to proper datetime format                                  |
| **Numeric coercion**             | Ensure numeric fields (premium, amount) are valid numbers                          |
| **Customer Name reconstruction** | If a file contains First/Middle/Last but not "Customer Name", we rebuild correctly |
| **Missing column creation**      | Any missing canonical column is added with NaN                                     |
| **Removal of invalid rows**      | Remove rows missing `Customer ID` or `Policy_Id`                                   |
| **Deduplication**                | Remove exact duplicates                                                            |

### Outcome:

A fully cleaned, standardized dataset with **consistent schema across all regions and all days**.

---

## **4. Step 3 — Data Warehouse Construction**

We now convert the cleaned dataset into a **proper dimensional model** with surrogate keys and historical tracking.

### **4.1 Surrogate Keys (SKs)**

We do not use natural IDs directly.
Instead, we generate SKs using `pd.factorize()`, which ensures:

* Integer keys
* Consistent ordering
* Compact dimensions

Example:

```
customer_sk, policy_sk, policy_type_sk, address_sk
```

---

## **4.2 SCD Type-2 Dimensions (Slowly Changing Dimension)**

We maintain history of **Customer** and **Policy** attributes across days.

### **How SCD Type-2 Works Here**

For each entity (customer or policy):

1. Sort by `snapshot_day`
2. Convert all tracked fields to string
3. Generate a **hash** for the combination of tracked fields
4. Compare with the previous snapshot
5. If hash differs → values changed → **create new version**
6. Assign:

   * `start_day`
   * `end_day` (next version’s start_day)
   * `is_current`

### **Why use hashing?**

Because it reliably detects changes across multiple columns without manually comparing each field.

---

## **4.3 Dimensions Created**

### **a) dim_customer (SCD2)**

Tracks changes in:

* customer_name
* customer_segment
* marital_status
* gender
* dob

### **b) dim_policy (SCD2)**

Tracks changes in:

* policy_name
* policy_term
* policy_start_dt / policy_end_dt
* premium_amt
* policy_type

### **c) dim_policy_type**

Static dimension.

### **d) dim_address (Hash NK)**

Address_NK is generated using MD5 hash:

```
MD5(customer_id | country | region | state | city | postal_code)
```

This ensures:

* Uniqueness
* Consistency
* Fast joins

---

## **4.4 Fact Table: fact_transactions**

Contains:

* customer_sk
* policy_sk
* policy_type_sk
* address_sk
* total_policy_amt
* premium_amt
* premium_paid_tilldate
* next_premium_dt
* actual_premium_paid_dt
* snapshot_day

This table stores the **state of each policy snapshot** per day.

---

---

# **5. Step 4 — Analytical Queries (b–f)**

After building the DWH, we solved the business queries provided in the problem statement.

---

## **(b) Customers Who Changed Policy Type**

### Approach:

* Build customer-day snapshots
* Identify latest policy type
* Scan backwards to find previous different policy type

### Output:

Shows for each customer:

* Previous Policy Type
* Current Policy Type
* Policy Type ID and Description

This detects transitions like:

```
Life → Auto
Health → Life
```

---

## **(c) Total Policy Amount by All Customers**

Using the latest snapshot per policy:

* Group by customer
* Sum `total_policy_amt`
* Add `Region = 'All'`

This gives the total exposure per customer.

---

## **(d) Total Policy Amount for Auto Policies**

Same as (c) but filter:

```
policy_type = 'Auto'
```

Used to analyze performance of Auto portfolio.

---

## **(e) East & West Customers with Quarterly Term in 2012**

Filter criteria:

* region = East or West
* policy_term = Quarterly
* policy_start_year = 2012

Then group & compute:

* Earliest policy start date
* Total policy amount

---

## **(f) Customers Whose Marital Status Changed**

We track marital_status across days using SCD-like logic:

* When status changes → close old record
* Next record starts new range
* Last record ends at **2099-12-31**

Output includes:

* Start_Dt_Marital_Status
* End_Dt_Marital_Status
* Marital status for each time window

---

# **6. Directory Outputs**

### **Data Warehouse Outputs**

Located in:

```
/content/output_insurance_dwh/
```

Files:

* dim_customer.csv
* dim_address.csv
* dim_policy.csv
* dim_policy_type.csv
* fact_transactions.csv
* merged_clean_26cols.csv

---

### **Analytical Query Outputs**

Located in:

```
/content/q2_outputs/
```

Files:

* q2_b_policy_type_changed.csv
* q2_c_total_policy_amt_all.csv
* q2_d_total_policy_amt_auto.csv
* q2_e_east_west_quarterly_2012.csv
* q2_f_marital_status_changed.csv

---

# **7. Summary (Use This in Your Hackathon Presentation)**

**“We ingested four regional ZIP files, merged and cleaned all CSVs into a uniform 26-column schema, and built a full enterprise-grade Data Warehouse.
We used surrogate keys and hash-based SCD Type-2 to track historical changes for customers and policies.
Finally, using this DWH, we delivered all required analytical reports (b–f) accurately and efficiently.”**



Just tell me!
