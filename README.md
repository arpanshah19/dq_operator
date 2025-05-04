
# 🧪 Airflow Data Quality Operator

A streamlined solution to define and run data quality (DQ) checks in **Airflow** using configurable SQL rules. It supports any SQL engine (e.g., **Snowflake**, **Databricks**, **SQL Server**) and integrates seamlessly with orchestration workflows or standalone Python scripts.

---

## ✨ Highlights

- ⚙️ Define DQ checks using a simple `dq_config.json`
- 🏗️ Auto-generates Airflow `TaskGroup` and tasks per rule
- 🔌 Works with any SQL engine via a custom `sql_executor`
- 🧾 Logs results in a centralized DQ summary table
- 📏 Supports multiple check types: `EXCEPT`, `RECON`, `TREND`, `NULL`, and `CUSTOM`

---

## 🚀 Quick Example: Airflow Integration

```python
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from dq_taskgroup import create_dq_taskgroup
from datetime import datetime

def snowflake_executor(sql: str):
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    hook.run(sql)

with DAG("dq_sales_dag", start_date=datetime(2024, 1, 1), schedule_interval=None, catchup=False) as dag:
    dq_group = create_dq_taskgroup(
        group_id="sales_dq_checks",
        sql_executor=snowflake_executor,
        dq_table="DQ_SUMMARY_TABLE"
    )
```

---

## 🧩 Components Overview

### `DataQualityOperator`

Runs a single DQ check:
- Builds SQL using `DQQueryBuilder`
- Executes SQL via the provided executor
- Logs result in the summary table

### `create_dq_taskgroup()`

- Parses `dq_config.json`
- Creates an Airflow task per DQ rule
- Wraps logic inside `DataQualityOperator`

### `DQQueryBuilder()`

- Create a query to execute for dataquality check
- Direct SQL generation for `TREND`, `RECON`, `EXCEPT`, `NULL`, and `CUSTOM`
- Clean integration into pipelines with or without orchestration tools

---

## ⚙️ The DQ Framework (With or Without Airflow)

A Python class to generate and optionally execute SQL-based DQ checks in **Airflow**, **Databricks**, or any Python-based workflow.

Supports:
- Easy method calls with overloads: `DQQueryBuilder.check(...)`
- Direct SQL generation for `TREND`, `RECON`, `EXCEPT`, `NULL`, and `CUSTOM`
- Clean integration into pipelines with or without orchestration tools

---

## ✅ Check Types

| Type      | Description                                                                 |
|-----------|-----------------------------------------------------------------------------|
| `EXCEPT`  | Compares row-level differences across two datasets                         |
| `RECON`   | Validates aggregate metrics (e.g. COUNT/SUM) between sources               |
| `TREND`   | Compares current data against historical trends                             |
| `NULL`    | Checks for missing/null values in specific columns                          |
| `CUSTOM`  | Fully custom queries for advanced or domain-specific checks                 |

---

## 🧭 Usage Examples

### Airflow + Snowflake

```python
from dq_framework import DataQuality

def generate_dq_query():
    return DQQueryBuilder.check(
        check_type='RECON',
        check_name='Sales aggregation match',
        main_table='sales_fact',
        compare_table='sales_summary',
        agg_function='SUM',
        date_column='txn_date',
        business_date='{{ ds_nodash }}'
    )
```

### Databricks Notebook

```python
from dq_framework import DataQuality

dq_sql = DQQueryBuilder.check(
    check_type='TREND',
    check_name='Login anomaly detection',
    source_query='SELECT COUNT(*) AS value FROM login_events WHERE login_date = "20250101"',
    target_query='''
        SELECT AVG(cnt) AS value FROM (
            SELECT COUNT(*) AS cnt FROM login_events 
            WHERE login_date BETWEEN "20241225" AND "20241231" 
            GROUP BY login_date
        )
    ''',
    column_name='user_id',
    threshhold=10,
    business_date='20250101'
)

spark.sql(dq_sql)
```

---

## 📦 Sample Output SQL

```sql
INSERT INTO SUMMARY_DQ_TABLE
SELECT
    'RECON' AS check_type,
    'Sales aggregation match' AS check_name,
    ...
```

---

## 🛠 DQ Summary Table Schema

Centralized storage for DQ results. Auditable and consistent.

```sql
CREATE TABLE SUMMARY_DQ_TABLE (
    check_type       STRING      NOT NULL, 
    check_name       STRING      NOT NULL, 
    main_table       STRING,
    compare_table    STRING, 
    column_name      STRING,
    status           STRING      NOT NULL,
    analysis         STRING,
    business_date    STRING      NOT NULL,
    row_created_ts   TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP, 
    row_created_dt   DATE        NOT NULL DEFAULT CURRENT_DATE   
);
```

### Recommended Columns

| Column Name      | Description                                           |
|------------------|-------------------------------------------------------|
| `check_type`     | DQ type: `RECON`, `EXCEPT`, `NULL`, etc.              |
| `check_name`     | Unique identifier for the check                       |
| `status`         | Result status (`Passed`, `Failed`)                    |
| `analysis`       | Summary insight or message from the check             |
| `business_date`  | The logical date the check pertains to                |
| `row_created_ts` | System timestamp of record creation                   |
| `row_created_dt` | System date of record creation                        |

---

## ✅ Example Entries

| check_type | check_name        | main_table   | compare_table | column_name | status | analysis                          | business_date |
|------------|-------------------|--------------|----------------|--------------|--------|-----------------------------------|----------------|
| NULL       | Check for nulls   | customer     |                | customer_id  | Failed | 12 nulls found                    | 20240501       |
| RECON      | Count match       | orders       | orders_archive |              | Passed | Main = 1250, Compare = 1250       | 20240501       |
| EXCEPT     | Order mismatches  | orders       | backup_orders  | order_id     | Failed | 14 differing rows                 | 20240501       |
| TREND      | Volume trend      | orders       | orders         | order_count  | Passed | Current = 200, Previous = 180     | 20240501       |

---

## 🔮 What’s Next

Planned enhancements:

- [ ] Automatic query submission/execution layer
- [ ] Alerts via email or Slack on check failures
- [ ] Visual trend reporting for dashboards
- [ ] Support for nested/semi-structured data checks

---

## 📚 References

- [Case Study: Real-time DQ Framework](https://valensdatalabs.com/case-studies/strengthening-dashboard-integrity-with-real-time-data-quality-framework/)
