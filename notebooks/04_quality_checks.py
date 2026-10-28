# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Quality checks: data quality rules plus independent answer key
# MAGIC Rules write to `workspace.silver.dq_log`. The task fails if any error severity rule fails or the answer key finds a mismatch.

# COMMAND ----------
import datetime
run_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
spark.sql("""CREATE TABLE IF NOT EXISTS workspace.silver.dq_log (
  run_id STRING, run_ts TIMESTAMP, table_name STRING, check_name STRING, severity STRING,
  description STRING, rows_checked BIGINT, rows_failed BIGINT, status STRING)""")
errors = []

def dq(table, name, severity, desc, fail_sql, scope="1=1"):
    t = spark.table(table).where(scope)
    total, failed = t.count(), t.where(fail_sql).count()
    status = "PASS" if failed == 0 else {"error": "FAIL", "warn": "WARN", "info": "INFO"}[severity]
    spark.sql(f"""INSERT INTO workspace.silver.dq_log VALUES
      ('{run_id}', current_timestamp(), '{table}', '{name}', '{severity}', '{desc}', {total}, {failed}, '{status}')""")
    print(f"{status:5} {name:28} failed {failed:>8,} of {total:,}")
    if status == "FAIL":
        errors.append(name)

counties = [r.county for r in spark.table("workspace.silver.ref_county").collect()]
county_list = ",".join(f"'{c}'" for c in counties)

# COMMAND ----------
p = "workspace.silver.ppr_sales"
dq(p, "ppr_price_not_null", "error", "price_eur must not be null", "price_eur IS NULL")
dq(p, "ppr_price_positive", "error", "price_eur must be greater than 0", "price_eur <= 0")
dq(p, "ppr_price_min_5000", "warn", "price_eur below 5000 is suspicious", "price_eur < 5000")
dq(p, "ppr_price_max_20m", "warn", "price_eur above 20 million is suspicious", "price_eur > 20000000")
dq(p, "ppr_date_not_null", "error", "sale_date must parse", "sale_date IS NULL")
dq(p, "ppr_date_not_future", "error", "sale_date must not be in the future", "sale_date > current_date()")
dq(p, "ppr_date_since_2010", "error", "sale_date on or after 2010-01-01", "sale_date < DATE'2010-01-01'")
dq(p, "ppr_address_not_blank", "error", "address must not be blank", "address IS NULL OR trim(address) = ''")
dq(p, "ppr_county_valid", "error", "county must be one of 26 reference counties", f"county NOT IN ({county_list})")
dq(p, "ppr_condition_known", "error", "property_condition is New or Second-hand", "property_condition NOT IN ('New','Second-hand')")
dq(p, "ppr_size_band_known", "error", "size_band must not be Other", "size_band = 'Other'")
dq(p, "ppr_non_market_flagged", "info", "non market sales flagged not deleted", "is_non_market")
dq(p, "ppr_vat_exclusive_flagged", "info", "VAT exclusive sales flagged not adjusted", "is_vat_exclusive")

h = "workspace.silver.hpm09_price_index"
idx = "unit = 'Base 2015=100'"
dq(h, "hpm09_index_not_null", "warn", "index missing for some geography and month combinations", "value IS NULL", idx)
dq(h, "hpm09_index_positive", "error", "index value must be greater than 0 when present", "value <= 0", idx)
dq(h, "hpm09_month_not_null", "error", "month_start must parse", "month_start IS NULL")
dq(h, "hpm09_geography_parsed", "error", "geography and property_type parsed", "geography IS NULL OR property_type IS NULL")
dq(h, "hpm09_unit_not_null", "error", "unit must not be null", "unit IS NULL")
dq(h, "hpm09_change_range", "warn", "percentage change within minus 60 to 150", "value < -60 OR value > 150", "unit <> 'Base 2015=100'")

r = "workspace.silver.rih02_rent"
dq(r, "rih02_period_valid", "error", "year and half must parse", "year IS NULL OR half NOT IN (1,2)")
dq(r, "rih02_rent_missing", "info", "rent suppressed or missing in source", "avg_monthly_rent IS NULL")
dq(r, "rih02_rent_positive", "error", "rent must be greater than 0 when present", "avg_monthly_rent <= 0")
dq(r, "rih02_geography_known", "error", "geography_level must be assigned", "geography_level = 'other'")

i = "workspace.silver.income_county"
dq(i, "income_value_not_null", "error", "income value must not be null", "value IS NULL")
dq(i, "income_value_positive", "error", "income must be greater than 0", "value <= 0")
dq(i, "income_county_valid", "error", "county is a reference county or Ireland", f"county NOT IN ({county_list},'Ireland')")
dq(i, "income_year_range", "error", "year between 2000 and 2025", "year < 2000 OR year > 2025")

# COMMAND ----------
# Independent answer key: recomputed from Bronze with its own price parsing and its own definition of a duplicate.
spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.answer_key_ppr AS
WITH raw AS (
  SELECT DISTINCT date_of_sale, address, county, eircode, price, not_full_market_price,
         vat_exclusive, property_description, property_size_description
  FROM workspace.bronze.ppr_raw
)
SELECT year(to_date(date_of_sale, 'dd/MM/yyyy')) AS yr, trim(county) AS county,
       count(*) AS sales,
       sum(CASE WHEN not_full_market_price = 'No' THEN 1 ELSE 0 END) AS market_sales,
       percentile(CASE WHEN not_full_market_price = 'No'
                  THEN CAST(replace(substring(price, 2), ',', '') AS DOUBLE) END, 0.5) AS median_price
FROM raw GROUP BY 1, 2
""")
spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.gold_check_ppr AS
SELECT d.year AS yr, c.county_name AS county, count(*) AS sales,
       sum(CASE WHEN f.is_non_market THEN 0 ELSE 1 END) AS market_sales,
       percentile(CASE WHEN NOT f.is_non_market THEN CAST(f.price_eur AS DOUBLE) END, 0.5) AS median_price
FROM workspace.gold.fact_sale f
JOIN workspace.gold.dim_date d ON f.date_key = d.date_key
JOIN workspace.gold.dim_county c ON f.county_key = c.county_key
GROUP BY 1, 2
""")
cmp = spark.sql("""
SELECT coalesce(a.yr, g.yr) AS yr, coalesce(a.county, g.county) AS county,
       a.sales AS key_sales, g.sales AS gold_sales,
       a.market_sales AS key_market, g.market_sales AS gold_market,
       a.median_price AS key_median, g.median_price AS gold_median
FROM workspace.gold.answer_key_ppr a
FULL OUTER JOIN workspace.gold.gold_check_ppr g ON a.yr = g.yr AND a.county = g.county
""")
cmp.createOrReplaceTempView("cmp")
total = cmp.count()
bad = spark.sql("""SELECT * FROM cmp WHERE key_sales IS NULL OR gold_sales IS NULL
  OR key_sales <> gold_sales OR key_market <> gold_market
  OR abs(key_median - gold_median) > 0.01""")
n_bad = bad.count()
print(f"answer key: county-year cells compared {total}, mismatched {n_bad}")
bad.orderBy("yr", "county").show(40, truncate=False)
spark.sql(f"""INSERT INTO workspace.silver.dq_log VALUES
  ('{run_id}', current_timestamp(), 'workspace.gold.fact_sale', 'answer_key_ppr_county_year', 'error',
   'sales, market sales and median price per county and year equal independent SQL from Bronze',
   {total}, {n_bad}, '{"PASS" if n_bad == 0 else "FAIL"}')""")
if n_bad:
    errors.append("answer_key_ppr_county_year")

# COMMAND ----------
spark.sql(f"""SELECT status, count(*) AS n FROM workspace.silver.dq_log WHERE run_id = '{run_id}' GROUP BY status""").show()
if errors:
    raise Exception(f"Quality checks failed: {errors}")
print("All error severity checks passed")
