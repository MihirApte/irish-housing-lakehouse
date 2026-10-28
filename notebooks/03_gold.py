# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold: star schema plus analytics tables
# MAGIC `fact_sale` grain: one row per registered sale after removing fully identical duplicate rows.

# COMMAND ----------
spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.dim_date AS
SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key, d AS date,
       year(d) AS year, quarter(d) AS quarter, month(d) AS month_number,
       date_format(d, 'MMMM') AS month_name, trunc(d, 'MM') AS month_start,
       CASE WHEN month(d) <= 6 THEN 1 ELSE 2 END AS half,
       concat(year(d), ' H', CASE WHEN month(d) <= 6 THEN 1 ELSE 2 END) AS year_half,
       dayofweek(d) IN (1, 7) AS is_weekend
FROM (SELECT explode(sequence(DATE'2005-01-01', DATE'2026-12-31', INTERVAL 1 DAY)) AS d)
""")
spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.dim_geography AS
SELECT row_number() OVER (ORDER BY geography) AS geography_key, geography, geography_level
FROM (SELECT DISTINCT geography, geography_level FROM workspace.silver.hpm09_price_index)
""")
spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.dim_county AS
SELECT row_number() OVER (ORDER BY c.county) AS county_key, c.county AS county_name,
       c.province, c.hpm09_region, c.county = 'Dublin' AS is_dublin,
       g.geography_key AS index_geography_key
FROM workspace.silver.ref_county c
LEFT JOIN workspace.gold.dim_geography g
  ON g.geography = c.hpm09_region AND g.geography_level IN ('region', 'dublin')
""")
spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.dim_property_type AS
SELECT row_number() OVER (ORDER BY property_condition, size_band) AS property_type_key,
       property_condition, size_band
FROM (SELECT DISTINCT property_condition, size_band FROM workspace.silver.ppr_sales)
""")

# COMMAND ----------
spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.fact_sale AS
SELECT s.sale_id, CAST(date_format(s.sale_date, 'yyyyMMdd') AS INT) AS date_key,
       c.county_key, p.property_type_key, s.address, s.eircode, s.price_eur,
       s.is_non_market, s.is_vat_exclusive
FROM workspace.silver.ppr_sales s
JOIN workspace.gold.dim_county c ON s.county = c.county_name
JOIN workspace.gold.dim_property_type p
  ON s.property_condition = p.property_condition AND s.size_band = p.size_band
""")
spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.fact_price_index AS
SELECT CAST(date_format(i.month_start, 'yyyyMMdd') AS INT) AS date_key, g.geography_key,
       i.property_type, i.statistic_code, i.statistic_label, i.unit, i.value
FROM workspace.silver.hpm09_price_index i
JOIN workspace.gold.dim_geography g
  ON i.geography = g.geography AND i.geography_level = g.geography_level
""")
spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.fact_rent AS
SELECT CAST(r.year * 10000 + CASE WHEN r.half = 1 THEN 101 ELSE 701 END AS INT) AS date_key,
       c.county_key, r.bedrooms, r.property_type, r.avg_monthly_rent
FROM workspace.silver.rih02_rent r
JOIN workspace.gold.dim_county c ON r.location = c.county_name
WHERE r.geography_level = 'county' AND r.avg_monthly_rent IS NOT NULL
""")
spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.fact_income AS
SELECT i.year * 10000 + 101 AS date_key, i.year, c.county_key,
       i.value AS disposable_income_per_person_eur
FROM workspace.silver.income_county i
JOIN workspace.gold.dim_county c ON i.county = c.county_name
""")

# COMMAND ----------
spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.county_year_affordability AS
WITH sales AS (
  SELECT f.county_key, d.year, percentile(CAST(f.price_eur AS DOUBLE), 0.5) AS median_price, count(*) AS market_sales
  FROM workspace.gold.fact_sale f JOIN workspace.gold.dim_date d ON f.date_key = d.date_key
  WHERE NOT f.is_non_market GROUP BY 1, 2),
rent AS (
  SELECT r.county_key, d.year, avg(r.avg_monthly_rent) AS avg_monthly_rent
  FROM workspace.gold.fact_rent r JOIN workspace.gold.dim_date d ON r.date_key = d.date_key
  WHERE r.bedrooms = 'All bedrooms' AND r.property_type = 'All property types' GROUP BY 1, 2)
SELECT s.county_key, s.year, s.median_price, s.market_sales,
       i.disposable_income_per_person_eur, r.avg_monthly_rent,
       s.median_price / i.disposable_income_per_person_eur AS price_to_income,
       s.median_price / (r.avg_monthly_rent * 12) AS price_to_rent
FROM sales s
LEFT JOIN workspace.gold.fact_income i ON s.county_key = i.county_key AND s.year = i.year
LEFT JOIN rent r ON s.county_key = r.county_key AND s.year = r.year
""")
spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.sale_outliers AS
WITH base AS (
  SELECT f.sale_id, f.county_key, d.year, p.property_condition, f.price_eur, ln(CAST(f.price_eur AS DOUBLE)) AS lp
  FROM workspace.gold.fact_sale f
  JOIN workspace.gold.dim_date d ON f.date_key = d.date_key
  JOIN workspace.gold.dim_property_type p ON f.property_type_key = p.property_type_key
  WHERE NOT f.is_non_market AND f.price_eur > 0),
stats AS (
  SELECT county_key, year, property_condition,
         percentile(lp, 0.25) AS q1, percentile(lp, 0.75) AS q3, count(*) AS n
  FROM base GROUP BY 1, 2, 3 HAVING count(*) >= 30)
SELECT b.sale_id, b.county_key, b.year, b.property_condition, b.price_eur,
       exp(s.q1 + 0.5 * (s.q3 - s.q1)) AS typical_price_eur,
       CASE WHEN b.lp > s.q3 + 3 * (s.q3 - s.q1) THEN 'high' ELSE 'low' END AS direction
FROM base b JOIN stats s ON b.county_key = s.county_key AND b.year = s.year
  AND b.property_condition = s.property_condition
WHERE b.lp > s.q3 + 3 * (s.q3 - s.q1) OR b.lp < s.q1 - 3 * (s.q3 - s.q1)
""")

# COMMAND ----------
pairs = [
 ("fact_sale", "SELECT count(*) FROM workspace.silver.ppr_sales"),
 ("fact_price_index", "SELECT count(*) FROM workspace.silver.hpm09_price_index"),
 ("fact_rent", "SELECT count(*) FROM workspace.silver.rih02_rent WHERE geography_level='county' AND avg_monthly_rent IS NOT NULL"),
 ("fact_income", "SELECT count(*) FROM workspace.silver.income_county WHERE county <> 'Ireland'"),
]
bad = []
for t, q in pairs:
    g = spark.table(f"workspace.gold.{t}").count()
    s = spark.sql(q).collect()[0][0]
    print(f"{t:17} gold {g:>8,}  silver expected {s:>8,}  {'MATCH' if g == s else 'MISMATCH'}")
    if g != s:
        bad.append(t)
if bad:
    raise Exception(f"Gold reconciliation failed for {bad}")
