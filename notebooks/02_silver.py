# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver: clean, standardise, deduplicate, flag
# MAGIC Non market and VAT exclusive PPR sales are flagged, not deleted. Only fully identical rows are removed.

# COMMAND ----------
spark.sql("""
CREATE OR REPLACE TABLE workspace.silver.ref_county AS
SELECT * FROM VALUES
 ('Carlow','Leinster','South-East'),('Cavan','Ulster','Border'),('Clare','Munster','Mid-West'),
 ('Cork','Munster','South-West'),('Donegal','Ulster','Border'),('Dublin','Leinster','Dublin'),
 ('Galway','Connacht','West'),('Kerry','Munster','South-West'),('Kildare','Leinster','Mid-East'),
 ('Kilkenny','Leinster','South-East'),('Laois','Leinster','Midland'),('Leitrim','Connacht','Border'),
 ('Limerick','Munster','Mid-West'),('Longford','Leinster','Midland'),('Louth','Leinster','Border'),
 ('Mayo','Connacht','West'),('Meath','Leinster','Mid-East'),('Monaghan','Ulster','Border'),
 ('Offaly','Leinster','Midland'),('Roscommon','Connacht','West'),('Sligo','Connacht','Border'),
 ('Tipperary','Munster',NULL),('Waterford','Munster','South-East'),('Westmeath','Leinster','Midland'),
 ('Wexford','Leinster','South-East'),('Wicklow','Leinster','Mid-East')
 AS t(county, province, hpm09_region)
""")

# COMMAND ----------
spark.sql("""
CREATE OR REPLACE TABLE workspace.silver.ppr_sales AS
WITH parsed AS (
  SELECT
    to_date(date_of_sale, 'dd/MM/yyyy') AS sale_date,
    trim(address) AS address,
    trim(county) AS county,
    nullif(trim(eircode), '') AS eircode,
    CAST(regexp_replace(price, '[^0-9.]', '') AS DECIMAL(12,2)) AS price_eur,
    (not_full_market_price = 'Yes') AS is_non_market,
    (vat_exclusive = 'Yes') AS is_vat_exclusive,
    CASE WHEN property_description LIKE 'New%' OR property_description LIKE '%Nua'
         THEN 'New' ELSE 'Second-hand' END AS property_condition,
    CASE
      WHEN property_size_description IS NULL THEN 'Not stated'
      WHEN property_size_description LIKE 'less than 38%'
        OR property_size_description LIKE 'n%os l% n% 38 m%adar cearnach' THEN 'Under 38 sq m'
      WHEN property_size_description LIKE '%38%125%' THEN '38 to under 125 sq m'
      WHEN property_size_description LIKE '%125%' THEN '125 sq m and over'
      ELSE 'Other'
    END AS size_band,
    _source_file, _load_date
  FROM workspace.bronze.ppr_raw
),
ranked AS (
  SELECT *, row_number() OVER (
    PARTITION BY sale_date, address, county, eircode, price_eur,
                 is_non_market, is_vat_exclusive, property_condition, size_band
    ORDER BY _source_file) AS rn
  FROM parsed
)
SELECT row_number() OVER (ORDER BY sale_date, address, price_eur) AS sale_id,
       sale_date, year(sale_date) AS sale_year, month(sale_date) AS sale_month,
       address, county, eircode, price_eur, is_non_market, is_vat_exclusive,
       property_condition, size_band, _source_file, _load_date
FROM ranked WHERE rn = 1
""")

# COMMAND ----------
spark.sql("""
CREATE OR REPLACE TABLE workspace.silver.hpm09_price_index AS
SELECT
  to_date(concat(month_code, '01'), 'yyyyMMdd') AS month_start,
  CAST(left(month_code, 4) AS INT) AS year,
  statistic AS statistic_code, statistic_label, type_label,
  trim(split_part(type_label, ' - ', 1)) AS geography,
  trim(split_part(type_label, ' - ', 2)) AS property_type,
  CASE trim(split_part(type_label, ' - ', 1))
    WHEN 'National' THEN 'national'
    WHEN 'National excluding Dublin' THEN 'national_ex_dublin'
    WHEN 'Dublin' THEN 'dublin'
    WHEN 'Dublin City' THEN 'dublin_local_authority'
    WHEN 'Dun Laoghaire-Rathdown' THEN 'dublin_local_authority'
    WHEN 'Fingal' THEN 'dublin_local_authority'
    WHEN 'South Dublin' THEN 'dublin_local_authority'
    ELSE 'region' END AS geography_level,
  CAST(nullif(value, '') AS DECIMAL(12,4)) AS value,
  unit, _source_file, _load_date
FROM workspace.bronze.cso_hpm09_raw
""")

# COMMAND ----------
spark.sql("""
CREATE OR REPLACE TABLE workspace.silver.rih02_rent AS
SELECT
  CAST(left(r.half_year_code, 4) AS INT) AS year,
  CAST(substring(r.half_year_code, 5, 1) AS INT) AS half,
  r.half_year, r.bedrooms, r.property_type, r.location_code, r.location,
  CASE WHEN c.county IS NOT NULL THEN 'county'
       WHEN r.location LIKE 'Dublin %' THEN 'dublin_postal_district'
       WHEN r.location LIKE '%, %' THEN 'town_in_county'
       WHEN r.location LIKE '% City' OR r.location LIKE '% Town' THEN 'city_or_town'
       ELSE 'other' END AS geography_level,
  CAST(nullif(r.value, '') AS DECIMAL(10,2)) AS avg_monthly_rent,
  r.unit, r._source_file, r._load_date
FROM workspace.bronze.cso_rih02_raw r
LEFT JOIN workspace.silver.ref_county c ON r.location = c.county
""")

# COMMAND ----------
spark.sql("""
CREATE OR REPLACE TABLE workspace.silver.income_county AS
SELECT CAST(year AS INT) AS year,
       replace(county, 'Co. ', '') AS county,
       statistic_label, unit,
       CAST(nullif(value, '') AS DECIMAL(14,2)) AS value,
       _source_file, _load_date
FROM workspace.bronze.cso_raa02_raw
WHERE statistic_label = 'Disposable Income per Person'
""")

# COMMAND ----------
for t in ["ref_county", "ppr_sales", "hpm09_price_index", "rih02_rent", "income_county"]:
    print(f"silver.{t:18} {spark.table('workspace.silver.' + t).count():>9,} rows")
b = spark.table("workspace.bronze.ppr_raw").count()
s = spark.table("workspace.silver.ppr_sales").count()
print("PPR bronze", b, "silver", s, "duplicates removed", b - s)
