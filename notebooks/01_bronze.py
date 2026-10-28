# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze: download raw files and load them as received
# MAGIC Raw files go to a Unity Catalog Volume in an ingest_date folder. Bronze tables keep every column as a string, plus `_source_file` and `_load_date`.

# COMMAND ----------
import datetime, urllib.request, zipfile
from urllib.parse import urljoin
from pyspark.sql import functions as F

VOL = "/Volumes/workspace/bronze/raw_files"
today = datetime.date.today().isoformat()

# COMMAND ----------
def download(url, folder, filename):
    dbutils.fs.mkdirs(folder)
    path = f"{folder}/{filename}"
    urllib.request.urlretrieve(url, path)
    return path

CSO = "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/{}/CSV/1.0/en"
hpm09_path = download(CSO.format("HPM09"), f"{VOL}/cso/hpm09/ingest_date={today}", "hpm09.csv")
rih02_path = download(CSO.format("RIH02"), f"{VOL}/cso/rih02/ingest_date={today}", "rih02.csv")
raa02_path = download(CSO.format("RAA02"), f"{VOL}/cso/raa02/ingest_date={today}", "raa02.csv")

ppr_url = urljoin("https://www.propertypriceregister.ie/",
                  "/website/npsra/ppr/npsra-ppr.nsf/Downloads/PPR-ALL.zip/$FILE/PPR-ALL.zip")
ppr_folder = f"{VOL}/ppr/ingest_date={today}"
ppr_zip = download(ppr_url, ppr_folder, "PPR-ALL.zip")
with zipfile.ZipFile(ppr_zip) as z:
    z.extractall(ppr_folder)
ppr_path = f"{ppr_folder}/PPR-ALL.csv"

# COMMAND ----------
def load_bronze(path, columns, table, encoding="UTF-8", quoted=False):
    reader = spark.read.format("csv").option("header", "true").option("encoding", encoding)
    if quoted:
        reader = reader.option("quote", '"').option("escape", '"')
    df = reader.load(path).toDF(*columns)
    df = (df.withColumn("_source_file", F.lit(path))
            .withColumn("_load_date", F.lit(today)))
    df.write.mode("overwrite").saveAsTable(f"workspace.bronze.{table}")
    rows = spark.table(f"workspace.bronze.{table}").count()
    lines = sum(1 for _ in open(path, "rb")) - 1
    print(f"{table:15} table rows {rows:>8,}  file data lines {lines:>8,}  {'MATCH' if rows == lines else 'MISMATCH'}")
    if rows != lines:
        raise Exception(f"Bronze reconciliation failed for {table}")

ppr_cols = ["date_of_sale", "address", "county", "eircode", "price", "not_full_market_price",
            "vat_exclusive", "property_description", "property_size_description"]
hpm09_cols = ["statistic", "statistic_label", "month_code", "month", "type_code", "type_label", "unit", "value"]
rih02_cols = ["statistic", "statistic_label", "half_year_code", "half_year", "bedrooms_code", "bedrooms",
              "property_type_code", "property_type", "location_code", "location", "unit", "value"]
raa02_cols = ["statistic", "statistic_label", "year_code", "year", "county_code", "county", "unit", "value"]

load_bronze(ppr_path, ppr_cols, "ppr_raw", encoding="windows-1252", quoted=True)
load_bronze(hpm09_path, hpm09_cols, "cso_hpm09_raw")
load_bronze(rih02_path, rih02_cols, "cso_rih02_raw")
load_bronze(raa02_path, raa02_cols, "cso_raa02_raw")
