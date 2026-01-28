from pyspark.sql.functions import *
from pyspark.sql.types import *

dbutils.widgets.text("catalog_name","priceriskanalysis")
catalog_name = dbutils.widgets.get("catalog_name")
dbutils.widgets.text("commodity","CORN")
commodity = dbutils.widgets.get("commodity")
dbutils.widgets.text("statistic","YIELD")
statistic = dbutils.widgets.get("statistic")

if statistic == "YIELD":
    bronze_path = f"/Volumes/{catalog_name}/bronze/usda_bronze/yeild/"
elif statistic == "PRODUCTION":
    bronze_path = f"/Volumes/{catalog_name}/bronze/usda_bronze/production/"
elif statistic == "AREA HARVESTED":
    bronze_path = f"/Volumes/{catalog_name}/bronze/usda_bronze/area_harvested/"
elif statistic == "PRICE RECEIVED":
    bronze_path = f"/Volumes/{catalog_name}/bronze/usda_bronze/price_received/"
else:
    raise ValueError("Unknown Statistic")

@dlt.table(
    name = "silver_usda_corn"
)
def silver_usda_corn():
    files = dbutils.fs.ls(bronze_path)
    latest_json = sorted(files)[-1].path
    df = spark.read.format("json")\
               .load(latest_json)
    df = df.select(explode(col("data")).alias("record"))
    df = df.select("record.*")
    df_silver = df.select(
    "year",
    "state_name",
    "state_alpha",
    "country_name",
    "commodity_desc",
    "statisticcat_desc",
    "Value",
    col("CV (%)").alias("cv_percent"),
    "unit_desc",
    "agg_level_desc",
    "freq_desc",
    "group_desc",
    "sector_desc",
    "prodn_practice_desc",
    "util_practice_desc",
    "reference_period_desc",
    "source_desc",
    "load_time")
    df_silver= df_silver.withColumn("Value",when(trim(col('Value')) == "(D)","0").otherwise(col('Value')))\
                      .withColumn("cv_percent",when(trim(col('cv_percent')) == "(D)","0").otherwise(col('cv_percent')))\
                      .withColumn("cv_percent",when(trim(col('cv_percent')) == "(L)","0").otherwise(col('cv_percent')))\
                      .withColumn("cv_percent",when(trim(col('cv_percent')) == "","0").otherwise(col('cv_percent')))
    df_silver = df_silver.withColumn("Value",col('Value').cast('float'))\
                      .withColumn("cv_percent",col('cv_percent').cast('float'))\
                       .withColumn("load_time",to_timestamp(col('load_time')))
    df_silver = df_silver.fillna("Not available").fillna(0)
    return df_silver

