#%%
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.functions as F
import pandas as pd
import duckdb
from pyspark.sql import Window
from pyspark.sql import functions as F

#%%
def get_spark_session(app_name):
    """Centralized Spark Session builder to avoid resource conflicts."""
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "5") \
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:3.5.1_0.20.4") \
        .config("spark.driver.memory", "3g") \
        .config("spark.executor.memory", "3g") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "5000") \
        .getOrCreate()


#%%
# ==========================================
# 0. CORE CLEANING & PRE-AGGREGATION
# ==========================================

#%%
def core_cleaning_preaggretion_and_gold_layer_creation():
# Create a clean, exact-match district roll-up of your charging infrastructure data
    print("\n🚀 Initializing Distributed Spark Engine for Phase 3: Gold Layer...")
    spark = get_spark_session("vahan_gold_pipeline")

    IS_DOCKER = os.path.exists('/.dockerenv') or os.environ.get('AIRFLOW_HOME') is not None

# 2. Compute the correct root directory dynamically
    if IS_DOCKER:
    # Inside Docker, everything sits right inside the opt path
      base_dir = "/opt/airflow"
    else:
    # On Windows, your code uses the parent file layout structure
      base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "EVMLProject")) 
    #base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    silver_path_charging = os.path.join(base_dir, "data", "2_silver","charging_master_parquet")
    silver_path_vahan = os.path.join(base_dir, "data", "2_silver","vahan_master_parquet")
    silver_path_charging_agg = os.path.join(base_dir, "data", "2_silver","charging_agg_master_parquet")
    df_charging = spark.read.parquet(silver_path_charging)
    df_charging_Agg = spark.read.parquet(silver_path_charging_agg)
    df_vahan_silver = spark.read.parquet(silver_path_vahan)
    
    df_charging_district_agg = df_charging_Agg \
    .groupBy("state", "district_v1") \
    .agg(
        F.sum("Total_Charging_Stations_Built").alias("total_charging_stations_built_district_level"),
        F.sum("sum_total_conn").alias("sum_total_conn_d_level")
    )
    keywords = ["rto", "uo", "rta"]
   
    word_pattern = r"(?i)\b(" + "|".join(keywords) + r")\b"
  
    def clean_geo_text(col_name):
    # Returns lowercase string with target words, spaces, commas, and periods removed
      return F.lower(
        F.regexp_replace(
            F.regexp_replace(col_name, word_pattern, ""), 
            r"[\s,.]", ""
          )
       )

# Fetch preceding metrics using lag windows to build acceleration features
    df_vahan_staged = df_vahan_silver.select(F.col('state').alias('v_state'),
                                      F.col('district').alias('v_district'),
                                      'data_year',
                                      (F.col('PURE EV') + F.col('ELECTRIC(BOV)')).alias('ev_sales'),
                                      F.col('Total_Sales').alias('total_sales'),
                                      ).withColumn('v_dist_clean',clean_geo_text(F.col('v_district')))
    #print("df vahan staged print")  
    #df_vahan_staged.show(5) 
    district_time_window = Window.partitionBy("v_state", "v_district").orderBy("data_year")
    df_vahan_with_lag = df_vahan_staged \
    .withColumn("prev_total_sales", F.lag("total_sales", 1).over(district_time_window)) \
    .withColumn("prev_ev_sales", F.lag("ev_sales", 1).over(district_time_window))
    #df_vahan_with_lag.filter(F.col('v_district') == 'madurantagam uo' ).show(5)
# Derive mathematically safe YoY percentage growth rates and target classifications
    df_vahan_intermediate = df_vahan_with_lag \
    .withColumn("total_sales_yoy_growth_pct", 
                F.when(F.col("prev_total_sales") > 0, 
                       ((F.col("total_sales") - F.col("prev_total_sales")) / F.col("prev_total_sales")) * 100).otherwise(0.0)) \
    .withColumn("ev_sales_yoy_growth_pct", 
                F.when(F.col("prev_ev_sales") > 0, 
                       ((F.col("ev_sales") - F.col("prev_ev_sales")) / F.col("prev_ev_sales")) * 100).otherwise(0.0)) \
    .withColumn("ev_share_pct", F.when(F.col("total_sales") > 0, (F.col("ev_sales") / F.col("total_sales")) * 100).otherwise(0.0)) \
    
    df_vahan_metrics_intermediate = df_vahan_intermediate \
        .withColumn("is_high_adoption_zone", F.expr("""
            CASE 
                WHEN data_year = 2021 AND ev_share_pct >= 2.0 THEN 1
                WHEN data_year = 2022 AND ev_share_pct >= 4.0 THEN 1
                WHEN data_year = 2023 AND ev_share_pct >= 6.0 THEN 1
                WHEN data_year = 2024 AND ev_share_pct >= 8.0 THEN 1
                WHEN data_year IN (2025, 2026) AND ev_share_pct >= 10.0 THEN 1
                ELSE 0 
            END
        """)) \
        .withColumn("unique_district", F.concat_ws("_", F.lower(F.col("v_state")), F.lower(F.col("v_district"))))
    unique_district_window_II = (
    Window.partitionBy("unique_district")
    .orderBy("data_year")
    .rowsBetween(Window.unboundedPreceding, -1)
     )
    state_window = Window.partitionBy("v_state")
    
    
    df_vahan_metrics = (df_vahan_metrics_intermediate.withColumn("state_ev_avg_pct", F.avg(F.when(F.col('data_year') <= 2025, F.col('ev_share_pct'))).over(state_window))
                        .withColumn("district_encoded",F.coalesce(F.avg(F.col('ev_share_pct')).over(unique_district_window_II),F.col('state_ev_avg_pct'))) )
    #df_vahan_metrics.filter(F.col('v_district') == 'madurantagam uo' ).show(5)
# ==========================================
# 1. GOLD TABLE: HISTORICAL TRAINING DATA (2021-2025)
# ==========================================
# Keep it free of static charger features so the model doesn't learn false rules
    df_gold_market_training = df_vahan_metrics \
    .filter(F.col("data_year") < 2026) \
    .select(
        "data_year",
        F.col("v_state").alias("state"),
        F.col("v_district").alias("district"),
        "total_sales",
        "ev_sales",
        F.round("total_sales_yoy_growth_pct", 2).alias("total_sales_yoy_growth_pct"),
        F.round("ev_sales_yoy_growth_pct", 2).alias("ev_sales_yoy_growth_pct"),
        F.round("ev_share_pct", 2).alias("ev_share_pct"),
        "district_encoded",
        "state_ev_avg_pct",
        "unique_district"
    )
    #print("df gold marketing training debug")
    #df_gold_market_training.filter(F.col('is_high_adoption_zone') == 1).show(5)
    #df_gold_market_training.filter(F.col('is_high_adoption_zone') == 1).groupBy('state','district').agg(F.count("*")).alias("counts").show()
# ==========================================
# 2. GOLD TABLE: CURRENT INFERENCE SNAPSHOT (Year 2026)
# ==========================================
#Bring in the static modern infrastructure count exactly at the 2026 deployment anchor row
    df_gold_market_inference = df_vahan_metrics \
    .filter(F.col("data_year") == 2026) \
    .join(
        df_charging_district_agg,
        ((df_vahan_metrics.v_state.contains(df_charging_district_agg.state)) & 
        (df_vahan_metrics.v_dist_clean.contains(df_charging_district_agg.district_v1))),
        how="left"
    ) \
    .select(
        "data_year",
        F.col("v_state").alias("state"),
        F.col("v_district").alias("district"),
        "total_sales",
        "ev_sales",
        F.round("total_sales_yoy_growth_pct", 2).alias("total_sales_yoy_growth_pct"),
        F.round("ev_sales_yoy_growth_pct", 2).alias("ev_sales_yoy_growth_pct"),
        F.round("ev_share_pct", 2).alias("ev_share_pct"),
        "district_encoded",
        "state_ev_avg_pct",
        "unique_district",
        F.coalesce("total_charging_stations_built_district_level", F.lit(0)).alias("total_charging_stations_built_district_level"),
        F.coalesce("sum_total_conn_d_level", F.lit(0)).alias("total_operational_charging_points")
    ) \
    .withColumn("charger_to_market_ratio", 
                F.when(F.col("total_sales") > 0, F.col("total_operational_charging_points") / F.col("total_sales")).otherwise(0.0))
    #print("df gold marketing inference debug")
    #df_gold_market_inference.filter((F.col('total_charging_stations_built_district_level') > 0) & (F.col('ev_sales_yoy_growth_pct') > 0)).show()

# ==========================================
# 3. GOLD TABLE: CITY ALLOCATION LOOKUP MAP
# ==========================================
# Maps city-level infrastructure concentration weight patterns inside each macro district
    district_total_window = Window.partitionBy("state", "district_v1")
    
    df_gold_city_alloc = df_charging_Agg.withColumnRenamed("sum_total_conn","city_total_charging_points"
                                    ).withColumn("district_total_points", F.sum("city_total_charging_points").over(district_total_window)
                                    ).withColumn("city_infra_share_pct", 
                F.when(F.col("district_total_points") > 0, 
                       (F.col("city_total_charging_points") / F.col("district_total_points")) * 100).otherwise(0.0)) \
    .select(
        F.col("state").alias("state"),
        F.col("district_v1").alias("district"),
        F.col("city_village").alias("city"),
        "city_total_charging_points",
        F.round("city_infra_share_pct", 2).alias("city_infra_share_pct")
    )

   

# ==========================================
# 4. WRITE PHYSICAL GOLD TABLES TO DATABASE
# ==========================================
    
    gold_ev_market_training_parquet_path= os.path.join(base_dir, "data", "3_gold", "gold_ev_market_training")
    gold_ev_market_inference_parquet_path= os.path.join(base_dir, "data", "3_gold", "gold_ev_market_inference")
    gold_ev_market_city_allocation_parquet_path= os.path.join(base_dir, "data", "3_gold", "gold_ev_city_allocation")


    try:
        print("💾 Commencing Parquet write operations training table...")
    
    # Partitioning splits data by year into clean subfolders
        df_gold_market_training.write \
        .mode("overwrite")\
        .parquet(gold_ev_market_training_parquet_path)
        
        print(f"✔️ gold layer Parquet training table save complete!")
        print(f"Location: {gold_ev_market_training_parquet_path}")

    except Exception as e:
        print(f"❌ Failed to write Parquet files training table: {e}")   
    try:
        print("💾 Commencing Parquet write operations inference table...")
    
    # Partitioning splits data by year into clean subfolders
        df_gold_market_inference.write \
        .mode("overwrite")\
        .parquet(gold_ev_market_inference_parquet_path)
        
        print(f"✔️ gold layer Parquet inference table save complete!")
        print(f"Location: {gold_ev_market_inference_parquet_path}")
    except Exception as e:
        print(f"❌ Failed to write Parquet files inference table: {e}")  
    try:
        print("💾 Commencing Parquet write operations city allocations table...")
    
    # Partitioning splits data by year into clean subfolders
        df_gold_city_alloc.write \
        .mode("overwrite")\
        .parquet(gold_ev_market_city_allocation_parquet_path)
        
        print(f"✔️ gold layer Parquet city allocations table save complete!")
        print(f"Location: {gold_ev_market_city_allocation_parquet_path}")
    except Exception as e:
        print(f"❌ Failed to write Parquet files city allocations : {e}")  
    spark.stop()
#%%    
def createGoldTable():
    

    IS_DOCKER = os.path.exists('/.dockerenv') or os.environ.get('AIRFLOW_HOME') is not None

# 2. Compute the correct root directory dynamically
    if IS_DOCKER:
    # Inside Docker, everything sits right inside the opt path
      base_dir = "/opt/airflow"
    else:
    # On Windows, your code uses the parent file layout structure
      base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "EVMLProject")) 
    #base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    gold_ev_market_training_parquet_path= os.path.join(base_dir, "data", "3_gold", "gold_ev_market_training")
    gold_ev_market_inference_parquet_path= os.path.join(base_dir, "data", "3_gold", "gold_ev_market_inference")
    gold_ev_market_city_allocation_parquet_path= os.path.join(base_dir, "data", "3_gold", "gold_ev_city_allocation")
    # silver_parquet_path_vahan = os.path.join(base_dir, "EVMLProject","data", "2_silver", "vahan_master_parquet")
    # silver_parquet_path_charge = os.path.join(base_dir, "EVMLProject","data", "2_silver", "charging_master_parquet")
    # silver_agg_parquet_path_charge = os.path.join(base_dir, "EVMLProject","data", "2_silver", "charging_agg_master_parquet")
    db_path = os.path.join(base_dir, "data", "3_gold", "ev_warehouse_gold.duckdb")
    config = {
        "access_mode": "READ_WRITE"
        # Forces it to save and release cleanly
    }
    
    #Using 'with' guarantees it closes even if the parquet read fails midway
    with duckdb.connect(db_path, config=config) as con:
        parquet_glob = os.path.join(gold_ev_market_training_parquet_path, "**", "*.parquet")
        query = f"""
        CREATE OR REPLACE TABLE ev_market_training_gold_table AS 
        SELECT * FROM read_parquet('{parquet_glob}');
        """
        con.execute(query)
        print("Table ev_market_training_gold_table recreated and data safely written!")
    with duckdb.connect(db_path, config=config) as con:
        parquet_glob = os.path.join(gold_ev_market_inference_parquet_path, "**", "*.parquet")
        query = f"""
        CREATE OR REPLACE TABLE ev_market_inference_gold_table AS 
        SELECT * FROM read_parquet('{parquet_glob}');
        """
        con.execute(query)
        print("Table ev_market_inference_gold_table recreated and data safely written!")

    with duckdb.connect(db_path, config=config) as con:
        parquet_glob = os.path.join(gold_ev_market_city_allocation_parquet_path, "**", "*.parquet")
        query = f"""
        CREATE OR REPLACE TABLE ev_city_allocation_gold_table AS 
        SELECT * FROM read_parquet('{parquet_glob}');
        """
        con.execute(query)
        print("Table ev_city_allocation_gold_table recreated and data safely written!")   
#%%        
def main():
    try:
       core_cleaning_preaggretion_and_gold_layer_creation()
    except Exception as e:
        print(f"❌ Failed to write Parquet files for gold layer: {e}") 
    try:        
       createGoldTable()
    except Exception as e:
        print(f"❌ Failed to create tables for gold layer: {e}")          
#%%
if __name__ == "__main__":
    try:
       core_cleaning_preaggretion_and_gold_layer_creation()
    except Exception as e:
        print(f"❌ Failed to write Parquet files for gold layer: {e}") 
    try:        
       createGoldTable()
    except Exception as e:
        print(f"❌ Failed to create tables for gold layer: {e}")    
# %%
