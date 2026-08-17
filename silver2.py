#%%
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pandas as pd
import duckdb
from pyspark.sql.types import DoubleType



#%%
def get_spark_session(app_name):
    """Centralized Spark Session builder to avoid resource conflicts."""
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "50") \
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:3.5.1_0.20.4") \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "6g") \
        .config("spark.network.timeout", "36000s") \
        .config("spark.executor.heartbeatInterval", "300s") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "5000") \
        .getOrCreate()

#%%
def process_silver_layer():
    print("\n🚀 Initializing Distributed Spark Engine for Phase 2: Silver Cleaning...")
    import gc
    import shutil
    import time 

    print("\n🚀 Initializing Distributed Spark Engine for Phase 2: Silver Cleaning...")
    spark = get_spark_session("vahan_silver_pipeline")

    IS_DOCKER = os.path.exists('/.dockerenv') or os.environ.get('AIRFLOW_HOME') is not None

    if IS_DOCKER:
        base_dir = "/opt/airflow"
    else:
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "EVMLProject"))

    bronze_path = os.path.join(base_dir, "data", "1_bronze", "States")
    
    # 📁 Setup a temporary directory for intermediate, flat staging
    temp_stage_path = os.path.join(base_dir, "data", "temp_silver_stage")
    if os.path.exists(temp_stage_path):
        shutil.rmtree(temp_stage_path)
    os.makedirs(temp_stage_path, exist_ok=True)

    print("Bronze path is being printed:", bronze_path)
    processed_count = 0

    for root, dirs, files in os.walk(bronze_path):
        for f in files:
            if f.endswith(".xlsx"):
                state_name = os.path.basename(root)
                full_path = os.path.join(root, f)
                year_val = "Unknown"
                for y in ["2022", "2023", "2024", "2025", "2026", "21", "22", "23", "24", "25", "26"]:
                    if y in f:
                        year_val = y
                        break
                
                try:
                    # 1. Read single heavy Excel file
                    df_file = spark.read.format("com.crealytics.spark.excel") \
                        .option("header", "true") \
                        .option("dataAddress", "0!A4") \
                        .load(full_path)
                    all_cols = df_file.columns

                    # 2. Schema Transformation
                    df_file = df_file.select(
                        F.col(all_cols[0]).alias("SlNo"),
                        F.trim(F.lcase(F.col(all_cols[1]))).alias("District"),
                        *[
                            F.coalesce(
                                F.trim(F.regexp_replace(F.col(all_cols[i]), ",", "")).cast("double").cast("int"), 
                                F.lit(0)
                            ).alias(all_cols[i]) 
                            for i in range(2, 38)
                        ],
                        F.regexp_replace(F.col(all_cols[38]), ",", "").cast("double").cast("int").alias("Total_Sales")
                    )
                    df_file = df_file.withColumn("state", F.lcase(F.lit(state_name))) \
                                     .withColumn("data_year",
                                      F.when(F.length(F.lit(year_val)) == 2, F.concat(F.lit("20"), F.lit(year_val)))
                                                    .otherwise(F.lit(year_val))) \
                                     .withColumn("source_file", F.lit(f))
                    df_file = df_file.dropna(how="all", subset=["SlNo", "District"])
                    
                    # 3. 🎯 MEMORY FIX: Write immediately to disk to break lineage and free memory
                    # Using a unique file-based sub-folder to ensure clean isolation
                    file_output_path = os.path.join(temp_stage_path, f"part_{processed_count}")
                    df_file.write.mode("overwrite").parquet(file_output_path)
                    
                    processed_count += 1
                    print(f"✅ Staged and cleared from RAM: {f}")

                except Exception as e:
                    print(f"❌ Error loading file {f}: {e}")
                
                finally:
                    # 4. 🧹 FORCE GC: Evict uncompressed Excel objects from the JVM Heap & Python
                    if 'df_file' in locals():
                        del df_file
                    gc.collect()
                    try:
                        spark._jvm.java.lang.System.gc()
                    except Exception:
                        pass
                    
                    # 4. 🛌 THE DELAY: Let the Docker sandbox breathe for 5 seconds
                    print(f"⏳ Pausing for 5s to stabilize Docker resources and database sockets...")
                    time.sleep(5) 
                    
    # 5. 🎯 MERGE FIX: Efficiently combine all parts using native Parquet file discovery
    if processed_count > 0:
        master_spark_df = spark.read \
            .option("mergeSchema", "true") \
            .parquet(os.path.join(temp_stage_path, "part_*"))
        
        print(f"✔️ Successfully combined {processed_count} Excel assets via native schema merging!")
    else:
        master_spark_df = None
        print("⚠️ No matching Excel assets located.")
    silver_parquet_path = os.path.join(base_dir,"data", "2_silver", "vahan_master_parquet")
    ## update on 10-7-2026

    try:
        print("💾 Commencing Parquet write operations...")
    
    # Partitioning splits data by year into clean subfolders
        master_spark_df.write \
        .mode("overwrite") \
        .partitionBy("data_year") \
        .parquet(silver_parquet_path)
        
        # print(f"✔️ Silver layer Parquet save complete!")
        # print(f"Location: {silver_parquet_path}")

    except Exception as e:
        print(f"❌ Failed to write Parquet files: {e}")
    spark.stop()
#%%    
def createSilverTable():
     
    IS_DOCKER = os.path.exists('/.dockerenv') or os.environ.get('AIRFLOW_HOME') is not None 
    if IS_DOCKER:
    # Inside Docker, everything sits right inside the opt path
      base_dir = "/opt/airflow"
    else:
    # On Windows, your code uses the parent file layout structure
      base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "EVMLProject"))
    #base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    silver_parquet_path_vahan = os.path.join(base_dir, "data", "2_silver", "vahan_master_parquet")
    silver_parquet_path_charge = os.path.join(base_dir, "data", "2_silver", "charging_master_parquet")
    silver_agg_parquet_path_charge = os.path.join(base_dir, "data", "2_silver", "charging_agg_master_parquet")
    db_path = os.path.join(base_dir, "data", "2_silver", "ev_warehouse.duckdb")
    config = {
        "access_mode": "READ_WRITE"
        # Forces it to save and release cleanly
    }
    
    #Using 'with' guarantees it closes even if the parquet read fails midway
    with duckdb.connect(db_path, config=config) as con:
        parquet_glob = os.path.join(silver_parquet_path_vahan, "**", "*.parquet")
        query = f"""
        CREATE OR REPLACE TABLE vahan_silver_table AS 
        SELECT * FROM read_parquet('{parquet_glob}');
        """
        con.execute(query)
        print("Table vahan_silver_table recreated and data safely written!")
    with duckdb.connect(db_path, config=config) as con:
        parquet_glob = os.path.join(silver_parquet_path_charge, "**", "*.parquet")
        query = f"""
        CREATE OR REPLACE TABLE charge_silver_table AS 
        SELECT * FROM read_parquet('{parquet_glob}');
        """
        con.execute(query)
        print("Table charge_silver_table recreated and data safely written!")

    with duckdb.connect(db_path, config=config) as con:
        parquet_glob = os.path.join(silver_agg_parquet_path_charge, "**", "*.parquet")
        query = f"""
        CREATE OR REPLACE TABLE charge_agg_silver_table AS 
        SELECT * FROM read_parquet('{parquet_glob}');
        """
        con.execute(query)
        print("Table charge_silver_agg_table recreated and data safely written!")


#%% 
def process_charging_Stations():
    print("\n🚀 Initializing Distributed Spark Engine for Phase 2: Silver Cleaning...")
    spark = get_spark_session("chargers_silver_pipeline")
    IS_DOCKER = os.path.exists('/.dockerenv') or os.environ.get('AIRFLOW_HOME') is not None 
    if IS_DOCKER:
    # Inside Docker, everything sits right inside the opt path
      base_dir = "/opt/airflow"
    else:
    # On Windows, your code uses the parent file layout structure
      base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "EVMLProject"))
    #base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    bronze_path = os.path.join(base_dir, "data", "1_bronze","Charging_stations")
    spark_dfs = []
   # silver_path = os.path.join(base_dir, "2_silver").replace("\\", "/")
    print("Bronze path isbeing printed",bronze_path)
    for root, dirs, files in os.walk(bronze_path):
        for f in files:
           if f.endswith(".xlsx"):
            full_path = os.path.join(root, f)
            try:
                # Load Excel file cleanly using the Crealytics connector
                df_file = spark.read.format("com.crealytics.spark.excel") \
                    .option("header", "true") \
                    .option("dataAddress", "0!A1") \
                    .load(full_path)
                all_cols = df_file.columns

# 3. Apply transformations and custom names
                df_file = df_file.select(
                               F.trim(F.lcase(F.col(all_cols[1]))).alias("operators"),
                               F.col(all_cols[2]).alias("govt_private"),
                               F.trim(F.lcase(F.col(all_cols[3]))).alias("state"),
                               F.trim(F.lcase(F.col(all_cols[4]))).alias("district_v1"),
                               F.trim(F.lcase(F.col(all_cols[5]))).alias("district_v2"),
                               F.coalesce(F.trim(F.col(all_cols[6])).cast("double").cast("int"),F.lit(0)).alias("district_code"),
                               F.trim(F.lcase(F.col(all_cols[7]))).alias("city_village"),
                               F.coalesce(F.trim(F.col(all_cols[9])).cast("double"),F.lit(0)).alias("lat"),
                               F.coalesce(F.trim(F.col(all_cols[10])).cast("double"),F.lit(0)).alias("long"),
                               F.trim(F.col(all_cols[11])).alias("chargers_type"),
                               F.coalesce(F.trim(F.col(all_cols[12])).cast("double").cast("int"),F.lit(0)).alias("charger_rating"),
                               F.coalesce(F.trim(F.col(all_cols[13])).cast("double").cast("int"),F.lit(0)).alias("conn_rating"),
                               F.coalesce(F.trim(F.col(all_cols[14])).cast("double").cast("int"),F.lit(0)).alias("total_connector")
)
                spark_dfs.append(df_file)
            except Exception as e:
                print(f"❌ Error loading file {f}: {e}")
    # len(spark_dfs)            
    silver_parquet_path_charge = os.path.join(base_dir, "data", "2_silver", "charging_master_parquet")

    try:
        print("💾 Commencing Parquet write operations...")
    
    # Partitioning splits data by year into clean subfolders
        df_file.write \
        .mode("overwrite")\
        .parquet(silver_parquet_path_charge)
        
        print(f"✔️ Silver layer Parquet save complete!")
        print(f"Location: {silver_parquet_path_charge}")

    except Exception as e:
        print(f"❌ Failed to write Parquet files: {e}")
    spark.stop()



#%%
def create_agg_chargingtable():
    print("\n🚀 Initializing Distributed Spark Engine for Phase 2: Silver Cleaning...")
    spark = get_spark_session("chargers_silver_pipeline_aggregation")
    IS_DOCKER = os.path.exists('/.dockerenv') or os.environ.get('AIRFLOW_HOME') is not None 
    if IS_DOCKER:
    # Inside Docker, everything sits right inside the opt path
      base_dir = "/opt/airflow"
    else:
    # On Windows, your code uses the parent file layout structure
      base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "EVMLProject"))
    #base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    silver_charging_parquet_path = os.path.join(base_dir, "data", "2_silver","charging_master_parquet")
    try:
       df = spark.read.parquet(silver_charging_parquet_path)
       df = df.groupBy('state','district_v1','city_village'
                    ).agg(F.count(F.concat(F.col('lat'),F.lit('-'),F.col('long'))).alias('Total_Charging_Stations_Built')
        ,F.sum('total_connector').alias('sum_total_conn'))
    #df.printSchema()
    except Exception as e:
                print(f"❌ Error {e}")
    silver_parquet_path_charge_agg = os.path.join(base_dir,"data", "2_silver", "charging_agg_master_parquet")

    try:
        print("💾 Commencing Parquet write operations...")
    
    # Partitioning splits data by year into clean subfolders
        df.write \
        .mode("overwrite")\
        .parquet(silver_parquet_path_charge_agg)
        
        print(f"✔️ Silver layer Parquet agg table save complete!")
        print(f"Location: {silver_parquet_path_charge_agg}")

    except Exception as e:
        print(f"❌ Failed to write Parquet files: {e}")
    spark.stop()

def main():
    process_silver_layer()
    process_charging_Stations()
    create_agg_chargingtable()
    createSilverTable()  
# %% 
if __name__ == "__main__":
    process_silver_layer()
    process_charging_Stations()
    create_agg_chargingtable()
    createSilverTable()

# %%
