import os
import shutil

def ingest_to_bronze():
    print("📥 Starting Phase 1: Ingesting Raw Telemetry to Bronze Storage...")
    
    IS_DOCKER = os.path.exists('/.dockerenv') or os.environ.get('AIRFLOW_HOME') is not None

# 2. Compute the correct root directory dynamically
    if IS_DOCKER:
    # Inside Docker, everything sits right inside the opt path
      base_dir = "/opt/airflow"
    else:
    # On Windows, your code uses the parent file layout structure
      base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "EVMLProject"))
    # Define local project directories
    bronze_dir = os.path.join(base_dir, "data", "1_bronze")
    #print(bronze_dir)
    indianStates =  ['Andaman', 'AndhraPradesh', 'ArunachalPradesh', 'Assam', 'Bihar', 'Chandigarh', 'Chattisgarh', 'DamanAndDiu', 'Delhi', 'Goa', 'Gujarat', 'Haryana', 'HimachalPradesh', 'JammuandKashmir', 'Jharkhand', 'Karnataka', 'Kerala', 'Ladakh', 'Laskhwadeep', 'MadhyaPradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram', 'Nagaland', 'Odisha', 'Puducherry', 'Punjab', 'Rajasthan', 'Sikkim', 'Tamil Nadu', 'Telangana', 'Tripura', 'Uttarakhand', 'Uttarpradesh', 'West Bengal']
    # In a real pipeline, this mimics moving data from a landing port or an API download
    # Ensure raw directory path exists
    os.makedirs(bronze_dir, exist_ok=True)
    for root, dirs, files in os.walk(bronze_dir):
           print("ROOT:", root)
           print("directories",dirs)
           print("FILES:", files)
          
    # if len(raw_files) == 0:
    #     print("⚠️ Warning: No raw files detected in 1_bronze folder yet. Please add your state CSVs.")
    # else:
    #     print(f"✔️ Bronze Layer validation complete. Found {len(raw_files)} raw source assets ready for processing.")


def main():
    ingest_to_bronze()
    
if __name__ == "__main__":
    ingest_to_bronze()