from dagster import asset, AssetKey, AssetsDefinition, define_asset_job, repository
import pandas as pd
import os
from sqlalchemy import create_engine

# Path to CSV files
CSV_DIRECTORY = "path_to_your_csvs"
CSV_FILES = ["random_data_1.csv", "random_data_2.csv"]

# Database connection string
DATABASE_URL = "sqlite:///example.db"

# Helper function to dynamically build assets from CSVs
def build_asset_from_csv(file_name: str) -> AssetsDefinition:
    @asset(
        name=file_name.replace(".csv", ""),
        description=f"Asset generated from {file_name}"
    )
    def asset_fn() -> pd.DataFrame:
        file_path = os.path.join(CSV_DIRECTORY, file_name)
        df = pd.read_csv(file_path)
        return df

    return asset_fn


# Helper function to create db load assets
#def build_db_load_asset(csv_asset_name: str) -> AssetsDefinition:
  #  @asset(
   #     name=f"{csv_asset_name}_db_load",
    #    description=f"Simulates DB load for asset {csv_asset_name}",
     #   non_argument_deps={AssetKey([csv_asset_name])}
   # )
   # def db_load_asset(context):
        # Simulate getting the DataFrame from the original asset
    #    df = context.resources[csv_asset_name]
        
        # Load the data into the database
      #  engine = create_engine(DATABASE_URL)
     #   table_name = csv_asset_name
       # df.to_sql(table_name, engine, if_exists="replace", index=False)
        #context.log.info(f"Data loaded into the table {table_name} in the database.")
    
    #return db_load_asset


# Dynamically generate the initial CSV assets and DB load assets
csv_assets = [build_asset_from_csv(file) for file in CSV_FILES]
#db_load_assets = [build_db_load_asset(asset.name) for asset in csv_assets]


# Define a single asset group
all_assets = csv_assets
# all_assets = csv_assets + db_load_assets

# Define a job to execute all assets
process_csvs_job = define_asset_job(name="process_csvs_job")

# Define the repository
@repository
def dynamic_assets_repository():
    return [*all_assets, process_csvs_job]

