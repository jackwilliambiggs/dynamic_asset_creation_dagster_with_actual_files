import os
import yaml
import pandas as pd
from sqlalchemy import create_engine
from dagster import asset, AssetIn, Output,  AssetKey, repository, define_asset_job
from typing import List

# Function to read the configuration from a YAML file
def load_config_from_yaml(file_path: str) -> List[dict]:
    with open(file_path, "r") as file:
        config = yaml.safe_load(file)
    return config

# Function to dynamically create an asset from the configuration spec

def build_asset(spec: dict):
    """
    This function dynamically creates a Dagster asset for loading a CSV file.

    Args:
        spec: A dictionary containing the asset's name, file path, etc.

    Returns:
        A dynamically created asset function that returns a pandas DataFrame.
    """
    @asset(
        name=spec["name"],
        description=spec.get("description", f"Asset generated from {spec['name']}"),
        non_argument_deps={AssetKey(dep) for dep in spec.get("upstreams", [])},  # Define upstream dependencies
    )
    def _asset(context) -> Output[pd.DataFrame]:  # Specify Output type as pandas DataFrame
        # Construct the file path from the directory and file name
        file_path = os.path.join(spec["directory"], spec["file_name"])

        # Load data from the CSV file
        try:
            df = pd.read_csv(file_path)
            context.log.info(f"Loaded data for {spec['name']} from {file_path}")
        except Exception as e:
            context.log.error(f"Error loading file {file_path}: {e}")
            raise

        # Validate that the output is a DataFrame
        if not isinstance(df, pd.DataFrame):
            context.log.error(f"Loaded data is not a DataFrame: {type(df)}")
            raise TypeError(f"Expected a pandas DataFrame, but got {type(df)}")

        # Return the DataFrame wrapped in an Output object
        return Output(df, output_name="result")  # Specify output name as 'result'

    return _asset



# Function to dynamically create a DB load asset from the original asset's spec

def build_db_load_asset(spec: dict):
    db_load_name = f"{spec['name']}_db_load"

    @asset(
        name=db_load_name,
        description=f"Simulates a DB load for {spec['name']}",
        # Define the dependency on the upstream asset via ins
        ins={"upstream_df": AssetIn(spec["name"])},  # This refers to the upstream asset by name
    )
    def _db_load_asset(context, upstream_df: pd.DataFrame):  # Accept the DataFrame from the upstream asset
        try:
            engine = create_engine(spec["database_url"])
            table_name = spec["name"]
            upstream_df.to_sql(table_name, engine, if_exists="replace", index=False)
            context.log.info(f"Data loaded into the table '{table_name}' in the database.")
        except Exception as e:
            context.log.error(f"Error loading data into DB: {e}")
            raise

    return _db_load_asset



# Load configuration from YAML file
assets_spec = load_config_from_yaml("assets_config.yaml")

# Create the initial assets dynamically using the build_asset function
initial_assets = [build_asset(spec) for spec in assets_spec]

# Create the DB load assets dynamically using the build_db_load_asset function
db_load_assets = [build_db_load_asset(spec) for spec in assets_spec]

# Combine both initial and DB load assets into one list
all_assets = initial_assets + db_load_assets

# Define a job that materializes the assets
dynamic_job = define_asset_job("dynamic_job")

# Define a repository to register assets and the job
@repository
def dynamic_assets_repo():
    return [*all_assets, dynamic_job]

