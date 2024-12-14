import os
import yaml
import pandas as pd
from sqlalchemy import create_engine
from dagster import asset, AssetKey, repository, define_asset_job
from typing import List


# Function to read the configuration from a YAML file
def load_config_from_yaml(file_path: str) -> List[dict]:
    """
    Loads asset configuration from a YAML file.

    Args:
        file_path (str): Path to the YAML configuration file.

    Returns:
        List[dict]: List of asset specifications (name, upstreams, sql).
    """
    with open(file_path, "r") as file:
        config = yaml.safe_load(file)
    return config


# Function to dynamically create an asset from the configuration spec
def build_asset(spec: dict):
    """
    This function dynamically creates a Dagster asset based on a given spec.

    Args:
        spec: A dictionary containing the asset's name, upstream dependencies, and SQL.

    Returns:
        A dynamically created asset function.
    """

    @asset(
        name=spec["name"],
        description=spec.get("description", f"Asset generated from {spec['name']}"),
        non_argument_deps={
            AssetKey(dep) for dep in spec.get("upstreams", [])
        },  # Define upstream dependencies
    )
    def _asset(context):
        file_path = os.path.join(spec["directory"], spec["file_name"])
        df = pd.read_csv(file_path)  # Load data from the CSV file
        context.log.info(f"Loaded data for {spec['name']} from {file_path}")
        return df

    return _asset


# Function to dynamically create a DB load asset from the original asset's spec
def build_db_load_asset(spec: dict):
    """
    This function dynamically creates a Dagster DB load asset for each original asset.

    Args:
        spec: A dictionary containing the original asset's name, upstream dependencies, and SQL.

    Returns:
        A dynamically created DB load asset function.
    """
    # Create the DB load asset name by adding "_db_load" suffix to the original asset name
    db_load_name = f"{spec['name']}_db_load"

    @asset(
        name=db_load_name,
        description=f"Simulates a DB load for {spec['name']}",
        non_argument_deps={
            AssetKey(spec["name"])
        },  # The DB load depends on the original asset
    )
    def _db_load_asset(context):
        df = context.resources[spec["name"]]  # Get the DataFrame from upstream asset
        engine = create_engine(spec["database_url"])
        table_name = spec["name"]
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        context.log.info(f"Data loaded into the table {table_name} in the database.")

    return _db_load_asset


# Load configuration from YAML file
assets_spec = load_config_from_yaml("assets_config.yaml")

# Create the initial assets dynamically using the build_asset function
initial_assets = [build_asset(spec) for spec in assets_spec]

# Create the DB load assets dynamically using the build_db_load_asset function
db_load_assets = [build_db_load_asset(spec) for spec in assets_spec]

# Combine both initial and DB load assets into one list
#all_assets = initial_assets
all_assets = initial_assets + db_load_assets

# Define a job that materializes the assets
dynamic_job = define_asset_job("dynamic_job")


# Define a repository to register assets and the job
@repository
def dynamic_assets_repo():
    return [*all_assets, dynamic_job]

