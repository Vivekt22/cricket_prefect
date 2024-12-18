from pathlib import Path
import yaml

import polars as pl

from prefect import flow, task
from prefect_dask.task_runners import DaskTaskRunner
from prefect.logging import get_run_logger

from cricket.conf.conf import Conf


@task(log_prints=True)
def extract_deliveries_details(files_batch: list[Path]) -> pl.LazyFrame|None:
    """
    Processes a batch of YAML files containing cricket match delivery details to extract relevant
    delivery data into a structured format.

    This function iterates over each file in the batch, extracting data such as the innings, batting team,
    bowling team, runs scored, and wickets. The data is collected into a list of dictionaries, which is
    then converted into a Polars LazyFrame for further processing or storage.

    Parameters:
        files_batch (list[Path]): A list of file paths to the YAML files containing the match data.

    Returns:
        pl.LazyFrame | None: A Polars LazyFrame containing all deliveries data from the batch if the
        batch is not empty; otherwise, None if there were no deliveries to process.

    Raises:
        Exception: Logs a warning with the file identifier if there is any issue in processing a file,
        such as file not found or data corruption.
    """
    logger = get_run_logger()
    batch_deliveries = []

    for file in files_batch:
        try:
            match_id = file.stem
            with open(file) as f:
                match_data = yaml.safe_load(f)

            match_info = match_data.get("info", {})
            for innings in match_data.get("innings", []):
                for inning, inning_details in innings.items():
                    batting_team = inning_details.get("team")
                    bowling_team = [
                        team
                        for team in match_info.get("teams", [])
                        if team != batting_team
                    ]
                    bowling_team = bowling_team[0] if bowling_team else "Unknown"

                    for delivery_details in inning_details.get("deliveries", []):
                        for delivery, delivery_info in delivery_details.items():
                            delivery_row = {
                                "match_id": match_id,
                                "innings": inning,
                                "batting_team": batting_team,
                                "bowling_team": bowling_team,
                                "declared": 1
                                if delivery_info.get("declared", "").lower() == "yes"
                                else 0,
                                "delivery": float(delivery),
                                "batter": delivery_info.get("batsman"),
                                "bowler": delivery_info.get("bowler"),
                                "non_striker": delivery_info.get("non_striker"),
                                "batter_runs": delivery_info["runs"].get("batsman", 0),
                                "extra_runs": delivery_info["runs"].get("extras", 0),
                                "total_runs": delivery_info["runs"].get("total", 0),
                                "wicket_type": delivery_info.get("wicket", {}).get(
                                    "kind", ""
                                ),
                                "player_out": delivery_info.get("wicket", {}).get(
                                    "player_out", ""
                                ),
                            }

                            batch_deliveries.append(delivery_row)

            if not batch_deliveries:
                logger.warning(
                    f"Extract of deliveries for {match_id} skipped due to empty innings"
                )
        except Exception as e:
            logger.warning(f"Extract of deliveries for {match_id} failed due to {e}")

    if batch_deliveries:
        deliveries_df = pl.LazyFrame(
            batch_deliveries, schema=Conf.schemas.deliveries, orient="row"
        ).lazy()
        return deliveries_df
    return None


@flow(
    task_runner=DaskTaskRunner(
        cluster_kwargs={
            "processes": True,
            "n_workers": 16,
            "threads_per_worker": 1,
            "memory_limit": "3GB",
        }
    )
)
def pool_extract_deliveries_details(
    dump_yaml_files: list[Path], batch_size: int = 100
) -> list[pl.LazyFrame]:
    """
    Orchestrates the parallel processing of large numbers of YAML files containing cricket match deliveries.
    This flow divides the files into batches and processes each batch in parallel using Dask to leverage
    multi-core processing.

    Parameters:
        dump_yaml_files (list[Path]): A list of paths to the YAML files to be processed.
        batch_size (int): The number of files to process in each batch.

    Returns:
        list[pl.LazyFrame]: A list of Polars LazyFrames, each representing the delivery data from one batch of files.

    The flow logs the start and completion of the processing and ensures all batches are processed,
    combining the results into a list of LazyFrames.
    """
    logger = get_run_logger()
    logger.info("Starting the pooling process for delivery details")

    batches = [
        dump_yaml_files[i : i + batch_size]
        for i in range(0, len(dump_yaml_files), batch_size)
    ]
    results = extract_deliveries_details.map(
        batches
    )  # Using map for parallel processing
    all_deliveries = results.result()  # Gather results after all tasks complete

    logger.info("Completed processing all batches")
    return all_deliveries


@flow(log_prints=True)
def update_deliveries(df_new_dump_files: pl.LazyFrame | None = None) -> pl.LazyFrame:
    """
    Updates the dataset by integrating new delivery data with previously processed delivery information.
    This flow ensures that the delivery dataset remains current and accurate, reflecting the latest available data.
    
    It checks the existence of preprocessed deliveries and attempts to merge new data from the specified new dump files.
    If new data is provided, it is concatenated with the existing preprocessed data after validation to avoid duplicate records.

    Parameters:
        df_new_dump_files (pl.LazyFrame | None): A Polars LazyFrame containing the new dump files to be integrated.
            If None, the function attempts to read new dump files from a predefined catalog path.

    Returns:
        pl.LazyFrame: A LazyFrame containing the updated dataset with both old and newly integrated delivery data.

    Raises:
        ValueError: If new match IDs from `df_new_dump_files` already exist in the preprocessed data,
                    indicating a data consistency issue.
    """
    logger = get_run_logger()

    # Define the path to the preprocessed deliveries
    delivery_path = Conf.catalog.preprocessed.deliveries
    delivery_data_exists = delivery_path.exists()

    # Check if the preprocessed deliveries exist
    # If they exist, load the preprocessed deliveries
    # If they do not exist, create an empty LazyFrame
    if delivery_data_exists:
        df_preprocessed_delivery = pl.scan_parquet(delivery_path)
    else:
        schema = Conf.schemas.deliveries
        df_preprocessed_delivery = pl.LazyFrame(schema=schema)

    # If there are no new match IDs, return the preprocessed deliveries
    if df_new_dump_files is None:
        df_new_dump_files = pl.scan_parquet(Conf.catalog.dump.new_dump_files)
        if (
            df_new_dump_files.select(
                pl.col("match_id")
                .is_in(df_preprocessed_delivery.select("match_id").collect())
                .any()
            )
            .collect()
            .item()
        ):
            raise ValueError(
                "New match IDs already exist in the preprocessed match_info."
            )
    if df_new_dump_files.drop_nulls().collect().height == 0:
        logger.info("No new match_ids to be updated. Ending update")
        return df_preprocessed_delivery

    # Get the paths to the dump files
    dump_yaml_directory = Conf.catalog.dump.yaml_dir
    dump_yaml_files = [
        dump_yaml_directory.joinpath(file + ".yaml")
        for file in df_new_dump_files.select("match_id").collect().to_series().to_list()
    ]

    # Extract the new deliveries
    # for file in dump_files
    all_deliveries = pool_extract_deliveries_details(dump_yaml_files)

    non_empty_dfs = [df for df in all_deliveries if df is not None]

    # If there are new deliveries, concatenate the preprocessed deliveries with the new deliveries
    if non_empty_dfs:
        df_new_deliveries = pl.concat(non_empty_dfs)
        df_deliveries = pl.concat([df_preprocessed_delivery, df_new_deliveries])

    # If there are no new deliveries, return the preprocessed deliveries
    else:
        df_deliveries = df_preprocessed_delivery

    return df_deliveries


@flow(name="extract_deliveries_flow", log_prints=True)
def extract_deliveries(df_new_dump_files: pl.LazyFrame | None = None) -> pl.LazyFrame:
    """
    Orchestrates the extraction and integration of delivery data from raw match files using a batch processing approach.
    This flow is designed to handle potentially large volumes of data by leveraging parallel processing capabilities.

    The function first updates existing delivery data with any new information provided. Once the data is updated,
    it writes the complete set of delivery data back to a persistent storage format (e.g., Parquet) for further use
    in analytics or reporting.

    Parameters:
        df_new_dump_files (pl.LazyFrame | None): An optional Polars LazyFrame that contains new delivery data.
            If None, the flow assumes there are no new deliveries to process.

    Returns:
        pl.LazyFrame: A LazyFrame representing the complete set of delivery data, including both updated and
            existing records.

    Notes:
        This flow is critical for maintaining the integrity and completeness of the delivery data used in downstream
        analytical tasks. It ensures data is stored in an optimized format for quick access and query performance.
    """
    df_deliveries = update_deliveries(df_new_dump_files)
    df_deliveries.collect().write_parquet(Conf.catalog.preprocessed.deliveries)
    return df_deliveries


if __name__ == "__main__":
    extract_deliveries()
