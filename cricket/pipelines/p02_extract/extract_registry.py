from pathlib import Path
import yaml

import polars as pl

from prefect import flow, task
from prefect.logging import get_run_logger
from prefect_dask.task_runners import DaskTaskRunner

from cricket.conf.conf import Conf


@task(log_prints=True)
def extract_registry_details(files_batch: list[Path]) -> pl.LazyFrame|None:
    """
    Extracts player registry details from a batch of YAML files, processing each file to gather information 
    about registered players listed within the match files.

    Parameters:
        files_batch (list[Path]): List of paths to the YAML files containing the registry information.

    Returns:
        pl.LazyFrame | None: A Polars LazyFrame containing structured data about player registries extracted from all 
        processed files, or None if no data could be extracted or an error occurred in processing all files.

    Raises:
        Exception: Captures and logs any issues encountered during file processing, such as YAML parsing errors or 
        missing expected data, and continues with other files in the batch.
    """
    logger = get_run_logger()
    batch_registry_info = []

    for file in files_batch:
        try:
            match_id = file.stem
            with open(file) as f:
                match_data = yaml.safe_load(f)
            registry_dict = (
                match_data.get("info", {}).get("registry", {}).get("people", {})
            )

            for name, person_id in registry_dict.items():
                batch_registry_info.append((match_id, name, person_id))

        except Exception as e:
            logger.warning(f"Extract of registry for {file.stem} failed due to {e}")

    if batch_registry_info:
        registry_df = pl.LazyFrame(
            batch_registry_info, schema=Conf.schemas.registry, orient="row"
        )
        return registry_df

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
def pool_extract_registry_details(
    dump_yaml_files: list[Path], batch_size: int = 100
) -> list[pl.LazyFrame]:
    """
    Coordinates the parallel extraction of registry information from a series of YAML files. 
    This flow divides the workload into batches and uses Dask for distributed processing.

    Parameters:
        dump_yaml_files (list[Path]): Full list of YAML file paths to be processed.
        batch_size (int): The number of files each processing batch should contain.

    Returns:
        list[pl.LazyFrame]: A list containing Polars LazyFrames, each from processing a batch of files, 
        compiled into structured registry data.

    Notes:
        Uses Dask to manage parallel processing, enhancing performance by distributing tasks across multiple workers.
    """
    logger = get_run_logger()
    logger.info("Starting the pooling process for registry details")

    batches = [
        dump_yaml_files[i : i + batch_size]
        for i in range(0, len(dump_yaml_files), batch_size)
    ]
    results = extract_registry_details.map(batches)  # Using map for parallel processing
    all_registry = results.result()  # Gather results after all tasks complete

    logger.info("Completed processing all batches")
    return all_registry


@flow(log_prints=True)
def update_registry(df_new_dump_files: pl.LazyFrame | None = None) -> pl.LazyFrame:
    """
    Updates and consolidates new player registry details with existing registry data. This flow manages 
    the integration of new entries, ensuring that the registry is current and accurate.

    Parameters:
        df_new_dump_files (pl.LazyFrame | None): Optional new dump files containing registry data that needs 
        to be integrated into the existing registry dataset.

    Returns:
        pl.LazyFrame: The updated player registry as a Polars LazyFrame.

    Raises:
        ValueError: If new registry data contains duplicate entries that conflict with existing data.
    """
    logger = get_run_logger()

    # Preprocessed registry from previous runs
    # Define the path to the preprocessed registry
    registry_path = Conf.catalog.preprocessed.registry
    registry_data_exists = registry_path.exists()

    # Check if the preprocessed registry exist
    # If they exist, load the preprocessed registry
    # If they do not exist, create an empty LazyFrame
    if registry_data_exists:
        df_preprocessed_registry = pl.scan_parquet(registry_path)
    else:
        schema = Conf.schemas.registry
        df_preprocessed_registry = pl.LazyFrame(schema=schema)

    # If there are no new match IDs, return the preprocessed registry
    if df_new_dump_files is None:
        df_new_dump_files = pl.scan_parquet(Conf.catalog.dump.new_dump_files)
        if (
            df_new_dump_files.select(
                pl.col("match_id")
                .is_in(df_preprocessed_registry.select("match_id").collect())
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
        return df_preprocessed_registry

    # Get the paths to the raw files
    dump_yaml_directory = Conf.catalog.dump.yaml_dir
    dump_yaml_files = [
        dump_yaml_directory.joinpath(file + ".yaml")
        for file in df_new_dump_files.select("match_id").collect().to_series().to_list()
    ]

    # Extract the new registry
    all_registry = pool_extract_registry_details(dump_yaml_files)

    non_empty_dfs = [df for df in all_registry if df is not None]

    # If there are new registry, concatenate the preprocessed registry with the new registry
    if non_empty_dfs:
        df_new_registry = pl.concat(non_empty_dfs)

        df_registry = pl.concat([df_preprocessed_registry, df_new_registry])

    # If there are no new registry, return the preprocessed registry
    else:
        df_registry = df_preprocessed_registry

    return df_registry


@flow(name="extract_registry_flow", log_prints=True)
def extract_registry(df_new_dump_files: pl.LazyFrame | None = None) -> pl.LazyFrame:
    """
    Facilitates the extraction and update of player registry information from match files. It processes new 
    data, integrates it with existing records, and ensures the updated registry is written back to persistent storage.

    Parameters:
        df_new_dump_files (pl.LazyFrame | None): An optional LazyFrame containing new registry data to be processed.

    Returns:
        pl.LazyFrame: The fully updated and stored player registry dataset.

    Notes:
        This flow is crucial for maintaining a central, up-to-date player registry that can be accessed across 
        various analytical or operational systems.
    """
    df_registry = update_registry(df_new_dump_files)
    df_registry.collect().write_parquet(Conf.catalog.preprocessed.registry)
    return df_registry


if __name__ == "__main__":
    extract_registry()
