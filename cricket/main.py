import polars as pl

from prefect import flow
from prefect.logging import get_run_logger

from cricket.pipelines import (
    ingest_raw_data,
    extract_deliveries,
    extract_match_info,
    extract_registry,
    create_cricket_dataset,
    update_raw_folder,
)


@flow(name="main_flow", log_prints=True)
def main():
    """
    The primary flow of the cricket data processing project, orchestrating the full data lifecycle 
    from ingestion through processing to dataset creation.

    This function acts as the central control unit, initiating the data ingestion process, processing new data,
    and integrating it into the existing datasets. It leverages several subprocesses to handle specific aspects
    of the data workflow, including data extraction for deliveries, match info, and registry details, followed by
    the creation of a comprehensive cricket dataset.

    Steps:
    1. Ingests new raw data and checks for new files.
    2. If new data is found, it processes deliveries, match info, and registry details in sequence.
    3. Integrates all processed data into a comprehensive dataset.
    4. Cleans up the raw data folder after processing to maintain data hygiene.

    Notes:
        - The function logs the number of new matches processed and manages the raw data directory post-processing,
          ensuring that the system is ready for subsequent data ingestion cycles.
        - This flow is critical for keeping the dataset up-to-date and ensuring that all new data is promptly and
          accurately reflected in the downstream analytics and reporting tools.
    """
    logger = get_run_logger()
    df_new_dump_files = ingest_raw_data()

    new_file_count = df_new_dump_files.drop_nulls().select(pl.len()).collect().item()
    if new_file_count > 0:
        df_deliveries = extract_deliveries(df_new_dump_files)
        df_match_info = extract_match_info(df_new_dump_files)
        df_registry = extract_registry(df_new_dump_files)
        create_cricket_dataset(
            df_deliveries=df_deliveries,
            df_match_info=df_match_info,
            df_registry=df_registry,
        )
        logger.info(f"Updated {new_file_count} matches")
        update_raw_folder()
        logger.info("Raw folder cleaned")
        return
    else:
        logger.info("No new mathes to update")
        update_raw_folder()
        logger.info("Raw folder cleaned")
    return


if __name__ == "__main__":
    main()
