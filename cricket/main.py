import polars as pl
from cricket.pipelines import (
    ingest_raw_data,
    extract_deliveries,
    extract_match_info,
    extract_registry,
    create_cricket_dataset,
    update_raw_folder,
)

from prefect import flow
from prefect.logging import get_run_logger


@flow(name="main_flow", log_prints=True)
def main():
    logger = get_run_logger()
    df_new_dump_files = ingest_raw_data()

    new_file_count = df_new_dump_files.drop_nulls().select(pl.len()).collect().item()
    if new_file_count > 0:
        df_deliveries = extract_deliveries(df_new_dump_files)
        df_match_info = extract_match_info(df_new_dump_files)
        df_registry = extract_registry(df_new_dump_files)
        df_cricket_dataset = create_cricket_dataset(
            df_deliveries=df_deliveries,
            df_match_info=df_match_info,
            df_registry=df_registry,
        )
        logger.info(f"Updated {new_file_count} matches")
        update_raw_folder()
        logger.info("Raw folder cleaned")
        return df_cricket_dataset
    else:
        logger.info("No new mathes to update")
        update_raw_folder()
        logger.info("Raw folder cleaned")
    return


if __name__ == "__main__":
    main()
