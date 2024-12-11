from pathlib import Path

import polars as pl
import yaml

from prefect import flow, task
from prefect_dask.task_runners import DaskTaskRunner
from prefect.logging import get_run_logger

from cricket.conf.conf import Conf


@task(log_prints=True)
def extract_deliveries_details(files_batch: list[Path]):
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
) -> list[pl.DataFrame]:
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
        df_new_deliveries = pl.concat(non_empty_dfs).lazy()
        df_deliveries = pl.concat([df_preprocessed_delivery, df_new_deliveries])

    # If there are no new deliveries, return the preprocessed deliveries
    else:
        df_deliveries = df_preprocessed_delivery

    df_deliveries.collect().write_parquet(Conf.catalog.preprocessed.deliveries)

    return df_deliveries


@flow(name="extract_deliveries_flow", log_prints=True)
def extract_deliveries(df_new_dump_files: pl.LazyFrame | None = None):
    df_deliveries = update_deliveries(df_new_dump_files)
    return df_deliveries


if __name__ == "__main__":
    extract_deliveries()
