from pathlib import Path

import orjson

import polars as pl
import yaml

from prefect import flow, task
from prefect.logging import get_run_logger
from prefect_dask import DaskTaskRunner

from cricket.conf.conf import Conf
from cricket.functions.functions import Functions as F

from typing import List


@task(log_prints=True)
def extract_match_info_details(files_batch: List[Path]):
    logger = get_run_logger()
    batch_match_info = []

    for file in files_batch:
        try:
            match_id = file.stem
            with open(file) as f:
                match_data = yaml.safe_load(f)
            match_info: dict = match_data["info"]

            outcome_by = match_info.get("outcome", {}).get("by", {})
            win_by_key = next(iter(outcome_by), None)
            win_margin_value = float(outcome_by[win_by_key]) if win_by_key else 0

            match_info_row = {
                "match_id": match_id,
                "city": F.safe_get(match_info, "city"),
                "match_start_date": F.parse_date(F.safe_get(match_info, "dates", 0)),
                "match_end_date": F.parse_date(F.safe_get(match_info, "dates", -1)),
                "match_type": F.safe_get(match_info, "match_type"),
                "gender": F.safe_get(match_info, "gender"),
                "umpire_1": F.safe_get(match_info, "umpires", 0),
                "umpire_2": F.safe_get(match_info, "umpires", -1),
                "win_by": win_by_key,
                "win_margin": win_margin_value,
                "winner": F.safe_get(F.safe_get(match_info, "outcome"), "winner"),
                "player_of_match": F.safe_get(match_info, "player_of_match", 0),
                "team1": F.safe_get(match_info, "teams", 0),
                "team2": F.safe_get(match_info, "teams", -1),
                "toss_decision": F.safe_get(F.safe_get(match_info, "toss"), "decision"),
                "toss_winner": F.safe_get(F.safe_get(match_info, "toss"), "winner"),
                "venue": F.safe_get(match_info, "venue"),
            }
            batch_match_info.append(match_info_row)

        except Exception as e:
            logger.warning(f"Extract of match info for {file.stem} failed due to {e}")

    if batch_match_info:
        match_info_df = pl.LazyFrame(
            batch_match_info, schema=Conf.schemas.match_info, orient="row"
        )
        return match_info_df

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
def pool_extract_match_info_details(
    dump_yaml_files: List[Path], batch_size: int = 100
) -> List[pl.DataFrame]:
    logger = get_run_logger()
    logger.info("Starting the pooling process for match info details")

    batches = [
        dump_yaml_files[i : i + batch_size]
        for i in range(0, len(dump_yaml_files), batch_size)
    ]
    results = extract_match_info_details.map(
        batches
    )  # Using map for parallel processing
    all_match_info = results.result()  # Gather results after all tasks complete

    logger.info("Completed processing all batches")
    return all_match_info


@task(log_prints=True)
def extract_event_data_details(files_batch: list[Path], event_dict_keys: list[str]):
    logger = get_run_logger()
    batch_events = []

    for file in files_batch:
        try:
            with open(file, "rb") as f:
                json_data = orjson.loads(f.read())

            # Extract data using the provided utility function
            event_value = F.get_nested_value(json_data, event_dict_keys)

            df_event = pl.DataFrame(
                {"match_id": [file.stem], "event": [event_value]}
            ).lazy()

            batch_events.append(df_event)
        except Exception as e:
            logger.warning(f"Failed to process {file.name} due to {e}")

    if batch_events:
        # Concatenate all LazyFrames in this batch
        return pl.concat(batch_events)
    return pl.LazyFrame(schema={"match_id": pl.String, "event": pl.String})


@flow(log_prints=True)
def pool_extract_event_data() -> pl.LazyFrame:
    json_files = [file for file in Conf.catalog.dump.json_dir.glob("*.json")]
    event_dict_keys = ["info", "event", "name"]
    batch_size = 100

    batches = [
        json_files[i : i + batch_size] for i in range(0, len(json_files), batch_size)
    ]
    results = [extract_event_data_details(batch, event_dict_keys) for batch in batches]

    # Concatenate results from all batches into one LazyFrame
    return pl.concat(results)


@flow(log_prints=True)
def update_match_info(df_new_dump_files: pl.LazyFrame | None = None) -> pl.LazyFrame:
    logger = get_run_logger()

    # Preprocessed match_info from previous runs
    # Define the path to the preprocessed match_info
    match_info_path = Conf.catalog.preprocessed.match_info
    match_info_data_exists = match_info_path.exists()

    # Check if the preprocessed match_info exist
    # If they exist, load the preprocessed match_info
    # If they do not exist, create an empty LazyFrame
    if match_info_data_exists:
        df_preprocessed_match_info = pl.scan_parquet(match_info_path)
    else:
        schema = Conf.schemas.match_info
        df_preprocessed_match_info = pl.LazyFrame(schema=schema)

    # If there are no new match IDs, return the preprocessed match_info
    if df_new_dump_files is None:
        df_new_dump_files = pl.scan_parquet(Conf.catalog.dump.new_dump_files)
        if (
            df_new_dump_files.select(
                pl.col("match_id")
                .is_in(df_preprocessed_match_info.select("match_id").collect())
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
        return df_preprocessed_match_info

    # Get the paths to the raw files
    dump_yaml_directory = Conf.catalog.dump.yaml_dir
    dump_yaml_files = [
        dump_yaml_directory.joinpath(file + ".yaml")
        for file in df_new_dump_files.select("match_id").collect().to_series().to_list()
    ]

    # Extract the new match_info
    all_match_info = pool_extract_match_info_details(dump_yaml_files)

    non_empty_dfs = [df for df in all_match_info if df is not None]

    # If there are new match_info, concatenate the preprocessed match_info with the new match_info
    if non_empty_dfs:
        df_new_match_info = pl.concat(non_empty_dfs).lazy()
        df_match_info = pl.concat(
            [df_preprocessed_match_info.cast(schema), df_new_match_info.cast(schema)]
        )

    # If there are no new match_info, return the preprocessed match_info
    else:
        df_match_info = df_preprocessed_match_info

    df_event = pool_extract_event_data()

    if df_event.drop_nulls().select(pl.len()).collect().item() > 0:
        df_match_info = df_match_info.join(
            df_event, how="left", on="match_id", coalesce=True
        )

    df_match_info.collect().write_parquet(Conf.catalog.preprocessed.match_info)

    return df_match_info


@flow(name="extract_match_info_flow", log_prints=True)
def extract_match_info(df_new_dump_files: pl.LazyFrame | None = None):
    df_match_info = update_match_info(df_new_dump_files)
    return df_match_info


if __name__ == "__main__":
    extract_match_info()
