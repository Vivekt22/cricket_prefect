import polars as pl
import polars.selectors as cs
from polars import col, when

from prefect import flow

from cricket.conf.conf import Conf


@flow(name="cricket_dataset_flow", log_prints=True)
def create_cricket_dataset(
    df_deliveries: pl.LazyFrame | None = None,
    df_match_info: pl.LazyFrame | None = None,
    df_registry: pl.LazyFrame | None = None,
    df_cricinfo_people: pl.LazyFrame | None = None,
    df_people_raw: pl.LazyFrame | None = None,
    df_team_league_mapping: pl.LazyFrame | None = None,
) -> pl.LazyFrame:
    """
    Compiles and integrates various data sources to construct a comprehensive cricket dataset.

    This flow synthesizes data from deliveries, match information, player registries, and additional sources to create 
    an enriched and holistic dataset. This dataset is aimed at providing deep insights into game strategies, player performances, 
    and historical data comparisons which can be used for advanced analytics and reporting.

    Parameters:
        df_deliveries (pl.LazyFrame | None): Preprocessed data frame of delivery information; if None, it's loaded from disk.
        df_match_info (pl.LazyFrame | None): Preprocessed match information data; if None, it's loaded from disk.
        df_registry (pl.LazyFrame | None): Preprocessed player registry data; if None, it's loaded from disk.
        df_cricinfo_people (pl.LazyFrame | None): Data frame containing people information from Cricinfo; if None, loaded from CSV.
        df_people_raw (pl.LazyFrame | None): Raw people data; if None, loaded from CSV.
        df_team_league_mapping (pl.LazyFrame | None): Data frame mapping teams to leagues; if None, loaded from CSV.

    Returns:
        pl.LazyFrame: A comprehensive LazyFrame that includes combined and refined data from all sources,
                      structured for analytical purposes.

    Notes:
        The function handles data loading, integration, transformation, and computation to produce a dataset that reflects
        current and accurate match details, player statistics, and other relevant information.
        The final dataset is persisted back to disk for use in downstream applications.
    """
    if df_deliveries is None:
        df_deliveries = pl.scan_parquet(Conf.catalog.preprocessed.deliveries)
    if df_match_info is None:
        df_match_info = pl.scan_parquet(Conf.catalog.preprocessed.match_info)
    if df_registry is None:
        df_registry = pl.scan_parquet(Conf.catalog.preprocessed.registry)
    if df_cricinfo_people is None:
        df_cricinfo_people = pl.scan_csv(Conf.catalog.helper.cricinfo_people)
    if df_people_raw is None:
        df_people_raw = pl.scan_csv(Conf.catalog.helper.people_raw)
    if df_team_league_mapping is None:
        df_team_league_mapping = pl.scan_csv(Conf.catalog.helper.team_league_mapping)

    df_deliveries_import = df_deliveries.with_columns(
        innings_id=pl.concat_str(
            "match_id", pl.lit("_"), col("innings").str.slice(0, 1)
        )
    )

    df_registry_import = df_registry

    df_cricinfo_people_import = df_cricinfo_people.rename(
        lambda c: c.lower().replace(" ", "_")
    ).select(
        col("id").alias("key_cricinfo"),
        col("name").alias("person_name_espn"),
        col("batting_style"),
        col("bowling_style"),
    )

    df_people_raw_data_import = df_people_raw.select(
        col("identifier").alias("person_id"),
        "key_cricinfo",
        col("name").alias("person_name"),
        col("unique_name").alias("person_unique_name"),
    )

    df_people = (
        df_people_raw_data_import.join(
            df_cricinfo_people_import, how="left", on="key_cricinfo"
        )
        .with_columns(
            person_name=pl.coalesce(col("person_name_espn"), col("person_name"))
        )
        .drop("person_name_espn", "batting_style", "bowling_style")
    )

    df_person_names = (
        df_registry_import.join(
            df_people.rename({"person_name": "person_name_full"}),
            how="left",
            on="person_id",
        )
        .with_columns(
            person_name=pl.coalesce(col("person_name_full"), col("person_name"))
        )
        .drop("person_name_full", "key_cricinfo")
        .unique()
    )

    SIX = 6

    df_deliveries = (
        df_deliveries_import.join(
            df_person_names.rename(
                {
                    "person_unique_name": "batter",
                    "person_name": "batter_name",
                    "person_id": "batter_id",
                }
            ),
            how="left",
            on=["batter", "match_id"],
        )
        .join(
            df_person_names.rename(
                {
                    "person_unique_name": "bowler",
                    "person_name": "bowler_name",
                    "person_id": "bowler_id",
                }
            ),
            how="left",
            on=["bowler", "match_id"],
        )
        .join(
            df_person_names.rename(
                {
                    "person_unique_name": "non_striker",
                    "person_name": "non_striker_name",
                    "person_id": "non_striker_id",
                }
            ),
            how="left",
            on=["non_striker", "match_id"],
        )
        .join(
            df_person_names.rename(
                {
                    "person_unique_name": "player_out",
                    "person_name": "player_out_name",
                    "person_id": "player_out_id",
                }
            ),
            how="left",
            on=["player_out", "match_id"],
        )
        .with_columns(
            missing_batter_name=when(col("batter_name").is_null())
            .then(True)
            .otherwise(False),
            missing_bowler_name=when(col("bowler_name").is_null())
            .then(True)
            .otherwise(False),
            missing_non_striker_name=when(col("non_striker_name").is_null())
            .then(True)
            .otherwise(False),
            missing_player_out_name=when(col("player_out_name").is_null())
            .then(True)
            .otherwise(False),
        )
        .with_columns(
            batter=pl.coalesce("batter_name", "batter"),
            bowler=pl.coalesce("bowler_name", "bowler"),
            non_striker=pl.coalesce("non_striker_name", "non_striker"),
            player_out=pl.coalesce("player_out_name", "player_out"),
        )
        .with_columns(
            over=col("delivery").cast(pl.Int64) + 1,
            ball=((col("delivery") - col("delivery").cast(pl.Int64)) * 10)
            .round()
            .cast(pl.Int64),
            wickets=when(col("wicket_type").is_not_null()).then(1).otherwise(0),
            six=when(col("batter_runs") >= SIX).then(1).otherwise(0),
            four=when(col("batter_runs").is_between(4, 5)).then(1).otherwise(0),
        )
        .select(
            [
                "match_id",
                "innings_id",
                "innings",
                "batting_team",
                "bowling_team",
                "declared",
                "bowler",
                "batter",
                "non_striker",
                "player_out",
                "delivery",
                "over",
                "ball",
                "wickets",
                "batter_runs",
                "extra_runs",
                "total_runs",
                "six",
                "four",
                "wicket_type",
                "missing_batter_name",
                "missing_bowler_name",
                "missing_non_striker_name",
                "missing_player_out_name",
                "batter_id",
                "bowler_id",
                "non_striker_id",
                "player_out_id",
            ]
        )
    )

    df_match_info_import = df_match_info

    df_match_info = (
        df_match_info_import.join(
            df_person_names.select(
                col("person_unique_name").alias("player_of_match"),
                col("person_name").alias("player_of_match_name"),
                "match_id",
            ),
            how="left",
            on=["player_of_match", "match_id"],
        )
        .with_columns(
            "player_of_match", pl.coalesce("player_of_match_name", "player_of_match")
        )
        .drop("player_of_match_name")
    )

    df_cricket_dataset = (
        df_deliveries.drop(
            *[
                c
                for c in df_deliveries.collect_schema().names()
                if c.startswith("missing_")
            ]
        )
        .drop(cs.starts_with("missing_"))
        .join(df_match_info, how="inner", on="match_id")
    )

    # Get league name
    df_cricket_dataset = (
        df_cricket_dataset.cast({cs.datetime(): pl.Date})
        .with_columns(
            cs.string().replace(
                "Royal Challengers Bengaluru", "Royal Challengers Bangalore"
            )
        )
        .join(
            df_team_league_mapping,
            how="left",
            left_on="batting_team",
            right_on="team_name",
            coalesce=True,
        )
        .with_columns(
            match_type=when(
                (col("match_type") == "ODM")
                & (col("league").str.to_lowercase().str.contains("international"))
            )
            .then(pl.lit("ODI"))
            .otherwise(col("match_type"))
        )
    )

    # Get batting order
    df_batting_order = (
        df_cricket_dataset.select(
            "innings_id",
            "delivery",
            pl.concat_list("batter", "non_striker").alias("batters"),
            "wickets",
        )
        .sort("innings_id", "delivery", maintain_order=True)
        .filter((col("delivery") == 0.1) | (col("wickets") == 1))
        .with_row_index(offset=1)
        .drop("wickets", "delivery")
        .explode("batters")
        .rename({"batters": "batter"})
        .filter(col("index") == col("index").min().over("innings_id", "batter"))
        .with_columns(
            batting_order=col("index").rank(method="ordinal").over("innings_id")
        )
        .drop("index")
    )

    # Get batting order
    df_cricket_dataset = (
        df_cricket_dataset.join(
            df_batting_order, how="left", on=["innings_id", "batter"], coalesce=True
        )
        .rename({"batting_order": "batter_number"})
        .join(
            df_batting_order,
            how="left",
            left_on=["innings_id", "non_striker"],
            right_on=["innings_id", "batter"],
            coalesce=True,
        )
        .rename({"batting_order": "non_striker_number"})
    )

    # Get wickets down
    df_cricket_dataset = df_cricket_dataset.with_columns(
        wickets_down=col("wickets").cum_sum().over("innings_id", order_by="delivery")
    )

    # Remove '' strings
    df_cricket_dataset = df_cricket_dataset.pipe(
        lambda df: df.with_columns(
            **{
                c: when(col(c) == "").then(None).otherwise(col(c))
                for c in df.select(cs.string()).collect_schema().names()
            }
        )
    )

    # Add delivery id
    df_cricket_dataset = df_cricket_dataset.sort(
        "match_start_date", "match_id", "innings_id", "delivery"
    ).with_row_index(name="delivery_id", offset=1)

    df_cricket_dataset.collect().write_parquet(Conf.catalog.processed.cricket_dataset)

    return df_cricket_dataset


if __name__ == "__main__":
    create_cricket_dataset()
