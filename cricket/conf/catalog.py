from pathlib import Path


class Project:
    root_dir: Path = Path(__file__).parent.parent
    data_dir: Path = root_dir.joinpath("data")

class Temp:
    temp_dir = Project.data_dir.joinpath("99_temp")
    yaml_zip_file = temp_dir.joinpath("all.zip") 
    json_zip_file = temp_dir.joinpath("all_json.zip") 

class Dump:
    dump_dir = Project.data_dir.joinpath("01_dump")
    yaml_dir = dump_dir.joinpath("yaml")
    json_dir = dump_dir.joinpath("json")
    track_dir = dump_dir.joinpath("track")

    new_dump_files = track_dir.joinpath("new_dump_files.parquet")

class Raw:
    raw_dir = Project.data_dir.joinpath("02_raw")
    yaml_dir = raw_dir.joinpath("yaml")

class Helper:
    helper_dir = Project.data_dir.joinpath("03_helpers")
    cricinfo_people = helper_dir.joinpath("espn_people_raw_data_2.csv")
    people_raw = helper_dir.joinpath("people_raw_data.csv")
    team_league_mapping = helper_dir.joinpath("team_league_map.csv")
    team_name_correction = helper_dir.joinpath("team_name_correction.csv")

class Preprocessed:
    preprocessed_dir = Project.data_dir.joinpath("04_preprocessed")
    deliveries = preprocessed_dir.joinpath("deliveries.parquet")
    match_info = preprocessed_dir.joinpath("match_info.parquet")
    registry = preprocessed_dir.joinpath("registry.parquet")

class Processed:
    processed_dir = Project.data_dir.joinpath("05_processed")
    cricket_dataset = processed_dir.joinpath("cricket_dataset.parquet")

class Catalog:
    project = Project
    temp = Temp
    dump = Dump
    raw = Raw
    helper = Helper
    preprocessed = Preprocessed
    processed = Processed

