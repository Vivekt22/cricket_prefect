import polars as pl

from prefect import flow

from cricket.conf.conf import Conf
from cricket.functions.functions import Functions as F


@flow
def download_raw_files() -> pl.LazyFrame|None:
    """
    Downloads new raw match data from remote sources and saves it to a designated local directory.
    Ensures data is ready for preprocessing by extracting and organizing files appropriately.
    """
    F.empty_data_folder(folder=Conf.catalog.dump.yaml_dir, file_types=["yaml", "txt"])

    F.extract_zip_file_from_url(
        url=Conf.params.cricsheet_yaml_url,
        zip_file_path=Conf.catalog.temp.yaml_zip_file,
        extract_file_path=Conf.catalog.dump.yaml_dir,
    )

    F.empty_data_folder(folder=Conf.catalog.dump.json_dir, file_types=["yaml", "txt"])

    F.extract_zip_file_from_url(
        url=Conf.params.cricsheet_json_url,
        zip_file_path=Conf.catalog.temp.json_zip_file,
        extract_file_path=Conf.catalog.dump.json_dir,
    )


@flow
def remove_old_dump_files() -> pl.LazyFrame:
    """
    Removes old files from the dump directory that have been processed and stored in the raw directory.
    Prevents reprocessing of data that has already been handled, maintaining data cleanliness.
    This cleanup helps manage storage efficiently and ensures only new data is processed.
    """
    dump_yaml_dir = Conf.catalog.dump.yaml_dir
    raw_yaml_dir = Conf.catalog.raw.yaml_dir

    dump_yaml_file_names = set([file.stem for file in dump_yaml_dir.glob("*.yaml")])
    raw_yaml_file_names = set([file.stem for file in raw_yaml_dir.glob("*.yaml")])

    file_names_to_remove = list(dump_yaml_file_names.intersection(raw_yaml_file_names))
    files_to_remove = [
        file
        for file in dump_yaml_dir.glob("*.yaml")
        if file.stem in file_names_to_remove
    ]

    F.remove_list_of_files(file_list=files_to_remove)

    new_dump_file_names = list(dump_yaml_file_names.difference(raw_yaml_file_names))
    df_new_dump_files = pl.DataFrame({"match_id": new_dump_file_names})

    df_new_dump_files.write_parquet(Conf.catalog.dump.new_dump_files)

    return df_new_dump_files.lazy()


@flow(name="ingest_flow")
def ingest_raw_data() -> pl.LazyFrame:
    download_raw_files()
    df_new_dump_files = remove_old_dump_files()
    return df_new_dump_files


if __name__ == "__main__":
    ingest_raw_data()
