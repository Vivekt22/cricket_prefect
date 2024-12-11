from pathlib import Path
from shutil import move

from prefect import flow

from cricket.conf.conf import Conf
from cricket.functions.functions import Functions as F


@flow(log_prints=True)
def update_raw_folder():
    """
    Updates the raw data directory by moving processed files from the dump to the raw folder.
    Cleans the dump directory post-transfer to ensure it's ready for new incoming data.
    Critical for maintaining a clear separation between new raw data and data ready for extraction.
    """
    dump_dir: Path = Conf.catalog.dump.yaml_dir
    raw_dir: Path = Conf.catalog.raw.yaml_dir

    dump_files: list[Path] = list(dump_dir.glob("*.yaml"))
    raw_files_stem: set[str] = {raw_file.stem for raw_file in raw_dir.glob("*.yaml")}

    dump_files_to_move: list[Path] = [
        dump_file for dump_file in dump_files if dump_file.stem not in raw_files_stem
    ]

    moved_files_count = 0
    for dump_file in dump_files_to_move:
        destination_path = raw_dir.joinpath(dump_file.name)
        try:
            move(dump_file, destination_path)
            moved_files_count += 1
        except Exception as e:
            print(f"Failed to move {dump_file} to {destination_path}: {e}")

    print(f"Moved {moved_files_count} files from '{dump_dir}' to '{raw_dir}'.")

    F.empty_data_folder(dump_dir, file_types=["yaml", "txt"])


if __name__ == "__main__":
    update_raw_folder()
