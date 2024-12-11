from pathlib import Path
from datetime import datetime, date
import requests
import zipfile
from tqdm import tqdm
import polars as pl
import duckdb

class Utils:
    @staticmethod
    def sql(query):
        return duckdb.sql(query)

    @staticmethod
    def convert_schema_dict(schema_dict: dict) -> dict:
        return {colname: getattr(pl, dtype) for colname, dtype in schema_dict.items()}

    @staticmethod
    def get_nested_value(data, keys, default=None):
        """
        Safe search for nested dictionary keys 
        """
        for key in keys:
            if isinstance(data, dict) and key in data:
                data = data[key]
            else:
                return default
        return data
    
    @staticmethod
    def safe_get(ref_dict, ref_key, ref_index=None):
        try:
            result = ref_dict[ref_key]
            if ref_index is not None:
                result = result[ref_index]
            return result
        except (KeyError, IndexError, TypeError):
            return None

    @staticmethod
    def parse_date(value):
        if isinstance(value, str):
            return datetime.strptime(value, '%Y-%m-%d').date()
        elif isinstance(value, (datetime, date)):
            return datetime.combine(value, datetime.min.time()).date()
        return None

    @staticmethod
    def convert_str_to_path(path: str|Path) -> Path:
        path = Path(path) if isinstance(path, str) else path
        return path
    
    @staticmethod
    def remove_list_of_files(file_list: list[Path|str]) -> None:
        for file in file_list:
            if isinstance(file, str):
                Path(file).unlink()
            else:
                file.unlink()

    @staticmethod
    def empty_data_folder(folder: str|Path, file_types: list|str = ["yaml", "txt"]) -> None:
        folder: Path = Utils.convert_str_to_path(folder)

        if isinstance(file_types, str):
            file_types = [file_types]

        for file_type in file_types:
            for file in folder.glob(f"*.{file_type}"):
                try:
                    file.unlink()  # Safely remove the file
                    
                except Exception as e:
                    raise ValueError(f"Error removing file {file}: {e}")
            print(f"Deleted {file_type} files")
                
    @staticmethod
    def extract_zip_file_from_url(url: str, zip_file_path: str|Path, extract_file_path: str|Path) -> None:
        zip_file_path: Path = Utils.convert_str_to_path(zip_file_path)
        extract_file_path: Path = Utils.convert_str_to_path(extract_file_path)
        
        # Download the zip file
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(zip_file_path, 'wb') as f:
                for chunk in tqdm(r.iter_content(chunk_size=8192), unit="KB"):
                    f.write(chunk)

        # Extract the zip file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            for member in tqdm(zip_ref.infolist(), desc="Extracting"):
                member_path = extract_file_path / member.filename  # Correct path joining
                if member_path.is_file() and member_path.stat().st_size == member.file_size:
                    continue
                zip_ref.extract(member, extract_file_path)

        zip_file_path.unlink()
