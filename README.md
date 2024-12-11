# Prefect Cricket Data Processing Project

[![Powered by Prefect](https://img.shields.io/badge/powered_by-Prefect-blue.svg)](https://prefect.io)

## Project Overview
This project processes cricket match data, from ingestion and extraction through to the final production of a cleaned and integrated dataset. It uses the Prefect framework to orchestrate the workflows, leveraging the power of Polars for efficient data handling.

This project utilizes the Prefect framework to orchestrate workflows for processing cricket match data efficiently, leveraging Polars and DuckDB for data manipulation and analysis.

## Installation and Setup

### Requirements
Ensure you have Python 3.12 or newer installed. This project uses Poetry for dependency management and package handling.

### Setting Up the Project
1. **Install Poetry**:
   If you do not have Poetry installed, install it with:
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
2. **Clone the Repository**:
    ```bash
    git clone https://yourrepositoryurl.com/path/to/cricket
    cd cricket
3. Install Dependencies:
    ```bash
    poetry install
4. Install the package:
    ```bash
    pip install -e .

### Running the Project
1. Using the Prefect CLI: Ensure you have configured Prefect according to your environment setup (server or cloud), and then run the flow using
    ```bash
    poetry run main-run
2. Programmatically: Execute the flow by running the script set in Poetry:
    ```bash
    poetry run main-run
## Pipelines Overview

### 1. **Ingestion Flow**: Manages raw match data
   - **download_raw_files**: Downloads and extracts new match data from remote sources, preparing it in specified directories.
   - **remove_old_dump_files**: Cleans up older files from the dump directory that already exist in the raw directory to prevent reprocessing.
   - **update_raw_folder**: Moves new files from the dump directory to the raw directory after processing, and clears out the dump directory.

### 2. **Extraction Flow**: Extracts detailed data from raw files into structured formats
   - **extract_deliveries**: Extracts detailed delivery data from match files, handling data in batches for efficiency and parallel processing with Dask.
   - **extract_match_info**: Extracts comprehensive match information, including outcomes, teams, and officials, and also supports updating existing records with new data.
   - **extract_registry**: Gathers player registry details from match files, including names and IDs, ensuring updated and new entries are processed correctly.
   - **extract_event_data**: Processes additional event data from JSON files, focusing on key information such as events and names associated with matches.

### 3. **Processing Flow**: Integrates extracted data to create enriched cricket datasets
   - **create_cricket_dataset**: Combines delivery, match information, and registry data to produce a comprehensive dataset. This includes player names, match events, and performance statistics, with detailed handling of relationships and historical data consistency.
   - **update_deliveries**: Integrates new delivery data with previously processed data, ensuring updates are consistently applied and new information is accurately reflected.
   - **update_match_info**: Similar to update deliveries, this function ensures that new match information is properly integrated with the existing dataset, maintaining data integrity and consistency across updates.
   - **update_registry**: Manages updates to the player registry, incorporating new entries and updating existing records as necessary.


