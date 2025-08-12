# Assette_Multi-Asset_Target_Date_Fund
This Repo include all of the content in this project

Target Date Fund ETL Pipeline
Overview
This project is designed to help Assette model and understand the data structures needed for multi-asset and target-date fund strategies. Using Vanguard’s Target Date Funds as a reference, we extracted data from Yahoo Finance, Vanguard, and a currency REST API to simulate real-world conditions. Python scripts transformed this data into Snowflake-compatible structures, and SQL was used to insert and organize it within a relational schema. The schema links key tables—such as holdings, benchmarks, performance, and fund metadata—into a unified framework, enabling clear analysis of asset allocation, benchmark comparisons, and performance trends. This work provides Assette with a foundation to enhance the model further, streamline data onboarding, and create analytical views that make target-date fund insights more actionable.
Table of Contents
1.	Project Objectives
2.	Project Structure
3.	Installation
4.	Script Overview
Project Objectives
- Automate the end-to-end process of Target Date Fund data ingestion and transformation.
- Integrate data from multiple external sources (Yahoo Finance, Vanguard, Currency REST API) into Snowflake.
- Provide a modular, maintainable codebase with separate scripts for extraction, transformation, and loading.
Project Structure

## Project Structure

| Path | Description |
|------|-------------|
| `project_root/` | Project root directory |
| ├── `documentation/` | Script documentation (detailed scripts info) |
| &emsp;├── `benchmark_characteristics_documentation.docx` | Documentation for benchmark characteristics script |
| &emsp;├── `benchmark_general_information_documentation.docx` | Documentation for benchmark general information script |
| &emsp;├── `benchmark_performance_documentation.docx` | Documentation for benchmark performance script |
| &emsp;├── `benchmark_performance_to_snowflake_documentation.docx` | Documentation for Snowflake benchmark load script |
| &emsp;├── `currency_code_table_documentation.docx` | Documentation for currency code table script |
| &emsp;├── `holding_details_documentation.docx` | Documentation for holding details script |
| &emsp;├── `portfolio_benchmark_association_documentation.docx` | Documentation for portfolio-benchmark association script |
| &emsp;├── `Portfolio_General_Information_Documentation_OFFICIAL.docx` | Official documentation for portfolio general information script |
| &emsp;├── `portfolio_performance_documentation.docx` | Documentation for portfolio performance script |
| &emsp;└── `product_master_documentation.docx` | Documentation for product master script |
| ├── `PlantUML/` | UML diagrams and outputs |
| &emsp;├── `Diagram/` | PNG image diagrams |
| &emsp;│ ├── `data_flow_diagram.png` | Data flow diagram image |
| &emsp;│ ├── `ETL.png` | ETL process diagram |
| &emsp;│ └── `Final_star_chema.png` | Final star schema diagram |
| &emsp;└── `SourceCode/` | UML source files |
| &emsp;&emsp;├── `data_flow_diagram.puml` | PlantUML source for data flow diagram |
| &emsp;&emsp;├── `ETL.puml` | PlantUML source for ETL diagram |
| &emsp;&emsp;└── `Final_star_chema.puml` | PlantUML source for final star schema diagram |
| ├── `source_code/` | ETL source code |
| &emsp;├── `Benchmark_Characteristic/` | Scripts for benchmark characteristics |
| &emsp;├── `Benchmark_General_Information/` | Scripts for benchmark general information |
| &emsp;├── `Benchmark_Performance/` | Scripts for benchmark performance |
| &emsp;├── `Holding_Details/` | Scripts for holding details |
| &emsp;├── `Portfolio_Benchmark_Association/` | Scripts for portfolio-benchmark association |
| &emsp;├── `Portfolio_General_Information/` | Scripts for portfolio general information |
| &emsp;├── `Portfolio_Performance/` | Scripts for portfolio performance |
| &emsp;├── `Product_Master/` | Scripts for product master |
| &emsp;├── `utils/` | Utility functions |
| &emsp;└── `__init__.py` | Package initializer |
| ├── `Disclosure_Data.csv` | Qualitative data file |
| ├── `local_config.env` | Environment configuration file |
| ├── `README_file.docx` | README documentation file |
| └── `requirements.txt` | Python dependencies list |


Installation
1. Clone the repository:
```bash
git clone https://github.com/Rick510/Assette_Multi-Asset_Target_Date_Fund.git

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set environment variables in `local_config.env`:
```env
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
```

## Script Overview

| Script Name | Function Summary |
|-------------|------------------|
| `HoldingDetails_Table.py` | Generates holding details from real-world and synthetic data; merges with portfolio information. |
| `PortfolioPerformance_Table.py` | Calculates portfolio-level performance based on holdings and benchmarks. |
| `Benchmark_Performance_table.py` | Fetches benchmark performance (e.g., GSPC, AGG) from Yahoo Finance. |
| `Benchmark_Performance_to_Snowflake.py` | Loads benchmark performance data into Snowflake incrementally. |
| `Product_Master_table.py` | Creates the product master table containing fund metadata. |
| `BenchmarkCharacteristic_table.py` | Generates benchmark characteristics for analysis. |
| `Currency_table.py` | Retrieves currency codes and rates from REST API and structures them for loading. |
| `PortfolioGeneralInformation_table.py` | Creates general portfolio information including categories, dates, and product links. |
| `Benchmark_General_Information.py` | Generates benchmark general information for target date funds, including realistic benchmark names, standardized symbols, and performance flags. |
| `Portfolio_Benchmark_Association.py` | Creates associations between portfolio codes and their primary (equity) and secondary (fixed income) benchmarks for target date funds. |


