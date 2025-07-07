# Notebooks Repository

This repository contains Databricks notebooks used for developing and testing data pipelines, transformations, and feature engineering logic. The structure reflects the stages of a typical data engineering and machine learning workflow.

## Structure


```text
.
├── 00_Data_Loading
├── 01_Data_Exploration
├── 02_Data_Cleaning
├── 03_Join_Data
├── 04_Feature_Engineering
├── data/
└── utils/
```

### Descriptions

- `00_Data_Loading`: Load raw CSVs and Delta tables from Unity Catalog volumes.
- `01_Data_Exploration`: Basic profiling and sanity checks to understand raw schema and distributions.
- `02_Data_Cleaning`: Handles null values, data type enforcement, and format corrections.
- `03_Join_Data`: Joins across game metadata, user reviews, and tags to unify inputs.
- `04_Feature_Engineering`: Constructs input features, including text embeddings and boolean/numeric transformations.

### Other folders

- `data/`: Sample raw files or static reference data if needed locally.
- `utils/`: Shared helper functions (e.g., text preprocessing, plotting, common joins).

## Environment

Designed for use within a Databricks environment. Required libraries:

- pyspark
- delta
- pandas
- sentence-transformers
- mlflow
- matplotlib (optional for visualizations)

Use `%pip install` inside a Databricks notebook to install any missing dependencies.

## Notes

- Data is read from and written to Unity Catalog Volumes.
- Cleaned outputs are stored as Delta tables to support efficient downstream processing.
- These notebooks are used for development and exploratory work. Final logic may be moved into scripts or scheduled jobs.

## Status

The current pipeline supports data loading, profiling, cleaning, and feature generation. Model training and evaluation will be added in later stages.

