import os
import sqlite3
import pandas as pd
import subprocess

# Define paths and constants for your pipeline
DATA_DIR = "../data"
ELECTRICITY_CSV = os.path.join(DATA_DIR, "electricity_production_processed.csv")
GDP_CSV = os.path.join(DATA_DIR, "gdp_growth_processed.csv")
SQLITE_DB = os.path.join(DATA_DIR, "cleaned_data.db")
PIPELINE_SCRIPT = "pipeline.py"

# List of expected countries and years for validation
LATIN_AMERICAN_COUNTRIES = [
    "Argentina", "Bolivia", "Brazil", "Chile", "Colombia", "Costa Rica", "Cuba", "Dominican Republic", 
    "Ecuador", "El Salvador", "Guatemala", "Honduras", "Mexico", "Nicaragua", "Panama", "Paraguay", "Peru", 
    "Suriname", "Uruguay", "Venezuela"
]
EXPECTED_YEARS = [str(year) for year in range(2015, 2022)]

def test_pipeline_execution():
    """
    Test if the pipeline script executes successfully.
    """
    print("Testing pipeline execution...")
    result = subprocess.run(["python", PIPELINE_SCRIPT], capture_output=True, text=True)
    assert result.returncode == 0, f"Pipeline script failed: {result.stderr}"
    print("Pipeline executed successfully.")


def test_electricity_csv_exists():
    """
    Test if the electricity production cleaned CSV file is created.
    """
    print("Testing if electricity cleaned CSV exists...")
    assert os.path.exists(ELECTRICITY_CSV), f"Electricity cleaned CSV file not found: {ELECTRICITY_CSV}"
    print("Electricity cleaned CSV exists.")


def test_gdp_csv_exists():
    """
    Test if the GDP growth cleaned CSV file is created.
    """
    print("Testing if GDP growth cleaned CSV exists...")
    assert os.path.exists(GDP_CSV), f"GDP growth cleaned CSV file not found: {GDP_CSV}"
    print("GDP growth cleaned CSV exists.")


def test_electricity_csv_content():
    """
    Test the content of the electricity production cleaned CSV file.
    """
    print("Testing electricity cleaned CSV content...")
    df = pd.read_csv(ELECTRICITY_CSV)
    assert not df.empty, "Electricity cleaned CSV file is empty."
    assert "Country Name" in df.columns, "Expected column 'Country Name' not found in electricity CSV."
    assert "Year" in df.columns, "Expected column 'Year' not found in electricity CSV."
    assert set(EXPECTED_YEARS).issubset(df['Year'].astype(str).unique()), "Missing expected years in electricity CSV."
    print("Electricity cleaned CSV content is valid.")


def test_gdp_csv_content():
    """
    Test the content of the GDP growth cleaned CSV file.
    """
    print("Testing GDP growth cleaned CSV content...")
    df = pd.read_csv(GDP_CSV)
    assert not df.empty, "GDP growth cleaned CSV file is empty."
    assert "Country Name" in df.columns, "Expected column 'Country Name' not found in GDP CSV."
    assert "Year" in df.columns, "Expected column 'Year' not found in GDP CSV."
    assert set(EXPECTED_YEARS).issubset(df['Year'].astype(str).unique()), "Missing expected years in GDP CSV."
    print("GDP cleaned CSV content is valid.")


def test_sqlite_db_exists():
    """
    Test if the SQLite database file is created.
    """
    print("Testing if SQLite database exists...")
    assert os.path.exists(SQLITE_DB), f"SQLite database not found: {SQLITE_DB}"
    print("SQLite database exists.")


def test_sqlite_tables():
    """
    Test if the expected tables exist in the SQLite database.
    """
    print("Testing SQLite database tables...")
    with sqlite3.connect(SQLITE_DB) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        assert "electricity_production" in tables, "Table 'electricity_production' not found in SQLite database."
        assert "gdp_growth" in tables, "Table 'gdp_growth' not found in SQLite database."
    print("Expected tables found in SQLite database.")


def test_sqlite_table_content():
    """
    Test the content of the tables in the SQLite database.
    """
    print("Testing SQLite database table content...")
    with sqlite3.connect(SQLITE_DB) as conn:
        electricity_df = pd.read_sql("SELECT * FROM electricity_production;", conn)
        gdp_df = pd.read_sql("SELECT * FROM gdp_growth;", conn)

        # Check that the tables are not empty
        assert not electricity_df.empty, "Table 'electricity_production' in SQLite database is empty."
        assert not gdp_df.empty, "Table 'gdp_growth' in SQLite database is empty."

        # Validate the structure of the tables
        assert "Country Name" in electricity_df.columns, "Missing 'Country Name' column in electricity table."
        assert "Year" in electricity_df.columns, "Missing 'Year' column in electricity table."
        assert "Value" in electricity_df.columns, "Missing 'Value' column in electricity table."

        assert "Country Name" in gdp_df.columns, "Missing 'Country Name' column in GDP table."
        assert "Year" in gdp_df.columns, "Missing 'Year' column in GDP table."
        assert "Value" in gdp_df.columns, "Missing 'Value' column in GDP table."
    print("SQLite database tables contain valid data.")


if __name__ == "__main__":
    # Run all tests
    test_pipeline_execution()
    test_electricity_csv_exists()
    test_gdp_csv_exists()
    test_electricity_csv_content()
    test_gdp_csv_content()
    test_sqlite_db_exists()
    test_sqlite_tables()
    test_sqlite_table_content()

    print("All tests passed successfully.")
