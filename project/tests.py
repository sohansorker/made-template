import os
import unittest
import subprocess
import pandas as pd

class DataPipelineTest(unittest.TestCase):
    
    # Define paths and filenames
    DATA_DIR = "../data"
    OUTPUT_FILE_1 = "electricity_production.csv"  
    OUTPUT_FILE_2 = "gdp_growth.csv" 
    OUTPUT_FILES = [OUTPUT_FILE_1, OUTPUT_FILE_2]

    def setUp(self):
       
        if os.path.exists(self.DATA_DIR):
            for f in os.listdir(self.DATA_DIR):
                file_path = os.path.join(self.DATA_DIR, f)
                if os.path.isfile(file_path):
                    os.remove(file_path)
        else:
            os.makedirs(self.DATA_DIR)
        
        print("Setup complete. Cleaned old data.")

    def test_run_data_pipeline(self):
        """Test if the data pipeline runs successfully and generates output files."""
        print("Running the data pipeline...")

        # Run the data pipeline (ensure the script fetch_and_process_data.py exists and is working)
        try:
            subprocess.check_call(["python3", "fetch_and_process_data.py"])
        except subprocess.CalledProcessError as e:
            self.fail(f"Data pipeline failed with error: {e}")

        # Check if the output files exist
        for file in self.OUTPUT_FILES:
            file_path = os.path.join(self.DATA_DIR, file)
            self.assertTrue(os.path.isfile(file_path), f"Output file {file} does not exist.")

        print("Data pipeline executed successfully, output files exist.")

    def test_check_file_content(self):
        """Test if the output files contain expected data, like 'Country Name'."""
        print("Validating file content...")

        # Check each output file for 'Country Name' in the header
        for file in self.OUTPUT_FILES:
            file_path = os.path.join(self.DATA_DIR, file)
            df = pd.read_csv(file_path)

            self.assertTrue('Country Name' in df.columns, f"File {file} does not contain 'Country Name' header.")

            print(f"File {file} contains the 'Country Name' header.")

    def test_validate_sample_data(self):
        """Test if certain expected data, like 'Brazil' or '2020', exists in the output files."""
        print("Validating sample data within the output files...")

        # Validate Brazil is in electricity production data
        file_path_1 = os.path.join(self.DATA_DIR, self.OUTPUT_FILE_1)
        df1 = pd.read_csv(file_path_1)
        self.assertTrue(df1['Country Name'].str.contains('Brazil').any(), "Brazil not found in the electricity production data.")

        # Validate 2020 is in GDP growth data
        file_path_2 = os.path.join(self.DATA_DIR, self.OUTPUT_FILE_2)
        df2 = pd.read_csv(file_path_2)
        self.assertTrue(df2['Country Name'].str.contains('Brazil').any(), "Brazil not found in the GDP growth data.")
        self.assertTrue(df2['Year'].str.contains('2020').any(), "Data for 2020 is missing in GDP growth data.")

        print("Sample data validation passed.")

    def tearDown(self):
        """Cleanup after each test: Delete the data directory and its contents."""
        if os.path.exists(self.DATA_DIR):
            for f in os.listdir(self.DATA_DIR):
                file_path = os.path.join(self.DATA_DIR, f)
                if os.path.isfile(file_path):
                    os.remove(file_path)
        print("Cleanup complete after tests.")

# Running the tests
if __name__ == "__main__":
    unittest.main()
