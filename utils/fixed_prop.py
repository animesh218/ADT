# fixed_properties.py
# Generate pricing data for each day of a specified month with proper structure and logging
import pandas as pd
import calendar
from datetime import datetime
import os
import argparse
import logging
import sys


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("properties_generator.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

class PropertyDataGenerator:
    """Class that handles all data generation functionality."""
    
    def __init__(self, logger=None):
        """Initialize the generator with a logger."""
        self.logger = logger or logging.getLogger(self.__class__.__name__)
    
    def validate_month_name(self, month_name):
        """Validate the month name and return the month number."""
        try:
            # Try full month name
            month_num = datetime.strptime(month_name, '%B').month
            self.logger.info(f"Month validated: {month_name} (full format)")
            return month_num
        except ValueError:
            try:
                # Try abbreviated month name
                month_num = datetime.strptime(month_name, '%b').month
                self.logger.info(f"Month validated: {month_name} (abbreviated format)")
                return month_num
            except ValueError:
                self.logger.error(f"Invalid month name: {month_name} in {self.__class__.__name__}.validate_month_name")
                return None
    
    def get_month_input(self):
        """Get month input from user with validation."""
        self.logger.info("Requesting month input from user")
        while True:
            month_name = input("Enter month name (e.g., January or Jan): ").strip().title()
            month_num = self.validate_month_name(month_name)
            if month_num:
                return month_name, month_num
            self.logger.warning(f"User entered invalid month: {month_name}")
            print(f"Invalid month name: {month_name}. Please enter a valid month name.")
    
    def create_base_dataframe(self):
        """Create the base dataframe with property pricing data."""
        self.logger.info("Creating base dataframe template")
        try:
            data = {
                'date': ['2025-05-01'] * 7,
                'property': ['Instagram Post', 'Instagram Story', 'Facebook Post', 'Facebook Story', 
                            'Push Notification', 'Push Notification-Custom', 'In App Notification'],
                'price_type': ['CPD'] * 7,
                'rate': [150000, 150000, 75000, 75000, 150000, 200000, 50000],
                'page': ['SOCIAL', 'SOCIAL', 'SOCIAL', 'SOCIAL', 'CRM', 'CRM', 'CRM'],
                'supply': [1] * 7,
                'allocation': [1] * 7,
                'bu': ['OPEN ALLOCATION', 'OPEN ALLOCATION', 'OPEN ALLOCATION', 'OPEN ALLOCATION',
                      'OPEN ALLOCATION', 'SUPPLY TEAM', 'SUPPLY TEAM'],
                'event': ['ALL'] * 7,
                'impressions': [1] * 7
            }
            return pd.DataFrame(data)
        except Exception as e:
            self.logger.error(f"Error creating base dataframe in {self.__class__.__name__}.create_base_dataframe: {str(e)}")
            raise
    
    def repeat_data_by_month(self, month_name, year=2025):
        """Generate data for each day in the specified month."""
        self.logger.info(f"Generating data for {month_name} {year}")
        
        # Validate month name
        month_num = self.validate_month_name(month_name)
        if not month_num:
            self.logger.error(f"Month validation failed in {self.__class__.__name__}.repeat_data_by_month")
            return None

        try:
            # Create the base dataframe
            df = self.create_base_dataframe()
            
            # Get the number of days in the specified month and year
            num_days = calendar.monthrange(year, month_num)[1]
            self.logger.info(f"Generating data for {num_days} days in {month_name} {year}")
            
            # Create an empty list to store the repeated dataframes
            repeated_dfs = []
            
            # Create a dataframe for each day in the month
            for day in range(1, num_days + 1):
                # Create a copy of the original dataframe
                temp_df = df.copy()
                
                # Update the date column to reflect the current day
                date_str = f"{year}-{month_num:02d}-{day:02d}"
                temp_df['date'] = date_str
                
                # Add this dataframe to the list
                repeated_dfs.append(temp_df)
            
            # Concatenate all dataframes
            result_df = pd.concat(repeated_dfs, ignore_index=True)
            self.logger.info(f"Successfully generated {len(result_df)} rows of data")
            
            return result_df
        except Exception as e:
            self.logger.error(f"Error in {self.__class__.__name__}.repeat_data_by_month: {str(e)}")
            return None
    
    def generate_verification_info(self, result_df, month_name, month_num, year):
        """Generate verification information about the dataset."""
        self.logger.info("Generating verification information")
        try:
            total_rows = len(result_df)
            days_in_month = calendar.monthrange(year, month_num)[1]
            unique_dates = result_df['date'].nunique()
            unique_properties = result_df['property'].nunique()
            properties_per_day = len(result_df) // unique_dates
            
            verification_text = f"""Verification Report
====================
Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Month: {month_name} {year}
Days in month: {days_in_month}
Total rows: {total_rows}
Unique dates: {unique_dates}
Unique properties: {unique_properties}
Properties per day: {properties_per_day}

Data Summary:
------------
Properties: {', '.join(result_df['property'].unique())}
Price types: {', '.join(result_df['price_type'].unique())}
Pages: {', '.join(result_df['page'].unique())}
Business units: {', '.join(result_df['bu'].unique())}
"""
            return verification_text
        except Exception as e:
            self.logger.error(f"Error in {self.__class__.__name__}.generate_verification_info: {str(e)}")
            return "Error generating verification information."
    
    def ensure_directory_exists(self, directory):
        """Ensure the output directory exists."""
        self.logger.info(f"Checking if directory exists: {directory}")
        if not os.path.exists(directory):
            try:
                os.makedirs(directory)
                self.logger.info(f"Created directory: {directory}")
                print(f"Created directory: {directory}")
            except Exception as e:
                self.logger.error(f"Error creating directory in {self.__class__.__name__}.ensure_directory_exists: {str(e)}")
                raise
    
    def save_files(self, result_df, month_name, year, output_dir='output'):
        """Save the data to CSV and create a verification file."""
        self.logger.info(f"Saving files for {month_name} {year} to {output_dir}")
        
        try:
            # Ensure the output directory exists
            self.ensure_directory_exists(output_dir)
            
            # Determine the full month name
            month_num = self.validate_month_name(month_name)
            full_month_name = calendar.month_name[month_num]
            
            # Save the CSV file
            csv_filename = os.path.join(output_dir, f"data_{full_month_name.lower()}_{year}.csv")
            result_df.to_csv(csv_filename, index=False)
            self.logger.info(f"Data saved to {csv_filename}")
            print(f"Data saved to {csv_filename}")
            
            # Generate and save verification file
            verification_text = self.generate_verification_info(result_df, full_month_name, month_num, year)
            verification_filename = os.path.join(output_dir, 'verification.txt')
            with open(verification_filename, 'w') as f:
                f.write(verification_text)
            self.logger.info(f"Verification information saved to {verification_filename}")
            print(f"Verification information saved to {verification_filename}")
            
            return csv_filename, verification_filename
        except Exception as e:
            self.logger.error(f"Error in {self.__class__.__name__}.save_files: {str(e)}")
            raise


class WorkflowManager:
    """Class that manages the workflow of the data generation process."""
    
    def __init__(self, logger=None):
        """Initialize the workflow manager with a generator and logger."""
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.generator = PropertyDataGenerator(self.logger)
    
    def parse_arguments(self):
        """Parse command line arguments."""
        self.logger.info("Parsing command line arguments")
        try:
            parser = argparse.ArgumentParser(description='Generate property pricing data for a specific month.')
            parser.add_argument('--month', type=str, help='Month name (e.g., January or Jan)')
            parser.add_argument('--year', type=int, default=2025, help='Year (default: 2025)')
            parser.add_argument('--output_dir', type=str, default='output', help='Directory for output files')
            return parser.parse_args()
        except Exception as e:
            self.logger.error(f"Error in {self.__class__.__name__}.parse_arguments: {str(e)}")
            raise
    
    def get_month_and_year(self, args):
        """Get month and year from arguments or user input."""
        self.logger.info("Determining month and year for data generation")
        
        # Get month
        if args.month:
            month_name = args.month.strip().title()
            month_num = self.generator.validate_month_name(month_name)
            if not month_num:
                self.logger.warning(f"Invalid month from command line: {month_name}")
                month_name, month_num = self.generator.get_month_input()
        else:
            self.logger.info("No month provided in arguments, requesting from user")
            month_name, month_num = self.generator.get_month_input()
        
        # Get year
        year = args.year
        self.logger.info(f"Using year: {year}")
        
        return month_name, month_num, year
    
    def execute(self):
        """Execute the full workflow."""
        self.logger.info("Starting property data generation workflow")
        
        try:
            # Parse arguments
            args = self.parse_arguments()
            
            # Get month and year
            month_name, month_num, year = self.get_month_and_year(args)
            full_month_name = calendar.month_name[month_num]
            
            # Generate data
            print(f"\nGenerating data for {full_month_name} {year}...")
            self.logger.info(f"Generating data for {full_month_name} {year}")
            result = self.generator.repeat_data_by_month(month_name, year)
            
            if result is not None:
                # Print summary
                print(f"\nData generated successfully:")
                print(f"- Month: {full_month_name} {year}")
                print(f"- Total rows: {len(result)}")
                print(f"- Days in month: {calendar.monthrange(year, month_num)[1]}")
                print(f"- Properties per day: {len(result) // calendar.monthrange(year, month_num)[1]}")
                
                # Save files
                self.generator.save_files(result, month_name, year, args.output_dir)
                self.logger.info("Workflow completed successfully")
            else:
                self.logger.error(f"Data generation failed in {self.__class__.__name__}.execute")
                print("Data generation failed. Check the logs for details.")
                
        except Exception as e:
            self.logger.error(f"Error in workflow execution {self.__class__.__name__}.execute: {str(e)}", exc_info=True)
            print(f"An error occurred: {str(e)}")
            print("Check the logs for more details.")


def main1():
    """Main function to run the script."""
    logger = logging.getLogger("PropertyDataGeneratorTool")
    logger.info("Starting application")
    
    try:
        # Initialize and execute the workflow
        workflow = WorkflowManager(logger)
        workflow.execute()
        logger.info("Application completed successfully")
    except Exception as e:
        logger.critical(f"Unhandled exception in main: {str(e)}", exc_info=True)
        print(f"Critical error occurred: {str(e)}")
        return 1
    
    return 0


#if __name__ == "__main1__":
   # sys.exit(main1())