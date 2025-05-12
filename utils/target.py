# hptargeting.py
# Process impression data with event information and create formatted output
import csv
import datetime
import sys
import os
import re
import logging
from dateutil import parser as date_parser


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("hp_targeting.log"),
        logging.StreamHandler(sys.stdout)
    ]
)


class CSVProcessor:
    """Class that handles all CSV processing functionality."""
    
    def __init__(self, logger=None):
        """Initialize the processor with a logger."""
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        
        # Define the fixed values for output
        self.fixed_values = {
            'property': 'HP_TARGETING 1',
            'bu': 'PERSONAL CARE',
            'page': 'HOME',
            'price_type': 'CPM'
        }
        
        # Initialize verification data
        self.verification_data = {
            'total_supply': 0,
            'total_allocation': 0,
            'total_rate_times_impressions': 0,
            'events': set(),
            'dates': [],
            'total_rows': 0,
            'skipped_rows': 0
        }
    
    def parse_date(self, date_str):
        """Parse date string to formatted date."""
        try:
            date_obj = date_parser.parse(date_str)
            new_date_format = date_obj.strftime('%d-%m-%Y')
            return new_date_format, date_obj
        except ValueError as e:
            self.logger.error(f"Date parsing error in {self.__class__.__name__}.parse_date: {str(e)}, date_str: {date_str}")
            return None, None
    
    def parse_impressions(self, impressions_str):
        """Parse impressions string to numeric value."""
        try:
            return float(impressions_str)
        except ValueError as e:
            self.logger.error(f"Impressions parsing error in {self.__class__.__name__}.parse_impressions: {str(e)}, value: {impressions_str}")
            return None
    
    def parse_rate(self, rate_str):
        """Parse rate string to numeric value."""
        try:
            return float(rate_str)
        except ValueError as e:
            self.logger.error(f"Rate parsing error in {self.__class__.__name__}.parse_rate: {str(e)}, value: {rate_str}")
            return None
    
    def process_row(self, row):
        """Process a single row of input data."""
        if len(row) < 4:  # Now expecting 4 columns: Date, Impressions, event, rate
            self.logger.warning(f"Insufficient columns in row: {row} in {self.__class__.__name__}.process_row")
            self.verification_data['skipped_rows'] += 1
            return None
        
        # Extract date, impressions, event, and rate
        date_str, impressions_str, event, rate_str = row[0], row[1], row[2], row[3]
        
        # Parse date
        new_date_format, date_obj = self.parse_date(date_str)
        if not new_date_format:
            self.verification_data['skipped_rows'] += 1
            return None
        
        # Parse impressions
        impressions = self.parse_impressions(impressions_str)
        if impressions is None:
            self.verification_data['skipped_rows'] += 1
            return None
        
        # Parse rate
        rate = self.parse_rate(rate_str)
        if rate is None:
            self.verification_data['skipped_rows'] += 1
            return None
        
        # Calculate supply (impressions * 10^6)
        supply = int(impressions * 1000000)
        
        # Allocation equals supply
        allocation = supply
        
        # Set impressions to 0 in output as requested
        output_impressions = 0
        
        # Update verification data
        self.verification_data['total_supply'] += supply
        self.verification_data['total_allocation'] += allocation
        self.verification_data['total_rate_times_impressions'] += float(rate) * impressions / 1000
        self.verification_data['events'].add(event)
        self.verification_data['dates'].append(date_obj)
        self.verification_data['total_rows'] += 1
        
        # Create output row
        return [
            new_date_format, 
            event,  # Use the event from input
            self.fixed_values['property'],
            self.fixed_values['bu'],
            self.fixed_values['page'],
            self.fixed_values['price_type'],
            supply,
            allocation,
            output_impressions,  # Always 0 as requested
            rate
        ]
    
    def process_csv(self, input_file, output_file):
        """Process the CSV file and generate output."""
        self.logger.info(f"Processing CSV: {input_file} -> {output_file}")
        
        try:
            with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
                reader = csv.reader(infile)
                writer = csv.writer(outfile)
                
                # Write header row
                header = ['date', 'event', 'property', 'bu', 'page', 'price_type', 'supply', 'allocation', 'impressions', 'rate']
                writer.writerow(header)
                self.logger.info(f"Writing header: {header}")
                
                # Skip header row
                try:
                    headers = next(reader)
                    self.logger.info(f"Skipped header row: {headers}")
                except StopIteration:
                    self.logger.warning("Input file is empty or has only a header row")
                    return False
                
                # Process each row
                for row in reader:
                    output_row = self.process_row(row)
                    if output_row:
                        writer.writerow(output_row)
            
            self.logger.info(f"Processing complete. {self.verification_data['total_rows']} rows processed, {self.verification_data['skipped_rows']} rows skipped.")
            return True
        except Exception as e:
            self.logger.error(f"Error in {self.__class__.__name__}.process_csv: {str(e)}", exc_info=True)
            return False
    
    def generate_verification_info(self, input_file, output_file):
        """Generate verification information about the processed data."""
        self.logger.info("Generating verification information")
        
        try:
            # Calculate date range
            date_range = ""
            if self.verification_data['dates']:
                min_date = min(self.verification_data['dates']).strftime('%d-%m-%Y')
                max_date = max(self.verification_data['dates']).strftime('%d-%m-%Y')
                date_range = f"{min_date} to {max_date}"
            
            # Format numbers for better readability
            formatted_supply = f"{self.verification_data['total_supply']:,} ({self.verification_data['total_supply']/10000000:.2f} crores)"
            formatted_allocation = f"{self.verification_data['total_allocation']:,} ({self.verification_data['total_allocation']/10000000:.2f} crores)"
            formatted_rate_imp = f"{self.verification_data['total_rate_times_impressions']:,.2f} ({self.verification_data['total_rate_times_impressions']/10000000:.2f} crores)"
            
            verification_text = f"""Verification Report
====================
Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Input file: {input_file}
Output file: {output_file}

Processed Data Summary:
---------------------
Total rows processed: {self.verification_data['total_rows']}
Rows skipped due to errors: {self.verification_data['skipped_rows']}
Date range: {date_range}
Events: {', '.join(sorted(self.verification_data['events']))}

Financial Summary:
----------------
Total Supply: {formatted_supply}
Total Allocation: {formatted_allocation}
Total Rate*Impressions/1000: {formatted_rate_imp}

Fixed Properties Used:
-------------------
Property: {self.fixed_values['property']}
Business Unit: {self.fixed_values['bu']}
Page: {self.fixed_values['page']}
Price Type: {self.fixed_values['price_type']}
"""
            return verification_text
        except Exception as e:
            self.logger.error(f"Error in {self.__class__.__name__}.generate_verification_info: {str(e)}")
            return "Error generating verification information."
    
    def save_verification_file(self, verification_text, output_dir='.'):
        """Save verification information to a file."""
        self.logger.info("Saving verification information")
        
        try:
            # Ensure the output directory exists
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
                self.logger.info(f"Created directory: {output_dir}")
            
            # Save the verification file
            verification_filename = os.path.join(output_dir, 'HP_TARGETINGverification.txt')
            with open(verification_filename, 'w') as f:
                f.write(verification_text)
            
            self.logger.info(f"Verification information saved to {verification_filename}")
            print(f"Verification information saved to {verification_filename}")
            
            return verification_filename
        except Exception as e:
            self.logger.error(f"Error in {self.__class__.__name__}.save_verification_file: {str(e)}")
            return None


class WorkflowManager:
    """Class that manages the workflow of the CSV processing."""
    
    def __init__(self, logger=None):
        """Initialize the workflow manager with a processor and logger."""
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.processor = CSVProcessor(self.logger)
    
    def validate_arguments(self, args):
        """Validate command line arguments."""
        self.logger.info("Validating command line arguments")
        
        # Now we only need input and output file paths
        if len(args) < 3:
            self.logger.error(f"Insufficient arguments in {self.__class__.__name__}.validate_arguments")
            return False
     
        input_file, output_file = args[1], args[2]
        
        # Check if input file exists
        if not os.path.exists(input_file):
            self.logger.error(f"Input file does not exist: {input_file} in {self.__class__.__name__}.validate_arguments")
            return False
        
        return True
    
    def execute(self, args):
        """Execute the full workflow."""
        self.logger.info("Starting HP targeting workflow")
        
        # Validate arguments
        if not self.validate_arguments(args):
            print("Usage: python hptargeting.py input.csv output.csv")
            print("Example: python hptargeting.py input.csv output.csv")
            self.logger.error("Invalid arguments. Workflow aborted.")
            return False
        
        input_file = args[1]
        output_file = args[2]
        
        try:
            # Process the CSV file - no rate needed now
            success = self.processor.process_csv(input_file, output_file)
            
            if success:
                # Print success message
                print(f"Conversion complete. Output saved to {output_file}")
                
                # Print verification information to console
                print("\n--- VERIFICATION INFORMATION ---")
                print(f"Total Supply: {self.processor.verification_data['total_supply']:,} " + 
                      f"({self.processor.verification_data['total_supply']/10000000:.2f} crores)")
                print(f"Total Allocation: {self.processor.verification_data['total_allocation']:,} " + 
                      f"({self.processor.verification_data['total_allocation']/10000000:.2f} crores)")
                print(f"Total Rate*Impressions/1000: {self.processor.verification_data['total_rate_times_impressions']:,.2f} " + 
                      f"({self.processor.verification_data['total_rate_times_impressions']/10000000:.2f} crores)")
                
                # Generate and save verification file
                verification_text = self.processor.generate_verification_info(input_file, output_file)
                self.processor.save_verification_file(verification_text)
                
                self.logger.info("Workflow completed successfully")
                return True
            else:
                self.logger.error(f"CSV processing failed in {self.__class__.__name__}.execute")
                print("Processing failed. Check the logs for details.")
                return False
                
        except Exception as e:
            self.logger.error(f"Error in workflow execution {self.__class__.__name__}.execute: {str(e)}", exc_info=True)
            print(f"An error occurred: {str(e)}")
            print("Check the logs for more details.")
            return False


def main():
    """Main function to run the script."""
    logger = logging.getLogger("HPTargetingTool")
    logger.info("Starting application")
    
    try:
        # Initialize and execute the workflow
        workflow = WorkflowManager(logger)
        success = workflow.execute(sys.argv)
        
        if success:
            logger.info("Application completed successfully")
            return 0
        else:
            logger.warning("Application completed with warnings or errors")
            return 1
            
    except Exception as e:
        logger.critical(f"Unhandled exception in main: {str(e)}", exc_info=True)
        print(f"Critical error occurred: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())