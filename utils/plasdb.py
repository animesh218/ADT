# data_processor.py
# Process multiple types of data from Excel input and generate formatted outputs
import pandas as pd
from datetime import datetime, timedelta
import re
import os
import sys
import logging
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_processor.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

class DataProcessor:
    """Base class for all data processors with common methods."""
    
    def __init__(self, output_dir="output", logger=None):
        """Initialize the processor with a logger."""
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.output_dir = output_dir
        
        # Ensure output directory exists
        self.ensure_directory_exists(output_dir)
        
        # Initialize verification data
        self.verification_data = {}
    
    def ensure_directory_exists(self, directory):
        """Ensure the output directory exists."""
        if not os.path.exists(directory):
            try:
                os.makedirs(directory)
                self.logger.info(f"Created directory: {directory}")
            except Exception as e:
                self.logger.error(f"Error creating directory: {str(e)}")
                raise
    
    @staticmethod
    def clean_currency(value):
        """Clean currency values by removing symbols and formatting."""
        if isinstance(value, str):
            # Remove currency symbols, spaces, and convert to float
            return float(re.sub(r'[₹\s,]', '', value))
        return float(value)
    
    def save_verification_info(self, output_filename="plasdbverification.txt"):
        """Save verification information to a file."""
        try:
            verification_text = f"""Verification Report for {self.__class__.__name__}
====================
Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

"""
            # Add all verification data
            for key, value in self.verification_data.items():
                if isinstance(value, float):
                    verification_text += f"{key}: {value:,.2f}\n"
                else:
                    verification_text += f"{key}: {value}\n"
            
            verification_path = os.path.join(self.output_dir, output_filename)
            
            with open(verification_path, 'a') as f:
                f.write(verification_text + "\n\n")
            
            self.logger.info(f"Verification information saved to {verification_path}")
            
            return verification_path
        except Exception as e:
            self.logger.error(f"Error saving verification file: {str(e)}")
            return None


class PLAProcessor(DataProcessor):
    """Class that processes PLA data."""
    
    def __init__(self, output_dir="output", logger=None):
        """Initialize the PLA processor."""
        super().__init__(output_dir, logger)
        self.property_map = {
            "Men's Casual Wear": "PLA - MCW",
            "Men's Work Wear": "PLA - MWW",
            "Men's Essentials": "PLA - MEN'S ESSENTIALS",
            "International Brands": "PLA - IB",
            "Jewellery": "PLA - JEWELLERY",
            "Watches and Wearables": "PLA - WATCHES AND WEARABLES",
            "Women's LTA": "PLA - WOMEN'S LTA",
            "Men's LTA & Eyewear": "PLA - MEN'S LTA",
            "Lingerie and Loungewear": "PLA - LINGERIE AND LOUNGEWEAR",
            "Personal Care": "PLA - PC",
            "Home Furnishing": "PLA - HOME FURNISHING"
        }
    
    def process_data(self, input_df, total_days, start_date, event_map):
        """Process PLA data from input dataframe."""
        self.logger.info("Processing PLA data")
        
        try:
            # Get first column name
            first_col = input_df.columns[0]
            
            # Clean the price values
            input_df['Floor Price PLA'] = input_df['Floor Price PLA'].apply(self.clean_currency)
            
            # Create mappings
            bu_to_target = dict(zip(input_df[first_col], input_df['PLA TARGET']))
            bu_to_rate = dict(zip(input_df[first_col], input_df['Floor Price PLA']))
            
            # Prepare data
            rows = []
            total_monthly_target = 0
            
            for bu, rev in bu_to_target.items():
                if pd.notna(bu) and pd.notna(rev):  # Skip rows with NaN values
                    revenue_inr = float(rev) * 1e7  # Convert crores to INR
                    total_monthly_target += revenue_inr
                    daily_rev = revenue_inr / total_days
                    rate = float(bu_to_rate.get(bu, 0))
                    
                    if rate > 0:
                        daily_inventory = round(daily_rev / rate)
                        property_name = self.property_map.get(bu, "PLA")
            
                        for i in range(total_days):
                            date = (start_date + timedelta(days=i)).strftime('%Y-%m-%d')
                            # Get event name for this date - lookup in event_map or default to ALL
                            event_name = event_map.get(date, "ALL")
                            
                            rows.append({
                                "date": date,
                                "event": event_name,
                                "property": property_name,
                                "bu": bu,
                                "ALLOCATION": daily_inventory,
                                "SUPPLY": None,
                                "PAGE": "SEARCH",
                                "rate": rate,
                                "price_type": "CPC",
                                "impressions": 0
                            })
            
            # Create DataFrame
            df = pd.DataFrame(rows)
            
            if not df.empty:
                # Set SUPPLY = ALLOCATION for mapped properties
                mapped_properties = list(self.property_map.values())
                df.loc[df["property"].isin(mapped_properties), "SUPPLY"] = df["ALLOCATION"]
            
                # Compute daily total ALLOCATION for unmapped properties
                dates = df["date"].unique()
            
                for date in dates:
                    # Sum ALLOCATION for unmapped properties on this date
                    total_supply = df.loc[
                        (df["date"] == date) & (~df["property"].isin(mapped_properties)),
                        "ALLOCATION"
                    ].sum()
            
                    # Assign total_supply to SUPPLY for unmapped properties
                    df.loc[
                        (df["date"] == date) & (~df["property"].isin(mapped_properties)),
                        "SUPPLY"
                    ] = total_supply
                
                # Verification calculations
                first_date = df["date"].iloc[0]
                first_date_df = df[df["date"] == first_date]
                total_daily_revenue = (first_date_df["ALLOCATION"] * first_date_df["rate"]).sum()
                monthly_revenue = total_daily_revenue * total_days
                
                # Store verification data
                self.verification_data = {
                    "Processor Type": "PLA",
                    "Start Date": start_date.strftime('%Y-%m-%d'),
                    "Total Days": total_days,
                    "Total Business Units": len(bu_to_target),
                    "Total Rows Generated": len(df),
                    "Daily Revenue (First Day)": total_daily_revenue,
                    "Monthly Revenue (Projected)": monthly_revenue,
                    "Monthly Revenue (In Crores)": monthly_revenue/1e7,
                    "Target Monthly Revenue (In Crores)": total_monthly_target/1e7
                }
                
                # Print verification information
                print("\n=== PLA VERIFICATION ===")
                print(f"Daily Revenue (First Day): ₹{total_daily_revenue:,.2f}")
                print(f"Monthly Revenue (Projected): ₹{monthly_revenue:,.2f}")
                print(f"Monthly Revenue (In Crores): ₹{monthly_revenue/1e7:.2f} Cr")
                
                # Save to CSV
                output_filename = os.path.join(self.output_dir, "plasdb_output.csv")
                df.to_csv(output_filename, index=False)
                self.logger.info(f"PLA data saved to {output_filename}")
                
                return df
            else:
                self.logger.warning("No data to process for PLA")
                return None
                
        except Exception as e:
            self.logger.error(f"Error processing PLA data: {str(e)}", exc_info=True)
            return None

class MonetisedProcessor(DataProcessor):
    """Class that processes Monetised data."""
    
    def __init__(self, output_dir="output", logger=None, property_name="MONETISED"):
        """Initialize the Monetised processor."""
        super().__init__(output_dir, logger)
        self.property_name = property_name
        self.rate = 50
        self.monthly_revenue_base = 1e7  # 1 Cr = 10^7
    
    def process_data(self, input_df, total_days, start_date, event_map, column_name='SDA'):
        """Process Monetised data from input dataframe."""
        self.logger.info(f"Processing {self.property_name} data using column {column_name}")
        
        try:
            # Get first column name
            first_col = input_df.columns[0]
            
            # Extract required data
            valid_data = input_df[[first_col, column_name]].dropna()
            
            data = {
                "bu": valid_data[first_col].tolist(),
                "sda": valid_data[column_name].astype(float).tolist()
            }
            
            # Create DataFrame
            df = pd.DataFrame(data)
            
            # Calculate Revenue & Inventory
            df["monthly_revenue"] = df["sda"].astype(float) * self.monthly_revenue_base
            df["revenue_per_day"] = df["monthly_revenue"] / total_days
            df["allocation"] = (df["revenue_per_day"] * 1000) / self.rate
            df["allocation"] = df["allocation"].round().astype(int)
            
            # Add static fields
            df["rate"] = self.rate
            df["supply"] = 0  # Will be calculated next
            df["page"] = "SEARCH"
            df["price_type"] = "CPM"
            df["impressions"] = 0
            
            # Calculate total supply
            daily_supply = df["allocation"].sum()
            df["supply"] = daily_supply
            
            # Calculate total revenue
            total_daily_revenue = (df["allocation"] * df["rate"]).sum() / 1000  # Divide by 1000 for CPM rate
            monthly_revenue_calc = total_daily_revenue * total_days
            expected_monthly_revenue = df["sda"].sum() * self.monthly_revenue_base
            
            # Store verification data
            self.verification_data = {
                "Processor Type": self.property_name,
                "Start Date": start_date.strftime('%Y-%m-%d'),
                "Total Days": total_days,
                "Total Business Units": len(df),
                "Rate (CPM)": self.rate,
                "Daily Revenue": total_daily_revenue,
                "Monthly Revenue (Calculated)": monthly_revenue_calc,
                "Monthly Revenue (In Crores)": monthly_revenue_calc/1e7
            }
            
            # Print verification information
            print(f"\n=== {self.property_name} VERIFICATION ===")
            print(f"Daily Revenue: ₹{total_daily_revenue:,.2f}")
            print(f"Monthly Revenue (Calculated): ₹{monthly_revenue_calc:,.2f}")
            print(f"Monthly Revenue (In Crores): ₹{monthly_revenue_calc/1e7:.2f} Cr")
            
            # Expand for all days with proper event names
            final_rows = []
            for i in range(total_days):
                date = (start_date + timedelta(days=i)).strftime('%Y-%m-%d')
                event_name = event_map.get(date, "ALL")
                
                for _, row in df.iterrows():
                    final_rows.append({
                        "date": date,
                        "event": event_name,
                        "property": self.property_name,
                        "bu": row["bu"],
                        "rate": row["rate"],
                        "allocation": row["allocation"],
                        "supply": row["supply"],
                        "page": row["page"],
                        "price_type": row["price_type"],
                        "impressions": row["impressions"]
                    })
            
            final_df = pd.DataFrame(final_rows)
            
            # Save as CSV
            output_filename = os.path.join(self.output_dir, f"{self.property_name.lower()}_output.csv")
            final_df.to_csv(output_filename, index=False)
            self.logger.info(f"{self.property_name} data saved to {output_filename}")
            
            return final_df
            
        except Exception as e:
            self.logger.error(f"Error processing {self.property_name} data: {str(e)}", exc_info=True)
            return None

class WorkflowManager:
    """Class that manages the workflow of the data processing."""
    
    def __init__(self, logger=None):
        """Initialize the workflow manager with processors and logger."""
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.output_dir = "output"  # Default output directory
        
        # Initialize processors
        self.pla_processor = PLAProcessor(self.output_dir, self.logger)
        self.monetised_processor = MonetisedProcessor(self.output_dir, self.logger)
        self.zeroslot_processor = MonetisedProcessor(self.output_dir, self.logger, "MONETISED_ZEROSLOT")
    
    def parse_arguments(self):
        """Parse command line arguments."""
        self.logger.info("Parsing command line arguments")
        try:
            parser = argparse.ArgumentParser(description='Process data from Excel and generate multiple output files.')
            parser.add_argument('--output_dir', type=str, default="output", help='Directory for output files')
            return parser.parse_args()
        except Exception as e:
            self.logger.error(f"Error parsing arguments: {str(e)}")
            raise
    
    def load_data_from_excel(self, filename="plasdb.xlsx"):
        """Load data from Excel file with data and event sheets."""
        try:
            # Read data sheet
            data_df = pd.read_excel(filename, sheet_name="data")
            
            # Try to read event mapping sheet
            try:
                event_df = pd.read_excel(filename, sheet_name="eventname")
                
                # Create event mapping dictionary - handle date formatting explicitly
                event_map = {}
                for _, row in event_df.iterrows():
                    # Convert date to string in YYYY-MM-DD format, handling different date types
                    if isinstance(row['date'], datetime):
                        date_str = row['date'].strftime('%Y-%m-%d')
                    elif isinstance(row['date'], str):
                        # Try to parse the string as date and reformat
                        try:
                            date_obj = pd.to_datetime(row['date'])
                            date_str = date_obj.strftime('%Y-%m-%d')
                        except:
                            # If parsing fails, use as is
                            date_str = row['date']
                    else:
                        # For any other type, convert to string
                        date_str = str(row['date'])
                    
                    # Get the event name
                    if pd.notna(row['event']):
                        event_name = str(row['event']).strip()
                        event_map[date_str] = event_name
                
                self.logger.info(f"Loaded {len(event_map)} events from eventname sheet")
                print(f"Loaded {len(event_map)} events from eventname sheet")
                
                # Debug: Print first few events
                for i, (date, event) in enumerate(event_map.items()):
                    if i < 5:  # Print first 5 items only
                        print(f"  Event mapping: {date} -> {event}")
                    else:
                        break
                        
            except Exception as e:
                self.logger.warning(f"Could not load eventname sheet: {str(e)}")
                print(f"Warning: Could not load eventname sheet: {str(e)}")
                event_map = {}
            
            return data_df, event_map
        except Exception as e:
            self.logger.error(f"Error loading Excel file: {str(e)}")
            raise
    
    def find_zeroslot_column(self, df):
        """Find the zero slot column in the dataframe."""
        zeroslot_col = None
        for col in df.columns:
            if isinstance(col, str) and ('0th' in col or 'zeroslot' in col.lower() or 'zero slot' in col.lower()):
                zeroslot_col = col
                break
        
        if not zeroslot_col:
            self.logger.warning("Could not find SDA(0th slot) column. Using 'SDA(0th slot)' as default name.")
            zeroslot_col = 'SDA(0th slot)'
        
        return zeroslot_col
    
    def create_verification_summary(self):
        """Create a summary of all verification data in one file."""
        verification_path = os.path.join(self.output_dir, "verification.txt")
        
        with open(verification_path, 'w') as f:
            f.write(f"""Combined Verification Report
=======================
Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

""")
        
        return verification_path
    
    def execute(self):
        """Execute the full workflow."""
        self.logger.info("Starting data processing workflow")
        
        try:
            # Parse arguments
            args = self.parse_arguments()
            
            # Update output directory if specified
            if args.output_dir:
                self.output_dir = args.output_dir
                self.pla_processor.output_dir = self.output_dir
                self.monetised_processor.output_dir = self.output_dir
                self.zeroslot_processor.output_dir = self.output_dir
                
                # Ensure output directory exists
                if not os.path.exists(self.output_dir):
                    os.makedirs(self.output_dir)
            
            # Create verification summary file header
            verification_path = self.create_verification_summary()
            
            # Load data from Excel
            input_df, event_map = self.load_data_from_excel("utils/plasdb.xlsx")
            
            # Determine month and days from the data
            # Assume all dates in the event map are in the same month, or use current month as default
            if event_map:
                # Get first date from event map
                first_date_str = list(event_map.keys())[0]
                try:
                    first_date = datetime.strptime(first_date_str, '%Y-%m-%d')
                except ValueError:
                    try:
                        # Try alternative date formats
                        first_date = pd.to_datetime(first_date_str).to_pydatetime()
                    except:
                        # If all parsing fails, use current date
                        first_date = datetime.now()
                
                start_date = first_date.replace(day=1)  # First day of the month
            else:
                # Default to current month
                start_date = datetime.now().replace(day=1)
            
            # Calculate days in month
            month_days = {1:31, 2:28, 3:31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}
            # Adjust for leap year
            if start_date.month == 2 and start_date.year % 4 == 0 and (start_date.year % 100 != 0 or start_date.year % 400 == 0):
                month_days[2] = 29
            total_days = month_days[start_date.month]
            
            self.logger.info(f"Using start date: {start_date.strftime('%Y-%m-%d')}, total days: {total_days}")
            print(f"Using start date: {start_date.strftime('%Y-%m-%d')}, total days: {total_days}")
            
            # Find the zero slot column
            zeroslot_col = self.find_zeroslot_column(input_df)
            
            # Process PLA data
            print(f"\nProcessing PLA data for {total_days} days starting from {start_date.strftime('%Y-%m-%d')}...")
            pla_df = self.pla_processor.process_data(input_df, total_days, start_date, event_map)
            self.pla_processor.save_verification_info()
            
            # Process MONETISED data
            print(f"\nProcessing MONETISED data...")
            monetised_df = self.monetised_processor.process_data(input_df, total_days, start_date, event_map, 'SDA')
            self.monetised_processor.save_verification_info()
            
            # Process MONETISED_ZEROSLOT data
            print(f"\nProcessing MONETISED_ZEROSLOT data...")
            zeroslot_df = self.zeroslot_processor.process_data(input_df, total_days, start_date, event_map, zeroslot_col)
            self.zeroslot_processor.save_verification_info()
            
            # Print summary
            print("\n=== PROCESSING SUMMARY ===")
            total_processed = sum(1 for df in [pla_df, monetised_df, zeroslot_df] if df is not None)
            print(f"Total processors completed successfully: {total_processed} out of 3")
            print(f"All files saved to directory: {self.output_dir}")
            print(f"Verification information saved to: {verification_path}")
            
            self.logger.info("Workflow completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error in workflow execution: {str(e)}", exc_info=True)
            print(f"An error occurred: {str(e)}")
            print("Check the logs for more details.")
            return False


def main2():
    """Main function to run the script."""
    logger = logging.getLogger("DataProcessorTool")
    logger.info("Starting application")
    
    try:
        # Initialize and execute the workflow
        workflow = WorkflowManager(logger)
        success = workflow.execute()
        
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


#if __name__ == "__main2__":
    #sys.exit(main2())