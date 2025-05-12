import numpy as np
import pandas as pd
import warnings
import os

# Suppress all warnings
warnings.filterwarnings("ignore")

# Display settings
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# File path
CAT_PAGES_FILE_PATH = r"C:\voiro-f\ADT\utils\cat.xlsx"

def remove_empty_rows_and_columns(df):
    """Remove completely empty rows and columns from DataFrame."""
    try:
        df = df.dropna(how='all')         # Remove empty rows
        df = df.dropna(how='all', axis=1) # Remove empty columns
    except Exception as e:
        print(f"Error removing empty rows and columns: {e}")
    return df

def remove_time_from_timestamp(df, timestamp_column):
    """Convert timestamp to date only."""
    try:
        df[timestamp_column] = pd.to_datetime(df[timestamp_column])
        df[timestamp_column] = df[timestamp_column].dt.date
    except Exception as e:
        print(f"Error handling timestamp: {e}")
    return df

def rename_columns_with_first_non_numeric_row(df):
    """Use the first appropriate row as column headers."""
    try:
        # Check if the current column names are already appropriate
        num_string_columns = df.columns.to_series().apply(
            lambda x: isinstance(x, str) and "Unnamed" not in x).sum()
        total_columns = len(df.columns)
        
        if num_string_columns > total_columns / 2:
            return df

        # Find first row with majority of string values
        for index, row in df.iterrows():
            non_null_values = row.notna().sum()
            string_count = row.apply(
                lambda x: isinstance(x, str) and "Unnamed" not in x).sum()

            if string_count > non_null_values / 2:
                new_column_names = row.values
                df.columns = new_column_names
                df = df.drop(index=range(0, index+1)).reset_index(drop=True)
                break

        return df
    except Exception as e:
        print(f"Error renaming columns: {e}")
        return df

def calculate_per_slot_rate(row):
    """Calculate per slot rate based on pricing type."""
    try:
        if row['price_type'] == 'CPM':
            row['Per_slot_rate'] = row['cpm_rate']
        elif row['price_type'] == 'CPD':
            row['Per_slot_rate'] = (row['cpm_rate'] * row['impressions']) / 1000
        else:
            row['Per_slot_rate'] = 0
        return row
    except Exception as e:
        print(f"Error calculating per slot rate: {e}")
        return row

def clean_dataframe(df):
    """Clean DataFrame by stripping whitespace and handling nulls."""
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    df = df.fillna(0)
    return df

def process_cat_pages():
    """Main function to process Category Pages data."""
    # Load the data
    print("Loading Category Pages data...")
    cat_pages_df = pd.read_excel(CAT_PAGES_FILE_PATH)
    
    # Clean and prepare data
    print("Cleaning and preparing data...")
    cat_pages_df = remove_empty_rows_and_columns(cat_pages_df)
    cat_pages_df = rename_columns_with_first_non_numeric_row(cat_pages_df)
    
    # Remove 'Traffic' column if present
    if 'Traffic' in cat_pages_df.columns:
        cat_pages_df.drop(columns='Traffic', inplace=True)
    
    # Extract rate card info (last 6 rows) and main data
    cat_pages_rate_card = cat_pages_df.iloc[-7:, :]
    cat_pages_df = cat_pages_df.iloc[:-7, :]
    
    # Process rate card
    cat_pages_rate_card = cat_pages_rate_card.rename(columns={'Date': 'Metric'})
    cat_pages_rate_card.drop(columns='Event', inplace=True)
    
    # Process main data
    cat_pages_df = remove_time_from_timestamp(cat_pages_df, "Date")
    
    # Reshape main data to long format
    print("Reshaping data...")
    cat_pages_final = cat_pages_df.melt(
        id_vars=['Date', 'Event'], 
        var_name='Property', 
        value_name='Impressions'
    )
    
    # Filter valid data
    cat_pages_final = cat_pages_final[
        (cat_pages_final['Date'].isna() == False) & 
        (cat_pages_final['Property'].isna() == False)
    ]
    
    # Round and convert impressions to integer
    cat_pages_final['Impressions'] = cat_pages_final['Impressions'].apply(lambda x: round(x, 0))
    cat_pages_final['Impressions'] = cat_pages_final['Impressions'].astype(int)
    
    # Reshape rate card
    cat_pages_rate_card_melted = cat_pages_rate_card.melt(
        id_vars=['Metric'], 
        var_name='Property', 
        value_name='Total_value'
    )
    
    # Pivot rate card to get one column for each metric
    cat_pages_rate_card_pivot = cat_pages_rate_card_melted.pivot(
        index='Property', 
        columns='Metric', 
        values='Total_value'
    ).reset_index()
    
    # Merge main data with rate card data
    print("Merging datasets...")
    cat_page_final_report = pd.merge(
        cat_pages_final, 
        cat_pages_rate_card_pivot, 
        on='Property', 
        how='left'
    )
    
    # Clean column names
    cat_page_final_report.rename(
        columns={col: col.strip().lower() for col in cat_page_final_report.columns}, 
        inplace=True
    )
    
    # Drop unnecessary columns
    cat_page_final_report.drop(columns=["revenue in cr", "total", "in mn"], inplace=True)
    
    # Further clean column names
    cat_page_final_report.columns = [col.strip() for col in cat_page_final_report.columns]
    
    # Set pricing type
    cat_page_final_report['price_type'] = "CPD"
    
    # Rename columns
    cat_page_final_report.rename(columns={'rate': 'cpm_rate'}, inplace=True)
    cat_page_final_report['supply'] = cat_page_final_report['no of slot']
    
    # Handle property names (remove trailing decimals)
    cat_page_final_report['property'] = cat_page_final_report['property'].str.replace(r'\.\d+', '', regex=True)
    
    # Calculate total slots by property and date
    allocation_sum = cat_page_final_report.groupby(['date', 'property'])['no of slot'].transform('sum')
    cat_page_final_report['supply'] = allocation_sum
    
    # Rename columns
    cat_page_final_report.rename(columns={'allocation': 'bu'}, inplace=True)
    
    # Calculate impressions per slot
    cat_page_final_report['impressions'] = cat_page_final_report['impressions'] / cat_page_final_report['supply']
    
    # Rename columns
    cat_page_final_report.rename(columns={'no of slot': 'allocation'}, inplace=True)
    cat_page_final_report.rename(columns={'supply': 'no of slot'}, inplace=True)
    
    # Calculate per slot rate
    cat_page_final_report = cat_page_final_report.apply(lambda row: calculate_per_slot_rate(row), axis=1)
    
    # Rename columns
    cat_page_final_report.rename(columns={'Per_slot_rate': 'rate'}, inplace=True)
    cat_page_final_report.rename(columns={'no of slot': 'supply'}, inplace=True)
    
    # Add page column
    # Load entire original Excel sheet
    full_df = pd.read_excel(CAT_PAGES_FILE_PATH)

    # Drop fully empty rows and get the last row
    last_row = full_df.dropna(how='all').iloc[-1]

    # Convert the last row into a dictionary (excluding 'Date', 'Event' columns if present)
    page_mapping = last_row.drop(labels=[col for col in ['Date', 'Event'] if col in last_row.index]).to_dict()

    # Clean property names if needed (to match cleaned property names in final report)
    page_mapping = {
        str(k).strip().replace(r'\.\d+', '', 1): v for k, v in page_mapping.items()
    }

    # Map to 'property' column
    cat_page_final_report['page'] = cat_page_final_report['property'].map(page_mapping)

    # Convert data types
    cat_page_final_report['impressions'] = cat_page_final_report['impressions'].astype(int)
    cat_page_final_report['rate'] = cat_page_final_report['rate'].apply(lambda x: int(round(x, 0)))
    
    # Calculate total revenue and impressions
    print("Calculating totals...")
    cat_page_final_report['total_revenue'] = cat_page_final_report['allocation'] * cat_page_final_report['rate']
    cat_page_final_report['total_impressions'] = cat_page_final_report['allocation'] * cat_page_final_report['impressions']
    
    # Generate verification data
    verification_data = generate_verification_data(cat_page_final_report)
    
    return cat_page_final_report, verification_data

def generate_verification_data(df):
    """Generate verification data for the processed DataFrame."""
    verification = {
        "total_rows": len(df),
        "total_revenue": df['total_revenue'].sum(),
        "total_revenue_in_cr": df['total_revenue'].sum() / 10**7,
        "total_impressions": df['total_impressions'].sum(),
        "total_impressions_in_mn": df['total_impressions'].sum() / 10**6,
        "unique_properties": df['property'].nunique(),
        "date_range": f"{df['date'].min()} to {df['date'].max()}",
        "missing_values": df.isna().sum().to_dict(),
        "zero_values": {col: (df[col] == 0).sum() for col in df.columns if df[col].dtype in [np.int64, np.float64]},
        "negative_values": {col: (df[col] < 0).sum() for col in df.columns if df[col].dtype in [np.int64, np.float64]}
    }
    
    return verification

def write_verification_file(verification_data, output_path="cat_verification.txt"):
    """Write verification data to a text file."""
    with open(output_path, 'w') as f:
        f.write("=== CATEGORY PAGES DATA VERIFICATION ===\n\n")
        
        f.write("=== SUMMARY STATISTICS ===\n")
        f.write(f"Total Rows: {verification_data['total_rows']}\n")
        f.write(f"Total Revenue: {verification_data['total_revenue']:,.2f}\n")
        f.write(f"Total Revenue (in cr): {verification_data['total_revenue_in_cr']:,.2f}\n")
        f.write(f"Total Impressions: {verification_data['total_impressions']:,.0f}\n")
        f.write(f"Total Impressions (in mn): {verification_data['total_impressions_in_mn']:,.2f}\n")
        f.write(f"Unique Properties: {verification_data['unique_properties']}\n")
        f.write(f"Date Range: {verification_data['date_range']}\n\n")
    
    print(f"Verification data saved to '{output_path}'")

def save_to_csv(df, output_file="outputcat.csv"):
    """Save DataFrame to CSV file."""
    df.to_csv(output_file, index=False)
    print(f"Report saved as '{output_file}'")
    return output_file

def main5():
    """Main function to process category pages data."""
    print("Starting Category Pages data processing...")
    
    # Process category pages data
    cat_page_final_report, verification_data = process_cat_pages()
    
    # Save to CSV file
    output_file = save_to_csv(cat_page_final_report)
    
    # Write verification file
    write_verification_file(verification_data)
    
    print("\nProcess completed successfully!")
    print(f"Generated report with {len(cat_page_final_report)} rows.")
    print(f"Data verification has been written to 'verification.txt'")
    
    return cat_page_final_report

#if __name__ == "__main__":
    # Clean main gate function that just calls the main function
    #print("Running Category Pages data processing utility...")
    #main()