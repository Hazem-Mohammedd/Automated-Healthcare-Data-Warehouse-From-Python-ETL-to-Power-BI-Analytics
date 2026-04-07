import os
import sys
import pandas as pd
import hashlib
from sqlalchemy import create_engine, text
import urllib
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# We need the SQL Server specific credentials
DB_SERVER = os.getenv("DB_SERVER", "localhost") # e.g., 'localhost', '.\SQLEXPRESS', or your machine name
DB_NAME = os.getenv("DB_NAME", "Healthcare_DW")
# Optional: if you are using SQL Server Authentication (username/password)
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# Create a database connection engine for Microsoft SQL Server
try:
    if DB_USER and DB_PASS:
        # If using SQL Server Authentication (Username & Password)
        encoded_password = urllib.parse.quote_plus(DB_PASS)
        connection_string = f"mssql+pyodbc://{DB_USER}:{encoded_password}@{DB_SERVER}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
    else:
        # If using Windows Authentication (Trusted Connection - Recommended for local SQL Server)
        connection_string = f"mssql+pyodbc://@{DB_SERVER}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes"
    
    engine = create_engine(connection_string, fast_executemany=True)
    print("Database connection engine created successfully.")
except Exception as e:
    print(f"Error creating database engine: {e}")
    sys.exit(1)


def extract_and_validate_csv(file_object): 
    print(f"Starting extraction and validation from uploaded file...")
    
    try:
        df = pd.read_csv(file_object)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return None

    # --- Amending column names
    df.columns = [col.replace(" ", "_") for col in df.columns]

    # --- 1. Data Cleaning ---
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()
        
    # --- FIX: Remove titles (Mr., Dr.) ignoring case, and fix capitalization
    # The regex ^(?i)(mr\.|dr\.)\s* means:
    # ^     : Start of the string
    # (?i)  : Ignore case (matches Mr, mR, MR, mr)
    # \s* : Remove any spaces immediately following the title
    prefix_pattern = r'^(?i)(mr\.|dr\.)\s*'
    
    if 'Name' in df.columns:
        df['Name'] = df['Name'].str.replace(prefix_pattern, '', regex=True).str.title()
        
    if 'Doctor' in df.columns:
        df['Doctor'] = df['Doctor'].str.replace(prefix_pattern, '', regex=True).str.title()

    df['Billing_Amount'] = pd.to_numeric(
        df['Billing_Amount'].astype(str).str.replace(r'[$,]', '', regex=True),
        errors='coerce'
    )
    
    # Fix the negative billing amount issue we found during EDA
    df['Billing_Amount'] = df['Billing_Amount'].abs()
    
    df['Age'] = pd.to_numeric(df['Age'], errors='coerce')
    
    # --- FIX: Match the actual Date Format from your dataset (dd-mm-yy) ---
    df['Date_of_Admission'] = pd.to_datetime(
        df['Date_of_Admission'], 
        format='%d-%m-%y', # Changed from %m/%d/%Y based on EDA
        errors='coerce'
    )
    df['Discharge_Date'] = pd.to_datetime(
        df['Discharge_Date'], 
        format='%d-%m-%y', # Changed from %m/%d/%Y based on EDA
        errors='coerce'
    )

    # --- 3. Deduplication ---
    initial_rows = len(df)
    df.drop_duplicates(inplace=True)
    if initial_rows > len(df):
        print(f"Dropped {initial_rows - len(df)} fully duplicate rows.")

    # --- NEW: Fix the "Aging Patient" Anomaly ---
    # Group by all columns EXCEPT Age to find the near-duplicates, 
    # then consolidate them by keeping the minimum age recorded for that event.
    initial_rows = len(df)
    group_cols = [col for col in df.columns if col != 'Age']
    
    if group_cols:
        agg_dict = {col: 'first' for col in df.columns if col not in group_cols}
        agg_dict['Age'] = 'min' # Keep the youngest age
        
        df = df.groupby(group_cols, as_index=False).agg(agg_dict)

    if initial_rows > len(df):
        print(f"Consolidated {initial_rows - len(df)} anomalous rows (conflicting ages).")
        
    # --- 4. Create the Unique Business Key ---
    # We now include ALL core columns to ensure minor variations 
    # (like different room numbers for the same admission) generate unique IDs
    key_cols = [
        'Name', 'Date_of_Admission', 'Doctor', 'Hospital', 
        'Medical_Condition', 'Billing_Amount', 'Room_Number'
    ]
    df['composite_key'] = df[key_cols].fillna('').astype(str).apply(lambda x: '|'.join(x), axis=1)
    df['SourceAdmissionID'] = df['composite_key'].apply(
        lambda x: hashlib.sha256(x.encode()).hexdigest()
    )
    df.drop(columns=['composite_key'], inplace=True)

    print(f"Validation complete. {len(df)} clean rows ready for staging.")
    return df


def load_to_staging(df, db_engine):
    print("Loading data to staging table...")
    try:
        with db_engine.connect() as conn:
            with conn.begin():
                # SQL Server TRUNCATE syntax
                conn.execute(text("TRUNCATE TABLE Staging_Admissions;"))
                # Load the data
                df.to_sql('Staging_Admissions', con=conn, if_exists='append', index=False)
        print(f"Successfully loaded {len(df)} rows to Staging_Admissions.")
    except Exception as e:
        print(f"Error loading to staging: {e}")
        raise


def run_data_warehouse_load(db_engine):
    print("Calling stored procedure sp_LoadDataWarehouse_Incremental...")
    try:
        with db_engine.connect() as conn:
            # SQL Server EXECUTE syntax
            conn.execute(text("EXEC sp_LoadDataWarehouse_Incremental;"))
            conn.commit() # Ensure the transaction commits in SQLAlchemy 2.0
        print("Data warehouse load procedure executed successfully.")
    except Exception as e:
        print(f"Error running stored procedure: {e}")
        raise


def main_etl_process(uploaded_file, db_engine):
    try:
        clean_df = extract_and_validate_csv(uploaded_file)
        if clean_df is not None and not clean_df.empty:
            load_to_staging(clean_df, db_engine)
            run_data_warehouse_load(db_engine)
            print("\nETL process completed successfully!")
            return True, f"Successfully loaded {len(clean_df)} new admission records."
        else:
            print("ETL process halted: No valid data to load.")
            return False, "ETL process halted: No valid data to load."
    except Exception as e:
        print(f"\nETL process FAILED: {e}")
        return False, f"ETL process FAILED: {e}"


if __name__ == "__main__":
    csv_file_path = 'healthcare_dataset.csv' # Pointing to your actual file
    print(f"Running ETL in standalone mode for: {csv_file_path}")
    
    try:
        with open(csv_file_path, 'rb') as f: 
            success, message = main_etl_process(f, engine)
        print(message)
    except FileNotFoundError:
        print(f"Error: Test file not found at {csv_file_path}")
    except Exception as e:
        print(f"Error in standalone run: {e}")