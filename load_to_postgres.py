import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os

# Database configuration
DB_CONFIG = {
    "dbname": "postgres",  # Connect to default DB first
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5432"
}

# Dataset paths and expected columns
DATASETS = {
    "providers": {
        "path": "datasets/providers_data.xlsx",
        "columns": ["provider_id", "name", "type", "address", "city", "contact"]
    },
    "receivers": {
        "path": "datasets/receivers_data.xlsx",
        "columns": ["receiver_id", "name", "type", "city", "contact"]
    },
    "food_listings": {
        "path": "datasets/food_listings_data.xlsx",
        "columns": ["listing_id", "food_name", "quantity", "expiry_date", 
                   "provider_id", "provider_type", "location", "food_type", "meal_type"]
    },
    "claims": {
        "path": "datasets/claims_data.xlsx",
        "columns": ["claim_id", "listing_id", "receiver_id", "status", "timestamp", "quantity"]
    }
}

def create_database():
    """Create the food_waste_db database if it doesn't exist"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        # Check if database exists
        cur.execute("SELECT 1 FROM pg_database WHERE datname='food_waste_db'")
        exists = cur.fetchone()
        
        if not exists:
            cur.execute("CREATE DATABASE food_waste_db;")
            print("Database 'food_waste_db' created successfully")
        else:
            print("Database 'food_waste_db' already exists")
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error creating database: {e}")

def connect_to_food_waste_db():
    """Connect to the food_waste_db database"""
    config = DB_CONFIG.copy()
    config["dbname"] = "food_waste_db"
    try:
        return psycopg2.connect(**config)
    except Exception as e:
        print(f"Error connecting to food_waste_db: {e}")
        return None

def create_tables(conn):
    """Create all required tables with proper schema"""
    table_ddl = {
        "providers": """
            CREATE TABLE IF NOT EXISTS providers (
                provider_id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                type VARCHAR(50) NOT NULL,
                address TEXT,
                city VARCHAR(50) NOT NULL,
                contact VARCHAR(50) NOT NULL
            );
        """,
        "receivers": """
            CREATE TABLE IF NOT EXISTS receivers (
                receiver_id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                type VARCHAR(50) NOT NULL,
                city VARCHAR(50) NOT NULL,
                contact VARCHAR(50) NOT NULL
            );
        """,
        "food_listings": """
            CREATE TABLE IF NOT EXISTS food_listings (
                listing_id INT PRIMARY KEY,
                food_name VARCHAR(100) NOT NULL,
                quantity INT NOT NULL CHECK (quantity >= 0),
                expiry_date DATE NOT NULL,
                provider_id INT NOT NULL REFERENCES providers(provider_id),
                provider_type VARCHAR(50) NOT NULL,
                location VARCHAR(50) NOT NULL,
                food_type VARCHAR(50) NOT NULL,
                meal_type VARCHAR(50) NOT NULL
            );
        """,
        "claims": """
            CREATE TABLE IF NOT EXISTS claims (
                claim_id INT PRIMARY KEY,
                listing_id INT NOT NULL REFERENCES food_listings(listing_id),
                receiver_id INT NOT NULL REFERENCES receivers(receiver_id),
                status VARCHAR(20) NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                quantity INT NOT NULL CHECK (quantity > 0)
            );
        """
    }
    
    with conn.cursor() as cur:
        for table, ddl in table_ddl.items():
            try:
                cur.execute(ddl)
                print(f"Table '{table}' created successfully")
            except Exception as e:
                print(f"Error creating table '{table}': {e}")
        conn.commit()

def load_data_from_excel(conn):
    """Load data from Excel files into the database"""
    for table, config in DATASETS.items():
        try:
            # Read Excel file
            df = pd.read_excel(config["path"])
            print(f"\nLoading data for {table} from {config['path']}")
            
            # Check if all expected columns exist
            missing_cols = set(config["columns"]) - set(df.columns)
            if missing_cols:
                print(f"Warning: Missing columns in {table}: {missing_cols}")
                continue
                
            # Select only the expected columns
            df = df[config["columns"]]
            
            # Clean data
            df = df.dropna().drop_duplicates()
            
            # Convert date columns
            if "expiry_date" in df.columns:
                df["expiry_date"] = pd.to_datetime(df["expiry_date"]).dt.date
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
            
            # Prepare for insertion
            columns = df.columns
            rows = [tuple(x) for x in df.to_numpy()]
            
            # Build the insert query
            insert_sql = f"""
                INSERT INTO {table} ({', '.join(columns)})
                VALUES ({', '.join(['%s']*len(columns))})
                ON CONFLICT DO NOTHING;
            """
            
            # Execute batch insert
            with conn.cursor() as cur:
                cur.executemany(insert_sql, rows)
                conn.commit()
                print(f"Inserted {len(rows)} rows into {table}")
                
        except Exception as e:
            conn.rollback()
            print(f"Error loading data for {table}: {e}")

def verify_data(conn):
    """Verify data was loaded correctly"""
    with conn.cursor() as cur:
        print("\nVerifying data:")
        for table in DATASETS.keys():
            cur.execute(f"SELECT COUNT(*) FROM {table};")
            count = cur.fetchone()[0]
            print(f"Table '{table}' has {count} records")

def main():
    print("Starting database setup...")
    
    # Step 1: Create database if needed
    create_database()
    
    # Step 2: Connect to food_waste_db
    conn = connect_to_food_waste_db()
    if conn is None:
        return
    
    try:
        # Step 3: Create tables
        print("\nCreating tables...")
        create_tables(conn)
        
        # Step 4: Load data from Excel
        print("\nLoading data from Excel files...")
        load_data_from_excel(conn)
        
        # Step 5: Verify data
        verify_data(conn)
        
        print("\nDatabase setup completed successfully!")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()