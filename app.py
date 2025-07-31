import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Database configuration
DB_CONFIG = {
    "dbname": "food_waste_db",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5432"
}

# Excel file paths
EXCEL_FILES = {
    "providers": "datasets/providers_data.xlsx",
    "receivers": "datasets/receivers_data.xlsx",
    "food_listings": "datasets/food_listings_data.xlsx",
    "claims": "datasets/claims_data.xlsx"
}

def create_database():
    """Create the database if it doesn't exist"""
    try:
        conn = psycopg2.connect(dbname="postgres", user="postgres", password="1234", host="localhost")
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("CREATE DATABASE IF NOT EXISTS food_waste_db;")
        cur.close()
        conn.close()
    except Exception as e:
        st.error(f"Error creating database: {e}")

def create_tables():
    """Create all required tables with proper column names"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Drop tables if they exist (for development)
    cur.execute("DROP TABLE IF EXISTS claims;")
    cur.execute("DROP TABLE IF EXISTS food_listings;")
    cur.execute("DROP TABLE IF EXISTS receivers;")
    cur.execute("DROP TABLE IF EXISTS providers;")
    
    # Create tables with proper column names (matching Excel files)
    cur.execute("""
        CREATE TABLE providers (
            provider_id INT PRIMARY KEY,
            name VARCHAR(100),
            type VARCHAR(50),
            address TEXT,
            City VARCHAR(50),  -- Note the capital 'C' in City
            contact VARCHAR(50)
        );
    """)
    
    cur.execute("""
        CREATE TABLE receivers (
            receiver_id INT PRIMARY KEY,
            name VARCHAR(100),
            type VARCHAR(50),
            City VARCHAR(50),  -- Note the capital 'C' in City
            contact VARCHAR(50)
        );
    """)
    
    cur.execute("""
        CREATE TABLE food_listings (
            listing_id INT PRIMARY KEY,
            food_name VARCHAR(100),
            quantity INT,
            expiry_date DATE,
            provider_id INT REFERENCES providers(provider_id),
            provider_type VARCHAR(50),
            location VARCHAR(50),
            food_type VARCHAR(50),
            meal_type VARCHAR(50)
        );
    """)
    
    cur.execute("""
        CREATE TABLE claims (
            claim_id INT PRIMARY KEY,
            listing_id INT REFERENCES food_listings(listing_id),
            receiver_id INT REFERENCES receivers(receiver_id),
            status VARCHAR(20),
            timestamp TIMESTAMP,
            quantity INT)
        );
    """)
    
    conn.commit()
    conn.close()

def load_data():
    """Load data from Excel files into database"""
    conn = psycopg2.connect(**DB_CONFIG)
    
    for table, file_path in EXCEL_FILES.items():
        df = pd.read_excel(file_path)
        
        # Convert date columns
        if 'expiry_date' in df.columns:
            df['expiry_date'] = pd.to_datetime(df['expiry_date']).dt.date
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Insert data
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                columns = ', '.join([f'"{col}"' for col in row.index])  # Quote column names
                placeholders = ', '.join(['%s'] * len(row))
                sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders});"
                cur.execute(sql, tuple(row))
        
        conn.commit()
        st.success(f"Loaded {len(df)} rows into {table}")
    
    conn.close()

def run_query(sql, params=None):
    """Execute a SQL query and return results"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        df = pd.read_sql_query(sql, conn, params=params)
        return df
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

# Initialize database
if st.button("Initialize Database"):
    with st.spinner("Setting up database..."):
        create_database()
        create_tables()
        load_data()
    st.success("Database initialized successfully!")

# All 15 queries with proper column names
QUERIES = {
    "1. Providers & Receivers in each city": """
        SELECT 
            COALESCE(p."City", r."City") AS city,
            COUNT(DISTINCT p.provider_id) AS total_providers,
            COUNT(DISTINCT r.receiver_id) AS total_receivers
        FROM providers p
        FULL JOIN receivers r ON p."City" = r."City"
        GROUP BY COALESCE(p."City", r."City");
    """,
    "2. Top food-contributing provider type": """
        SELECT 
            p.type AS provider_type,
            SUM(f.quantity) AS total_food_provided
        FROM providers p
        JOIN food_listings f ON p.provider_id = f.provider_id
        GROUP BY p.type
        ORDER BY total_food_provided DESC;
    """,
    "3. Provider contact info in specific city": """
        SELECT name, type, address, contact 
        FROM providers 
        WHERE "City" = %s;
    """,
    "4. Receivers who claimed most food": """
        SELECT 
            r.name,
            SUM(c.quantity) AS total_claimed
        FROM receivers r
        JOIN claims c ON r.receiver_id = c.receiver_id
        GROUP BY r.name
        ORDER BY total_claimed DESC;
    """,
    "5. Total quantity of food available": """
        SELECT SUM(quantity) AS total_available 
        FROM food_listings;
    """,
    "6. City with most food listings": """
        SELECT 
            location AS city,
            COUNT(*) AS total_listings
        FROM food_listings
        GROUP BY location
        ORDER BY total_listings DESC
        LIMIT 1;
    """,
    "7. Most commonly available food types": """
        SELECT 
            food_type,
            COUNT(*) AS count
        FROM food_listings
        GROUP BY food_type
        ORDER BY count DESC;
    """,
    "8. Claims count per food type": """
        SELECT 
            f.food_type,
            COUNT(c.claim_id) AS total_claims
        FROM claims c
        JOIN food_listings f ON c.listing_id = f.listing_id
        GROUP BY f.food_type;
    """,
    "9. Provider with most successful claims": """
        SELECT 
            p.name,
            COUNT(c.claim_id) AS total_claims
        FROM providers p
        JOIN food_listings f ON p.provider_id = f.provider_id
        JOIN claims c ON f.listing_id = c.listing_id
        GROUP BY p.name
        ORDER BY total_claims DESC
        LIMIT 1;
    """,
    "10. Claims by status": """
        SELECT 
            status,
            COUNT(*) AS count
        FROM claims
        GROUP BY status;
    """,
    "11. Avg food claimed per receiver": """
        SELECT 
            r.name,
            AVG(c.quantity) AS avg_claimed
        FROM receivers r
        JOIN claims c ON r.receiver_id = c.receiver_id
        GROUP BY r.name;
    """,
    "12. Most claimed meal type": """
        SELECT 
            f.meal_type,
            SUM(c.quantity) AS total
        FROM food_listings f
        JOIN claims c ON f.listing_id = c.listing_id
        GROUP BY f.meal_type
        ORDER BY total DESC
        LIMIT 1;
    """,
    "13. Total food donated per provider": """
        SELECT 
            p.name,
            SUM(f.quantity) AS total_donated
        FROM providers p
        JOIN food_listings f ON p.provider_id = f.provider_id
        GROUP BY p.name
        ORDER BY total_donated DESC;
    """,
    "14. Total claims per provider": """
        SELECT 
            p.name,
            COUNT(c.claim_id) AS total_claims
        FROM providers p
        JOIN food_listings f ON p.provider_id = f.provider_id
        JOIN claims c ON f.listing_id = c.listing_id
        GROUP BY p.name;
    """,
    "15. Total food claimed per food type": """
        SELECT 
            f.food_type,
            SUM(c.quantity) AS total_claimed
        FROM claims c
        JOIN food_listings f ON c.listing_id = f.listing_id
        GROUP BY f.food_type
        ORDER BY total_claimed DESC;
    """
}

# Main interface
st.title("Food Waste Management System")

# Query interface
selected_query = st.selectbox("Select Query", list(QUERIES.keys()))

if selected_query:
    st.subheader(selected_query)
    
    # Handle parameterized queries
    params = None
    if "specific city" in selected_query:
        cities = run_query('SELECT DISTINCT "City" FROM providers;')
        selected_city = st.selectbox("Select City", cities['City'])
        params = (selected_city,)
    
    # Execute query
    df = run_query(QUERIES[selected_query], params)
    
    if not df.empty:
        st.dataframe(df)
    else:
        st.warning("No data returned for this query")