import streamlit as st
import psycopg2
import pandas as pd
import altair as alt
from contextlib import contextmanager

# ======================
# Supabase DB Connection
# ======================

@contextmanager
def get_db_connection():
    conn = psycopg2.connect(
        host=st.secrets["postgres"]["host"],
        dbname=st.secrets["postgres"]["dbname"],
        user=st.secrets["postgres"]["user"],
        password=st.secrets["postgres"]["password"],
        port=st.secrets["postgres"]["port"]
    )
    try:
        yield conn
    finally:
        conn.close()

# ======================
# Query Runner
# ======================
def run_query(query):
    with get_db_connection() as conn:
        return pd.read_sql_query(query, conn)

# Sidebar navigation
st.sidebar.title("Navigation")
section = st.sidebar.selectbox("Select Section", [
    "Query Dashboard",
    "Filter Food Donations",
    "Contact Info",
    "CRUD Operations",
    "Data Analysis & Visualization"
])

# Query Dashboard
if section == "Query Dashboard":
    st.title("Data Insights: Food Waste Management")

    query_options = {
        "Providers & Receivers by City": """
           SELECT
    COALESCE(p.city, r.city) AS city,
    COUNT(DISTINCT p.provider_id) AS total_providers,
    COUNT(DISTINCT r.receiver_id) AS total_receivers
FROM providers p
FULL OUTER JOIN receivers r ON p.city = r.city
GROUP BY COALESCE(p.city, r.city)
ORDER BY city;
        """,
        "Top Contributing Provider Type": """
            SELECT type, COUNT(*) AS food_count
            FROM food_listings f
            JOIN providers p ON f.provider_id = p.provider_id
            GROUP BY type
            ORDER BY food_count DESC;
        """,
        "Provider Contacts in a City": """
            SELECT name, contact, city
            FROM providers
            WHERE city ILIKE 'Chennai';
        """,
        "Top Receivers by Claims": """
            SELECT r.name, COUNT(*) AS claim_count
            FROM claims c
            JOIN receivers r ON c.receiver_id = r.receiver_id
            GROUP BY r.name
            ORDER BY claim_count DESC;
        """,
        "Total Food Available": """
            SELECT SUM(quantity) AS total_quantity_available FROM food_listings;
        """,
        "City with Most Listings": """
            SELECT location, COUNT(*) AS listings
            FROM food_listings
            GROUP BY location
            ORDER BY listings DESC;
        """,
        "Common Food Types": """
            SELECT food_type, COUNT(*) AS count
            FROM food_listings
            GROUP BY food_type
            ORDER BY count DESC;
        """,
        "Claims per Food Item": """
            SELECT f.food_name, COUNT(*) AS claims
            FROM claims c
            JOIN food_listings f ON c.food_id = f.food_id
            GROUP BY f.food_name
            ORDER BY claims DESC;
        """,
        "Top Provider by Claims": """
            SELECT p.name, COUNT(*) AS claim_count
            FROM claims c
            JOIN food_listings f ON c.food_id = f.food_id
            JOIN providers p ON f.provider_id = p.provider_id
            WHERE c.status = 'Completed'
            GROUP BY p.name
            ORDER BY claim_count DESC;
        """,
        "Claim Status Breakdown": """
            SELECT status, COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS percentage
            FROM claims
            GROUP BY status;
        """,
        "Avg Claimed Quantity per Receiver": """
            SELECT r.name, AVG(f.quantity) AS avg_quantity
            FROM claims c
            JOIN food_listings f ON c.food_id = f.food_id
            JOIN receivers r ON c.receiver_id = r.receiver_id
            GROUP BY r.name
            ORDER BY avg_quantity DESC;
        """,
        "Most Claimed Meal Type": """
            SELECT f.meal_type, COUNT(*) AS claim_count
            FROM claims c
            JOIN food_listings f ON c.food_id = f.food_id
            GROUP BY f.meal_type
            ORDER BY claim_count DESC;
        """,
        "Total Donated Quantity per Provider": """
            SELECT p.name, SUM(f.quantity) AS total_donated
            FROM food_listings f
            JOIN providers p ON f.provider_id = p.provider_id
            GROUP BY p.name
            ORDER BY total_donated DESC;
        """,
        "Receivers by City": """
            SELECT city, COUNT(*) AS receiver_count
            FROM receivers
            GROUP BY city
            ORDER BY receiver_count DESC;
        """,
        "Top 5 Foods with Most Quantity": """
            SELECT food_name, SUM(quantity) AS total_quantity
            FROM food_listings
            GROUP BY food_name
            ORDER BY total_quantity DESC
            LIMIT 5;
        """
    }

    selected_query = st.selectbox("Choose a query", list(query_options.keys()))
    df = run_query(query_options[selected_query])
    st.dataframe(df)

# Filter Donations
elif section == "Filter Food Donations":
    st.title("Filter Food Donations")

    # Fix column names and quoting
    locations = run_query("SELECT DISTINCT location FROM food_listings;")["location"].tolist()
    providers = run_query("SELECT DISTINCT name FROM providers;")["name"].tolist()
    food_types = run_query("SELECT DISTINCT food_type FROM food_listings;")["food_type"].tolist()

    selected_location = st.selectbox("Select location", ["All"] + locations)
    selected_provider = st.selectbox("Select provider", ["All"] + providers)
    selected_food_type = st.selectbox("Select food type", ["All"] + food_types)

    query = """
        SELECT f.food_name, f.quantity, f.expiry_date, f.location, f.food_type, p.name AS provider
        FROM food_listings f
        JOIN providers p ON f.provider_id = p.provider_id
        WHERE 1=1
    """

    if selected_location != "All":
        query += f" AND f.location = '{selected_location}'"
    if selected_provider != "All":
        query += f" AND p.name = '{selected_provider}'"
    if selected_food_type != "All":
        query += f" AND f.food_type = '{selected_food_type}'"

    filtered_df = run_query(query)
    st.dataframe(filtered_df)

# Contact Info
elif section == "Contact Info":
    st.title("Contact Info")

    role = st.radio("Who do you want to contact?", ["Provider", "Receiver"])
    city = st.text_input("Enter City to Search")

    if role == "Provider":
        query = "SELECT name, city, contact FROM providers"
    else:
        query = "SELECT name, city, contact FROM receivers"

    if city:
        query += f" WHERE city ILIKE '%{city}%'"

    contact_df = run_query(query)
    st.dataframe(contact_df)

# CRUD Operations
elif section == "CRUD Operations":
    st.title("Manage Food Listings")

    st.subheader("Add New Listing")
    with st.form("add_form"):
        fname = st.text_input("Food Name")
        qty = st.number_input("Quantity", min_value=1)
        expiry = st.date_input("Expiry Date")
        pid = st.number_input("Provider ID", min_value=1)
        location = st.text_input("Location")
        ftype = st.selectbox("Food Type", ["Vegetarian", "Non-Vegetarian", "Vegan"])
        meal = st.selectbox("Meal Type", ["Breakfast", "Lunch", "Dinner", "Snacks"])

        if st.form_submit_button("Add Food"):
            try:
                with get_db_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO food_listings 
                        (food_name, quantity, expiry_date, provider_id, provider_type, location, food_type, meal_type)
                        VALUES (%s, %s, %s, %s, 
                            (SELECT type FROM providers WHERE provider_id = %s),
                            %s, %s, %s)
                    """, (fname, qty, expiry, pid, pid, location, ftype, meal))
                    conn.commit()
                st.success("✅ Food added successfully")
            except Exception as e:
                st.error(f"❌ Error: {e}")

    st.subheader("Delete Listing")
    del_id = st.number_input("Enter Food ID to delete", min_value=1)
    if st.button("Delete"):
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM food_listings WHERE food_id = %s", (del_id,))
                conn.commit()
            st.success("✅ Deleted successfully")
        except Exception as e:
            st.error(f"❌ Error: {e}")
# Data Analysis & Visualization
elif section == "Data Analysis & Visualization":
    st.title("Data Analysis & Visualization")

    # Top Cities by Listings
    st.subheader("Top Cities by Listings")
    city_data = run_query("""
        SELECT location, COUNT(*) AS listings
        FROM food_listings
        GROUP BY location
        ORDER BY listings DESC;
    """)
    chart1 = alt.Chart(city_data).mark_bar().encode(
        x=alt.X("location", sort='-y'),
        y="listings",
        tooltip=["location", "listings"]
    ).properties(width=700, height=400)
    st.altair_chart(chart1)

    # Food Type Distribution
    st.subheader("Food Type Distribution")
    food_type_data = run_query("""
        SELECT food_type, COUNT(*) AS count
        FROM food_listings
        GROUP BY food_type;
    """)
    chart2 = alt.Chart(food_type_data).mark_arc(innerRadius=50).encode(
        theta="count",
        color="food_type",
        tooltip=["food_type", "count"]
    ).properties(width=400, height=400)
    st.altair_chart(chart2)

    # Claims by Status
    st.subheader("Claims by Status")
    status_data = run_query("""
        SELECT status, COUNT(*) AS count
        FROM claims
        GROUP BY status;
    """)
    chart3 = alt.Chart(status_data).mark_bar().encode(
        x="status",
        y="count",
        color="status",
        tooltip=["status", "count"]
    ).properties(width=500, height=400)
    st.altair_chart(chart3)


