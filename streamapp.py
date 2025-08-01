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
            SELECT "Type", COUNT(*) AS food_count
            FROM food_listings f
            JOIN providers p ON f."Provider_ID" = p."Provider_ID"
            GROUP BY "Type"
            ORDER BY food_count DESC;
        """,
        "Provider Contacts in a City": """
            SELECT "Name", "Contact", "City"
            FROM providers
            WHERE "City" ILIKE 'Chennai';
        """,
        "Top Receivers by Claims": """
            SELECT r."Name", COUNT(*) AS claim_count
            FROM claims c
            JOIN receivers r ON c."Receiver_ID" = r."Receiver_ID"
            GROUP BY r."Name"
            ORDER BY claim_count DESC;
        """,
        "Total Food Available": """
            SELECT SUM("Quantity") AS total_quantity_available FROM food_listings;
        """,
        "City with Most Listings": """
            SELECT "Location", COUNT(*) AS listings
            FROM food_listings
            GROUP BY "Location"
            ORDER BY listings DESC;
        """,
        "Common Food Types": """
            SELECT "Food_Type", COUNT(*) AS count
            FROM food_listings
            GROUP BY "Food_Type"
            ORDER BY count DESC;
        """,
        "Claims per Food Item": """
            SELECT f."Food_Name", COUNT(*) AS claims
            FROM claims c
            JOIN food_listings f ON c."Food_ID" = f."Food_ID"
            GROUP BY f."Food_Name"
            ORDER BY claims DESC;
        """,
        "Top Provider by Claims": """
            SELECT p."Name", COUNT(*) AS claim_count
            FROM claims c
            JOIN food_listings f ON c."Food_ID" = f."Food_ID"
            JOIN providers p ON f."Provider_ID" = p."Provider_ID"
            WHERE c."Status" = 'Completed'
            GROUP BY p."Name"
            ORDER BY claim_count DESC;
        """,
        "Claim Status Breakdown": """
            SELECT "Status", COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS percentage
            FROM claims
            GROUP BY "Status";
        """,
        "Avg Claimed Quantity per Receiver": """
            SELECT r."Name", AVG(f."Quantity") AS avg_quantity
            FROM claims c
            JOIN food_listings f ON c."Food_ID" = f."Food_ID"
            JOIN receivers r ON c."Receiver_ID" = r."Receiver_ID"
            GROUP BY r."Name"
            ORDER BY avg_quantity DESC;
        """,
        "Most Claimed Meal Type": """
            SELECT f."Meal_Type", COUNT(*) AS claim_count
            FROM claims c
            JOIN food_listings f ON c."Food_ID" = f."Food_ID"
            GROUP BY f."Meal_Type"
            ORDER BY claim_count DESC;
        """,
        "Total Donated Quantity per Provider": """
            SELECT p."Name", SUM(f."Quantity") AS total_donated
            FROM food_listings f
            JOIN providers p ON f."Provider_ID" = p."Provider_ID"
            GROUP BY p."Name"
            ORDER BY total_donated DESC;
        """,
        "Receivers by City": """
            SELECT "City", COUNT(*) AS receiver_count
            FROM receivers
            GROUP BY "City"
            ORDER BY receiver_count DESC;
        """,
        "Top 5 Foods with Most Quantity": """
            SELECT "Food_Name", SUM("Quantity") AS total_quantity
            FROM food_listings
            GROUP BY "Food_Name"
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

    locations = run_query("SELECT DISTINCT \"Location\" FROM food_listings;")["Location"].tolist()
    providers = run_query("SELECT DISTINCT \"Name\" FROM providers;")["Name"].tolist()
    food_types = run_query("SELECT DISTINCT \"Food_Type\" FROM food_listings;")["Food_Type"].tolist()

    location = st.selectbox("Select Location", ["All"] + locations)
    provider = st.selectbox("Select Provider", ["All"] + providers)
    food_type = st.selectbox("Select Food Type", ["All"] + food_types)

    query = """
        SELECT f."Food_Name", f."Quantity", f."Expiry_Date", f."Location", f."Food_Type", p."Name" AS Provider
        FROM food_listings f
        JOIN providers p ON f."Provider_ID" = p."Provider_ID"
        WHERE 1=1
    """
    if location != "All":
        query += f" AND f.\"Location\" = '{location}'"
    if provider != "All":
        query += f" AND p.\"Name\" = '{provider}'"
    if food_type != "All":
        query += f" AND f.\"Food_Type\" = '{food_type}'"

    filtered_df = run_query(query)
    st.dataframe(filtered_df)

# Contact Info
elif section == "Contact Info":
    st.title("Contact Info")

    role = st.radio("Who do you want to contact?", ["Provider", "Receiver"])
    city = st.text_input("Enter City to Search")

    if role == "Provider":
        query = "SELECT \"Name\", \"City\", \"Contact\" FROM providers"
    else:
        query = "SELECT \"Name\", \"City\", \"Contact\" FROM receivers"

    if city:
        query += f" WHERE \"City\" ILIKE '%{city}%'"

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
                cursor = conn.cursor()
                cursor.execute(f"""
                    INSERT INTO food_listings 
                    ("Food_Name", "Quantity", "Expiry_Date", "Provider_ID", "Provider_Type", "Location", "Food_Type", "Meal_Type")
                    VALUES (%s, %s, %s, %s, 
                        (SELECT "Type" FROM providers WHERE "Provider_ID" = %s),
                        %s, %s, %s)
                """, (fname, qty, expiry, pid, pid, location, ftype, meal))
                conn.commit()
                st.success("Food added successfully")
            except Exception as e:
                st.error(f"Error: {e}")

    st.subheader("Delete Listing")
    del_id = st.number_input("Enter Food ID to delete", min_value=1)
    if st.button("Delete"):
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM food_listings WHERE \"Food_ID\" = %s", (del_id,))
            conn.commit()
            st.success("Deleted successfully")
        except Exception as e:
            st.error(f"Error: {e}")

# Data Analysis & Visualization
elif section == "Data Analysis & Visualization":
    st.title("Data Analysis & Visualization")

    st.subheader("Top Cities by Listings")
    city_data = run_query("""
        SELECT "Location", COUNT(*) AS listings
        FROM food_listings
        GROUP BY "Location"
        ORDER BY listings DESC;
    """)
    chart1 = alt.Chart(city_data).mark_bar().encode(
        x=alt.X("Location", sort='-y'),
        y="listings",
        tooltip=["Location", "listings"]
    ).properties(width=700, height=400)
    st.altair_chart(chart1)

    st.subheader("Food Type Distribution")
    food_type_data = run_query("""
        SELECT "Food_Type", COUNT(*) AS count
        FROM food_listings
        GROUP BY "Food_Type";
    """)
    chart2 = alt.Chart(food_type_data).mark_arc(innerRadius=50).encode(
        theta="count",
        color="Food_Type",
        tooltip=["Food_Type", "count"]
    ).properties(width=400, height=400)
    st.altair_chart(chart2)

    st.subheader("Claims by Status")
    status_data = run_query("""
        SELECT "Status", COUNT(*) AS count
        FROM claims
        GROUP BY "Status";
    """)
    chart3 = alt.Chart(status_data).mark_bar().encode(
        x="Status",
        y="count",
        color="Status",
        tooltip=["Status", "count"]
    ).properties(width=500, height=400)
    st.altair_chart(chart3)


