import pandas as pd

try:
    df = pd.read_excel("datasets/providers_data.xlsx", engine="openpyxl")
    print("✅ openpyxl is working. First few rows:")
    print(df.head())
except Exception as e:
    print("❌ openpyxl still not working:", e)