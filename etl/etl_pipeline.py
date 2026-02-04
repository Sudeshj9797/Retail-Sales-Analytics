import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_CLEANED_DIR = os.path.join(BASE_DIR, "data", "cleaned")

print("🔥 ETL SCRIPT STARTED 🔥")

import pandas as pd
import pymysql

print("➡️ Import completed")

try:
    print("➡️ Trying to connect to MySQL...")

    conn = pymysql.connect(
        host="localhost",
        user="root",
        password="10908",
        database="retail_analytics",
        port=3306
    )

    cursor = conn.cursor()
    print("✅ Connected to MySQL")

except Exception as e:
    print("❌ MySQL connection failed")
    print(e)
    exit()


# category dimension
print("Loading dim_category...")
categories = pd.read_csv(os.path.join(DATA_CLEANED_DIR, "categories_cleaned.csv"))

for _, row in categories.iterrows():
    cursor.execute("""
        INSERT INTO dim_category (category_id, category_name)
        VALUES (%s, %s)
    """, (row["category_id"], row["category_name"]))

conn.commit()
print("✅ dim_category loaded")


# products dimension
print("Loading dim_products...")
products = pd.read_csv(os.path.join(DATA_CLEANED_DIR, "products_cleaned.csv"))

for _, row in products.iterrows():
    cursor.execute("""
        INSERT INTO dim_products
        (product_id, product_name, category_id, launch_date, price)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        row["Product_ID"],
        row["Product_Name"],
        row["Category_ID"],
        row["Launch_Date"],
        row["Price"]
    ))

conn.commit()
print("dim_products loaded")

# stores dimension
print("Loading dim_stores...")
stores = pd.read_csv(os.path.join(DATA_CLEANED_DIR, "stores_cleaned.csv"))

for _, row in stores.iterrows():
    cursor.execute("""
        INSERT INTO dim_stores (store_id, store_name, city, country)
        VALUES (%s, %s, %s, %s)
    """, (
        row["Store_ID"],
        row["Store_Name"],
        row["City"],
        row["Country"]
    ))

conn.commit()
print("dim_stores loaded")


# date dimension
print("Generating and loading dim_date...")
dates = pd.date_range(
    start="2020-01-01",
    end="2025-12-31"
)

date_df = pd.DataFrame({
    "date": dates,
    "year": dates.year,
    "month": dates.month,
    "month_name": dates.strftime("%B"),
    "quarter": "Q" + dates.quarter.astype(str),
    "weekday": dates.strftime("%A")
})

for _, row in date_df.iterrows():
    cursor.execute("""
        INSERT INTO dim_date
        (date, year, month, month_name, quarter, weekday)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, tuple(row))

conn.commit()
print("dim_date loaded")


# -----------------------------
# FACT SALES
# -----------------------------
print("Loading fact_sales...")

sales = pd.read_csv(
    os.path.join(DATA_CLEANED_DIR, "sales_cleaned.csv")
)
sales["sale_date"] = pd.to_datetime(sales["sale_date"]).dt.date
sales["year"] = sales["year"].astype(int)
sales["month"] = sales["month"].astype(int)
sales["quantity"] = sales["quantity"].astype(int)
sales["revenue"] = sales["revenue"].astype(float)

# Drop duplicate sale_id rows before inserting
sales = sales.drop_duplicates(subset=["sale_id"])

print("Loading fact_sales in batches...")

batch_size = 500
data = []

for _, row in sales.iterrows():
    data.append((
        row["sale_id"],
        row["sale_date"],
        row["product_id"],
        row["store_id"],
        row["quantity"],
        row["revenue"],
        row["year"],
        row["month"]
    ))

    if len(data) == batch_size:
        cursor.executemany("""
            INSERT IGNORE INTO fact_sales
            (sale_id, sale_date, product_id, store_id, quantity, revenue, year, month)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, data)
        conn.commit()   # ✅ commit only after actual insert
        data = []
        print("Inserted 500 rows")

# insert remaining rows
if data:
    cursor.executemany("""
        INSERT IGNORE INTO fact_sales   # ✅ changed to IGNORE
        (sale_id, sale_date, product_id, store_id, quantity, revenue, year, month)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, data)
    conn.commit()
    print("Inserted remaining rows")

print("✅ fact_sales load completed")




# -----------------------------
# FACT WARRANTY
# -----------------------------
print("Loading fact_warranty...")

warranty = pd.read_csv(
    os.path.join(DATA_CLEANED_DIR, "warranty_cleaned.csv")
)
warranty = warranty.drop_duplicates(subset=["claim_id"])

batch_size = 500
data = []

for _, row in warranty.iterrows():
    data.append((
        row["claim_id"],
        row["claim_date"],
        row["sale_id"],
        row["repair_status"],
        row["is_completed"],
        int(row["year"]),
        int(row["month"])
    ))

    if len(data) == batch_size:
        cursor.executemany("""
            INSERT IGNORE INTO fact_warranty
            (claim_id, claim_date, sale_id, repair_status, is_completed, year, month)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, data)
        conn.commit()
        data = []
        print("Inserted 500 warranty rows")

# insert remaining rows
if data:
    cursor.executemany("""
        INSERT IGNORE INTO fact_warranty
        (claim_id, claim_date, sale_id, repair_status, is_completed, year, month)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, data)
    conn.commit()
    print("Inserted remaining warranty rows")

print("✅ fact_warranty loaded")


