import pandas as pd
import sqlite3

# Connect to the SQLite database (in root folder)
conn = sqlite3.connect("shipment_database.sqlite")
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS shipment (
        shipment_identifier TEXT,
        product_name TEXT,
        quantity INTEGER,
        origin TEXT,
        destination TEXT
    );
""")

# ----------------------------
# PART 1: Process shipping_data_0.csv
# ----------------------------
data0 = pd.read_csv("data/shipping_data_0.csv")
data0["shipment_identifier"] = ["auto-" + str(i) for i in range(len(data0))]  # Create dummy IDs

for _, row in data0.iterrows():
    cursor.execute("""
        INSERT INTO shipment (
            shipment_identifier, product_name, quantity, origin, destination
        ) VALUES (?, ?, ?, ?, ?)
    """, (
        row["shipment_identifier"],
        row["product"],
        row["product_quantity"],
        row["origin_warehouse"],
        row["destination_store"]
    ))

# ----------------------------
# PART 2: Process shipping_data_1.csv + shipping_data_2.csv
# ----------------------------
data1 = pd.read_csv("data/shipping_data_1.csv")  # shipment_identifier, product, on_time
data2 = pd.read_csv("data/shipping_data_2.csv")  # shipment_identifier, origin_warehouse, destination_store, driver_identifier

# Join on shipment_identifier
merged = pd.merge(data1, data2, on="shipment_identifier")

# Set quantity = 1 for each row (assumed)
merged["quantity"] = 1

for _, row in merged.iterrows():
    cursor.execute("""
        INSERT INTO shipment (
            shipment_identifier, product_name, quantity, origin, destination
        ) VALUES (?, ?, ?, ?, ?)
    """, (
        row["shipment_identifier"],
        row["product"],
        row["quantity"],
        row["origin_warehouse"],
        row["destination_store"]
    ))

# Finalize
conn.commit()
conn.close()

print("All data inserted successfully into shipment_database.sqlite")
