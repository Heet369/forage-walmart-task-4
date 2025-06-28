import sqlite3
import pandas as pd

sheet0 = pd.read_csv("spreadsheet_0.csv")
sheet1 = pd.read_csv("spreadsheet_1.csv")
sheet2 = pd.read_csv("spreadsheet_2.csv")

conn = sqlite3.connect("shipment_database.sqlite")
cursor = conn.cursor()

for _, row in sheet0.iterrows():
    cursor.execute("""
        INSERT INTO shipment (
            shipment_identifier, product_name, quantity, origin, destination
        ) VALUES (?, ?, ?, ?, ?)
    """, (row["shipment_identifier"], row["product_name"], row["quantity"], row["origin"], row["destination"]))

merged = pd.merge(sheet1, sheet2, on="shipment_identifier")

for _, row in merged.iterrows():
    cursor.execute("""
        INSERT INTO shipment (
            shipment_identifier, product_name, quantity, origin, destination
        ) VALUES (?, ?, ?, ?, ?)
    """, (row["shipment_identifier"], row["product_name"], row["quantity"], row["origin"], row["destination"]))

conn.commit()
conn.close()

print("All spreadsheets successfully imported into the database.")