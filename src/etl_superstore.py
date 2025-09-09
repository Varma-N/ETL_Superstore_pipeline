import pandas as pd
import mysql.connector
import logging
from datetime import datetime


# ------------------------------
# Setup Logging
# ------------------------------
logging.basicConfig(
    filename="etl_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# =============================
# EXTRACT: Load data from CSV
# =============================
def extract_data():
    """Reads the Superstore CSV file into a pandas DataFrame."""
    try:
        df = pd.read_csv(r"data\SampleSuperstore.csv", encoding="latin1")
        print("✅ Data extracted from CSV.")
        logging.info(f"Data extracted successfully. Rows read: {len(df)}")
        return df
    except Exception as e:
        logging.error("Error during data extraction", exc_info=True)
        raise
    


# =============================
# TRANSFORM: Clean and format data
# =============================
def transform_data(df):
    """Cleans column names and converts date columns to YYYY-MM-DD format."""
    try:
        # Clean column names (replace spaces with underscores)
        df.columns = [col.strip().replace(" ", "_") for col in df.columns]

        # Convert dates
        df['Order_Date'] = df['Order_Date'].apply(lambda x: datetime.strptime(x, "%m/%d/%Y").strftime("%Y-%m-%d"))
        df['Ship_Date'] = df['Ship_Date'].apply(lambda x: datetime.strptime(x, "%m/%d/%Y").strftime("%Y-%m-%d"))

        print("✅ Data transformed: columns cleaned and dates formatted.")
        logging.info("Data transformed successfully. Columns cleaned and dates formatted.")
        return df
    except Exception as e:
        logging.error("Error during data transformation", exc_info=True)
        raise

# =============================
# LOAD: Insert into MySQL (incremental + idempotent)
# =============================
def load_data(df):
    """Inserts new records into MySQL only if (order_id, product_id) doesn't exist."""
    # Connect to MySQL
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            database="etl_superstore"
        )
        cursor = connection.cursor()

        inserted_count = 0
        skipped_count = 0

        # Loop through each row
        for _, row in df.iterrows():
            order_id = row["Order_ID"]
            product_id = row["Product_ID"]

            # Check if record already exists
            cursor.execute(
                "SELECT COUNT(*) FROM superstore_orders_table WHERE order_id = %s AND product_id = %s",
                (order_id, product_id)
            )
            exists = cursor.fetchone()[0]

            if exists == 0:
                # Insert new record
                cursor.execute(
                    """
                    INSERT INTO superstore_orders_table
                    (order_id, product_id, order_date, ship_date, customer_name, category, segment, sales, quantity, profit)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        row["Order_ID"],
                        row["Product_ID"],
                        row["Order_Date"],
                        row["Ship_Date"],
                        row["Customer_Name"],
                        row["Category"],
                        row["Segment"],
                        row["Sales"],
                        row["Quantity"],
                        row["Profit"]
                    )
                )
                inserted_count += 1
            else:
                skipped_count += 1

        # Commit and close
        connection.commit()
        cursor.close()
        connection.close()

        print(f"✅ Load completed! Inserted: {inserted_count} | Skipped (duplicates): {skipped_count}")
        logging.info(f"Load completed. Inserted: {inserted_count}, Skipped: {skipped_count}")
    
    except Exception as e:
        logging.error("Error during data loading", exc_info=True)
        raise

# =============================
# MAIN: Run ETL pipeline
# =============================
def main():
    """Main function to run the full ETL process: extract → transform → load."""
    print("🚀 Starting ETL Process...")

    try:
        # Step 1: Extract
        df = extract_data()

        # Step 2: Transform
        df = transform_data(df)

        # Step 3: Load
        load_data(df)

        print("🎉 ETL completed successfully!")
        logging.info("ETL process completed successfully.")

    except Exception as e:
        logging.error("ETL process failed", exc_info=True)
        print("❌ ETL process failed. Check logs for details.")

# Run the script
if __name__ == "__main__":
    main()


