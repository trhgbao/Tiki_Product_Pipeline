import pyodbc
import pandas as pd
import Config as cf

class DatabaseManager:
    """Handles all interactions with the SQL Server database."""
    def __init__(self):
        """Initializes the DatabaseManager with connection configuration."""
        self.db_config = cf.DB_CONFIG
        self.cnxn = None
        self.cursor = None
        print("DatabaseManager initialized.")

    def connect(self):
        """Establishes a connection to the database."""
        if self.cnxn:
            return
        try:
            connection_string = (
                f'DRIVER={self.db_config["driver"]};'
                f'SERVER={self.db_config["server"]};'
                f'DATABASE={self.db_config["database"]};'
                f'UID={self.db_config["username"]};'
                f'PWD={self.db_config["password"]};'
                'TrustServerCertificate=yes;'
            )
            self.cnxn = pyodbc.connect(connection_string)
            self.cursor = self.cnxn.cursor()
            print("Successfully connected to SQL Server!")
        except pyodbc.Error as ex:
            print(f"Database connection failed: {ex}")
            self.cnxn = None; self.cursor = None
            raise

    def disconnect(self):
        """Closes the database connection."""
        if self.cursor: self.cursor.close()
        if self.cnxn: self.cnxn.close()
        print("Database connection closed.")
        self.cnxn = None; self.cursor = None

    def _setup_database_schema(self):
        """Creates the database tables and constraints if they do not exist."""

        print("--- Running Initial Database Setup ---")
        try:
            # --- Create BrandDetails table (Dimension) ---
            create_brands_query = """
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='BrandDetails' AND xtype='U')
            CREATE TABLE BrandDetails (
                BrandID INT IDENTITY(1,1) PRIMARY KEY,
                BrandName NVARCHAR(255) UNIQUE NOT NULL,
                BrandLink NVARCHAR(MAX),
                IsOfficial BIT,
                BrandRating FLOAT,
                NumRating INT,
                JoinedDate DATE,
                LastScrapedDate DATE
            )
            """
            
            # --- Create TikiProductsHistory table (Fact) ---
            create_history_query = """
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='TikiProductsHistory' AND xtype='U')
            CREATE TABLE TikiProductsHistory (
                ProductID INT IDENTITY(1,1) PRIMARY KEY,
                Name NVARCHAR(450), -- Max length for unique index compatibility
                Price INT,
                SoldCount INT,
                Link NVARCHAR(MAX),
                Rating FLOAT,
                ScrapedDate DATE,
                BrandID INT,
                FOREIGN KEY (BrandID) REFERENCES BrandDetails(BrandID)
            )
            """
            
            # --- Create UNIQUE constraint to prevent daily duplicates ---
            create_constraint_query = """
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE name = 'UQ_Product_History' AND type = 'UQ')
            BEGIN
                ALTER TABLE TikiProductsHistory
                ADD CONSTRAINT UQ_Product_History UNIQUE (Name, BrandID, ScrapedDate);
            END
            """
            
            print("Ensuring tables and constraints exist...")
            self.cursor.execute(create_brands_query)
            self.cursor.execute(create_history_query)
            self.cursor.execute(create_constraint_query)
            self.cnxn.commit()
            print("--- Database Schema Setup Complete ---")
        except pyodbc.Error as e:
            print(f"An error occurred during schema setup: {e}")
            self.cnxn.rollback()
            raise

    def upsert_data(self, brands_df, history_df):
        """
        Upserts brand and product history data into the database.
        It handles new brands and ensures product history is loaded correctly.
        """
        if history_df.empty:
            print("No product data to process.")
            return
            
        try:
            self.connect()
            self._setup_database_schema()
            
            # --- Step 1: Process BrandDetails Table (Dimension) ---
            print("\n--- Processing BrandDetails ---")
            existing_brands_df = pd.read_sql("SELECT BrandID, BrandName FROM BrandDetails", self.cnxn)
            merged_brands = pd.merge(brands_df, existing_brands_df, on='BrandName', how='left', indicator=True)
            new_brands_to_insert = merged_brands[merged_brands['_merge'] == 'left_only']
            
            if not new_brands_to_insert.empty:
                db_brand_columns = ['BrandName', 'BrandLink', 'IsOfficial', 'BrandRating', 'NumRating', 'JoinedDate', 'LastScrapedDate']
                columns_that_exist = [col for col in db_brand_columns if col in new_brands_to_insert.columns]
                new_brands_final = new_brands_to_insert[columns_that_exist]
                
                print(f"Found {len(new_brands_final)} new brands to insert.")
                data_tuples = [tuple(row) for row in new_brands_final.itertuples(index=False)]
                
                cols_sql = ', '.join(columns_that_exist)
                vals_sql = ', '.join(['?'] * len(columns_that_exist))
                insert_query = f"INSERT INTO BrandDetails ({cols_sql}) VALUES ({vals_sql})"
                
                self.cursor.executemany(insert_query, data_tuples)
                self.cnxn.commit()
                print("New brands inserted.")
            else:
                print("No new brands to insert.")

            # --- Step 2: Prepare Product History Data (Fact) ---
            print("\n--- Preparing TikiProductsHistory Data ---")
            all_brands_with_id_df = pd.read_sql("SELECT BrandID, BrandName FROM BrandDetails", self.cnxn)
            history_with_brand_id = pd.merge(history_df, all_brands_with_id_df, on='BrandName', how='left')
            
            # Remove duplicates from the current scrape batch before inserting
            history_with_brand_id.drop_duplicates(subset=['Name', 'BrandID', 'ScrapedDate'], keep='first', inplace=True)
            
            final_fact_data = history_with_brand_id.dropna(subset=['BrandID'])
            final_fact_data['BrandID'] = final_fact_data['BrandID'].astype(int)

            fact_columns = ['Name', 'Price', 'SoldCount', 'Link', 'Rating', 'ScrapedDate', 'BrandID']
            final_fact_data_to_insert = final_fact_data[fact_columns]

            if not final_fact_data_to_insert.empty:
                data_tuples = [tuple(row) for row in final_fact_data_to_insert.itertuples(index=False)]
                insert_query = f"INSERT INTO TikiProductsHistory ({', '.join(fact_columns)}) VALUES ({', '.join(['?'] * len(fact_columns))})"
                
                print(f"Inserting {len(data_tuples)} records into TikiProductsHistory...")
                self.cursor.executemany(insert_query, data_tuples)
                self.cnxn.commit()
                print("Product history data inserted successfully.")
            else:
                print("No valid product history data to insert.")

        except pyodbc.IntegrityError:
            print("Insertion ignored: Some records already exist in the database (due to UNIQUE constraint).")
            self.cnxn.rollback()
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        finally:
            self.disconnect()