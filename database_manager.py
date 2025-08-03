import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import Config as cf

class DatabaseManager:
    """Handles all interactions with the PostgreSQL database."""
    def __init__(self):
        self.db_config = cf.DB_CONFIG_POSTGRES
        self.cnxn = None
        self.cursor = None
        print("DatabaseManager initialized for PostgreSQL.")

    def connect(self):
        if self.cnxn: return
        try:
            self.cnxn = psycopg2.connect(
                host=self.db_config['host'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                port=self.db_config.get('port', 5432)
            )
            self.cnxn.autocommit = True
            self.cursor = self.cnxn.cursor()
            print("Successfully connected to PostgreSQL (autocommit mode)!")
        except psycopg2.OperationalError as ex:
            print(f"PostgreSQL connection failed: {ex}")
            raise

    def disconnect(self):
        if self.cursor: self.cursor.close()
        if self.cnxn: self.cnxn.close()
        print("PostgreSQL connection closed.")

    def _setup_database_schema(self):
        """Creates tables and constraints using PostgreSQL snake_case naming convention."""
        create_brands_query = """
        CREATE TABLE IF NOT EXISTS brand_details (
            brand_id SERIAL PRIMARY KEY,
            brand_name VARCHAR(255) UNIQUE NOT NULL,
            brand_link TEXT,
            is_official BOOLEAN,
            brand_rating REAL,
            num_rating INT,
            joined_date DATE,
            last_scraped_date DATE
        );
        """
        create_history_query = """
        CREATE TABLE IF NOT EXISTS tiki_products_history (
            product_id SERIAL PRIMARY KEY,
            name VARCHAR(500),
            price INT,
            sold_count INT,
            link TEXT,
            rating REAL,
            scraped_date DATE,
            brand_id INT REFERENCES brand_details(brand_id)
        );
        """
        create_constraint_query = """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_product_history') THEN
                ALTER TABLE tiki_products_history
                ADD CONSTRAINT uq_product_history UNIQUE (name, brand_id, scraped_date);
            END IF;
        END; $$;
        """
        print("Ensuring tables and constraints exist...")
        self.cursor.execute(create_brands_query)
        self.cursor.execute(create_history_query)
        self.cursor.execute(create_constraint_query)
        print("--- Database Schema Setup Complete ---")

    def upsert_data(self, brands_df, history_df):
        if history_df.empty:
            print("No product data to process.")
            return
            
        try:
            self.connect()
            self._setup_database_schema()
            
            # --- Step 1: Chuẩn hóa tên cột ngay từ đầu ---
            brands_df.columns = [col.lower() for col in brands_df.columns]
            history_df.columns = [col.lower() for col in history_df.columns]
            
            # --- Step 2: Process brand_details Table ---
            print("\n--- Processing brand_details ---")
            
            # Sử dụng tên cột chữ thường, không dấu ngoặc kép
            existing_brands_df = pd.read_sql("SELECT brand_id, brand_name FROM brand_details", self.cnxn)
            
            merged_brands = pd.merge(brands_df, existing_brands_df, on='brand_name', how='left', indicator=True)
            new_brands_to_insert = merged_brands[merged_brands['_merge'] == 'left_only']
            
            if not new_brands_to_insert.empty:
                # Đảm bảo các cột khớp với bảng CSDL
                db_brand_columns = ['brand_name', 'brand_link', 'is_official', 'brand_rating', 'num_rating', 'joined_date', 'last_scraped_date']
                columns_to_insert = [col for col in db_brand_columns if col in new_brands_to_insert.columns]
                new_brands_final = new_brands_to_insert[columns_to_insert]
                
                print(f"Found {len(new_brands_final)} new brands to insert.")
                
                data_tuples = [tuple(row) for row in new_brands_final.itertuples(index=False)]
                cols_sql = ', '.join(columns_to_insert)
                insert_query = f"INSERT INTO brand_details ({cols_sql}) VALUES %s"
                
                execute_values(self.cursor, insert_query, data_tuples)
                print("New brands inserted.")
            else:
                print("No new brands to insert.")

            # --- Step 3: Prepare tiki_products_history Data ---
            print("\n--- Preparing tiki_products_history ---")
            all_brands_with_id_df = pd.read_sql("SELECT brand_id, brand_name FROM brand_details", self.cnxn)
            
            # Join trên cột 'brand_name' (đã được chuẩn hóa thành chữ thường)
            history_with_brand_id = pd.merge(history_df, all_brands_with_id_df, on='brand_name', how='left')
            
            # Lọc trùng lặp và loại bỏ các dòng không join được
            history_with_brand_id.drop_duplicates(subset=['name', 'brand_id', 'scraped_date'], keep='first', inplace=True)
            final_fact_data = history_with_brand_id.dropna(subset=['brand_id'])
            final_fact_data['brand_id'] = final_fact_data['brand_id'].astype(int)

            # Xác định các cột cuối cùng để insert
            fact_columns = ['name', 'price', 'sold_count', 'link', 'rating', 'scraped_date', 'brand_id']
            final_fact_data_to_insert = final_fact_data[fact_columns]

            if not final_fact_data_to_insert.empty:
                data_tuples = [tuple(row) for row in final_fact_data_to_insert.itertuples(index=False)]
                cols_sql = ', '.join(fact_columns)
                insert_query = f"INSERT INTO tiki_products_history ({cols_sql}) VALUES %s ON CONFLICT (name, brand_id, scraped_date) DO NOTHING"
                
                print(f"Inserting {len(data_tuples)} records into tiki_products_history...")
                execute_values(self.cursor, insert_query, data_tuples)
                print("Product history data inserted successfully.")
            else:
                print("No valid product history data to insert.")

        except psycopg2.Error as e:
            print(f"A PostgreSQL error occurred: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        finally:
            self.disconnect()
