from scraper import TikiScraper
from database_manager import DatabaseManager
from urllib.parse import quote_plus
import Config as cf

def run_pipeline():
    """Main function to orchestrate the entire ETL pipeline."""
    
    print("--- Starting Tiki Data Pipeline ---")
    
    print("Running in automated mode with a predefined keyword.")
    formatted_keyword = quote_plus(cf.SEARCH_KEYWORD) 
    target_url = f"https://tiki.vn/search?q={formatted_keyword}"

    # --- Step 2: Scrape Data ---
    if target_url:
        scraper = TikiScraper()
        
        # The scrape method returns two ready-to-use DataFrames
        brands_df, history_df = scraper.scrape(
            base_url=target_url, 
            num_pages=cf.PAGES_TO_SCRAPE
        )
        
        # --- Step 3: Process and Load Data ---
        # Check if the history DataFrame is not empty before proceeding
        if not history_df.empty:
            print(f"\nScraping successful. Found {len(history_df)} product records and {len(brands_df)} unique brands.")
            
            # Optional: Save to CSV files for backup or inspection
            # TikiScraper.save_to_csv(history_df, filename="history_backup.csv")
            # TikiScraper.save_to_csv(brands_df, filename="brands_backup.csv")
            
            # Load the structured data into the SQL Server database
            print("\n--- Starting Database Operations ---")
            db_manager = DatabaseManager()
            
            try:
                db_manager.upsert_data(brands_df, history_df) 
            except Exception as e:
                print(f"An error occurred during the database operation: {e}")

        else:
            print("No products were scraped. Skipping database operations.")
            
    print("\n--- Pipeline finished successfully! ---")


if __name__ == "__main__":
    run_pipeline()
