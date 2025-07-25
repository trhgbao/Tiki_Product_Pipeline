# Đây là file cấu hình mẫu.
# Hãy sao chép file này, đổi tên thành "Config.py" và điền các giá trị của bạn vào.

# --- Cấu hình Selenium ---
# Đường dẫn đến file chromedriver.exe của bạn trên máy
# Trên Linux/macOS, nếu đã thêm vào PATH, có thể để trống hoặc không cần dùng
DRIVER_PATH = r"C:\path\to\your\chromedriver.exe"

# --- Cấu hình CSDL SQL Server ---
DB_CONFIG = {
    'server': 'YOUR_SERVER_ADDRESS', # ví dụ: 'localhost\SQLEXPRESS' hoặc tên container
    'database': 'TikiData',
    'username': 'YOUR_USERNAME', # ví dụ: 'sa'
    'password': 'YOUR_STRONG_PASSWORD',
    'driver': '{ODBC Driver 18 for SQL Server}'
}

# --- Cấu hình Pipeline ---
PAGES_TO_SCRAPE = 3
OUTPUT_FILE_HISTORY = "tiki_history_data.csv"
OUTPUT_FILE_BRANDS = "tiki_brands_data.csv"
