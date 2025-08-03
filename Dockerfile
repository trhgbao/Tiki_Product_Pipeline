# Dockerfile mới, đơn giản hơn cho PostgreSQL

# Step 1: Bắt đầu từ image Python chuẩn, không cần image của Microsoft nữa
FROM python:3.10-slim

# Step 2: Set the working directory
WORKDIR /app

# Step 3: Cài đặt các gói hệ thống cần thiết
# build-essential và libpq-dev cần để psycopg2 có thể biên dịch (nếu cần)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Step 4: Copy requirements file
COPY requirements.txt .

# Step 5: Install Python libraries
RUN pip install --default-timeout=100 --no-cache-dir -r requirements.txt

# Step 6: Copy the rest of the project code
COPY . .

# Step 7: Define the start command
CMD ["python3", "main.py"]
