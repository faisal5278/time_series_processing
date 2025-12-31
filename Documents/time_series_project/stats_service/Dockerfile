FROM python:3.10-slim

# Set working directory inside the container
WORKDIR /app

# Install system dependencies required by psycopg2 (PostgreSQL driver)
RUN apt-get update && apt-get install -y build-essential libpq-dev && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application source code
COPY src ./src

# Expose the port used by this service
EXPOSE 8002

# Start the FastAPI app with Uvicorn
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8002"]
