FROM python:3.6

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
# At run time, /credentials must be binded to a volume containing a valid
# Service Account credentials file named datacatalog-samples.json.
ENV GOOGLE_APPLICATION_CREDENTIALS=/credentials/datacatalog-samples.json

WORKDIR /app

# Copy the requirements.txt file and install all dependencies.
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy project files (see .dockerignore).
COPY . .

# Run the unit tests.
RUN pytest ./tests/unit
