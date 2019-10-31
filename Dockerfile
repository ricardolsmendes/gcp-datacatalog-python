FROM python:3.7
WORKDIR /app

# Copy the credentials file and use it to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
COPY ./credentials/*.json ./credentials/
ENV GOOGLE_APPLICATION_CREDENTIALS=./credentials/datacatalog-samples.json

# Copy the requirements.txt file and install all dependencies.
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy project files (see .dockerignore).
COPY . .

# Run the unit tests.
RUN pytest ./tests/unit
