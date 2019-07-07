# gcp-datacatalog-python

Python code samples to help Data Citizens who work with Google Cloud Data Catalog.

## 1. Understand the concepts behind this code

- [Data Catalog hands-on guide: a mental model](https://medium.com/google-cloud/data-catalog-hands-on-guide-a-mental-model-dae7f6dd49e) @ Google Cloud Community / Medium

- [Data Catalog hands-on guide: search, get & lookup with Python](https://medium.com/google-cloud/data-catalog-hands-on-guide-search-get-lookup-with-python-82d99bfb4056) @ Google Cloud Community / Medium

- [Data Catalog hands-on guide: templates & tags with Python](https://medium.com/google-cloud/data-catalog-hands-on-guide-templates-tags-with-python-c45eb93372ef) @ Google Cloud Community / Medium

## 2. Environment setup

Using *virtualenv* is optional, but strongly recommended.

##### 2.1. Install Python 3.7

##### 2.2. Create and activate a *virtualenv*

```bash
pip install --upgrade virtualenv
[python3 -m] virtualenv --python python3 env
source ./env/bin/activate
```

##### 2.3. Install the dependencies

```bash
pip install -r requirements.txt
```

##### 2.4. Setup credentials

###### 2.4.1. Create a service account and grant it below roles

- `BigQuery Admin`
- `Data Catalog Admin`

###### 2.4.2. Download a JSON key and save it as
- `./credentials/datacatalog-samples.json`

## 3. Quickstart

##### 3.1. Automated tests

Automated tests are useful to make sure your environment has been properly set up.
They communicate with GCP APIs and create temporary resources that are deleted just after being used.

- Virtualenv

```bash
export GOOGLE_APPLICATION_CREDENTIALS=./credentials/datacatalog-samples.json
export GOOGLE_CLOUD_TEST_ORGANIZATION_ID=your-organization-id
export GOOGLE_CLOUD_TEST_PROJECT_ID=your-project-id

pytest ./tests/quickstart_test.py
```

- Or using Docker

```bash
cd..
docker build --rm --tag gcp-datacatalog-python ./gcp-datacatalog-python/
docker run \
  --env GOOGLE_CLOUD_TEST_ORGANIZATION_ID=your-organization-id \
  --env GOOGLE_CLOUD_TEST_PROJECT_ID=your-project-id \
  --rm gcp-datacatalog-python pytest ./tests/quickstart_test.py
```

##### 3.2. Run quickstart.py

- Virtualenv

```bash
export GOOGLE_APPLICATION_CREDENTIALS=./credentials/datacatalog-samples.json

python quickstart.py your-organization-id your-project-id
```

- Or using Docker

```bash
cd..
docker build --rm --tag gcp-datacatalog-python ./gcp-datacatalog-python/
docker run --rm gcp-datacatalog-python python quickstart.py your-organization-id your-project-id
```
