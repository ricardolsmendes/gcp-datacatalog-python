# gcp-datacatalog-python

Self-contained ready-to-use Python scripts to help Data Citizens who work with
[Google Cloud Data Catalog][1].

[![license](https://img.shields.io/github/license/ricardolsmendes/gcp-datacatalog-python.svg)](https://github.com/ricardolsmendes/gcp-datacatalog-python/blob/master/LICENSE)
[![issues](https://img.shields.io/github/issues/ricardolsmendes/gcp-datacatalog-python.svg)](https://github.com/ricardolsmendes/gcp-datacatalog-python/issues)
[![CircleCI][2]][3]

<!--
  DO NOT UPDATE THE TABLE OF CONTENTS MANUALLY
  run `npx markdown-toc -i README.md`.

  Please stick to 100-character line wraps as much as you can.
-->

## Table of Contents

<!-- toc -->

- [1. Get to know the concepts behind this code](#1-get-to-know-the-concepts-behind-this-code)
- [2. Environment setup](#2-environment-setup)
  * [2.1. Get the code](#21-get-the-code)
  * [2.2. Auth credentials](#22-auth-credentials)
  * [2.3. Virtualenv](#23-virtualenv)
  * [2.4. Docker](#24-docker)
  * [2.5. Integration tests](#25-integration-tests)
- [3. Quickstart](#3-quickstart)
  * [3.1. Integration tests](#31-integration-tests)
  * [3.2. Run quickstart.py](#32-run-quickstartpy)
- [4. Load Tag Templates from CSV files](#4-load-tag-templates-from-csv-files)
  * [4.1. Provide CSV files representing the Template to be created](#41-provide-csv-files-representing-the-template-to-be-created)
  * [4.2. Integration tests](#42-integration-tests)
  * [4.3. Run load_template_csv.py](#43-run-load_template_csvpy)
- [5. Load Tag Templates from Google Sheets](#5-load-tag-templates-from-google-sheets)
  * [5.1. Enable the Google Sheets API in your GCP Project](#51-enable-the-google-sheets-api-in-your-gcp-project)
  * [5.2. Provide Google Spreadsheets representing the Template to be created](#52-provide-google-spreadsheets-representing-the-template-to-be-created)
  * [5.3. Integration tests](#53-integration-tests)
  * [5.4. Run load_template_google_sheets.py](#54-run-load_template_google_sheetspy)
- [6. How to contribute](#6-how-to-contribute)
  * [6.1. Report issues](#61-report-issues)
  * [6.2. Contribute code](#62-contribute-code)

<!-- tocstop -->

---

## 1. Get to know the concepts behind this code

- [Data Catalog hands-on guide: a mental model][4] @ Google Cloud Community / Medium

- [Data Catalog hands-on guide: search, get & lookup with Python][5] @ Google Cloud Community /
  Medium

- [Data Catalog hands-on guide: templates & tags with Python][6] @ Google Cloud Community / Medium

## 2. Environment setup

### 2.1. Get the code

```sh
git clone https://github.com/ricardolsmendes/gcp-datacatalog-python.git
cd gcp-datacatalog-python
```

### 2.2. Auth credentials

**2.2.1. Create a service account and grant it below roles**

- BigQuery Admin
- Data Catalog Admin

**2.2.2. Download a JSON key and save it as**

- `./credentials/datacatalog-samples.json`

### 2.3. Virtualenv

Using _virtualenv_ is optional, but strongly recommended unless you use [Docker](#24-docker).

**2.3.1. Install Python 3.6+**

**2.3.2. Create and activate an isolated Python environment**

```sh
pip install --upgrade virtualenv
python3 -m virtualenv --python python3 env
source ./env/bin/activate
```

**2.3.3. Install the dependencies**

```sh
pip install --upgrade -r requirements.txt
```

**2.3.4. Set environment variables**

```sh
export GOOGLE_APPLICATION_CREDENTIALS=./credentials/datacatalog-samples.json
```

### 2.4. Docker

Docker may be used to run all the scripts. In this case please disregard the
[Set up Virtualenv](#23-virtualenv) install instructions.

### 2.5. Integration tests

Integration tests help to make sure Google Cloud APIs and Service Accounts IAM Roles have been
properly set up before running a script. They actually communicate with the APIs and create
temporary resources that are deleted just after being used.

## 3. Quickstart

### 3.1. Integration tests

- pytest

```sh
export GOOGLE_CLOUD_TEST_ORGANIZATION_ID=<YOUR-ORGANIZATION-ID>
export GOOGLE_CLOUD_TEST_PROJECT_ID=<YOUR-PROJECT-ID>

pytest ./tests/integration/quickstart_test.py
```

- docker

```sh
docker build --rm --tag gcp-datacatalog-python .
docker run --rm --tty \
  --env GOOGLE_CLOUD_TEST_ORGANIZATION_ID=<YOUR-ORGANIZATION-ID> \
  --env GOOGLE_CLOUD_TEST_PROJECT_ID=<YOUR-PROJECT-ID> \
  --volume <CREDENTIALS-FILE-FOLDER>:/credentials \
  gcp-datacatalog-python pytest ./tests/integration/quickstart_test.py
```

### 3.2. Run quickstart.py

- python

```sh
python quickstart.py --organization-id <YOUR-ORGANIZATION-ID> --project-id <YOUR-PROJECT-ID>
```

- docker

```sh
docker build --rm --tag gcp-datacatalog-python .
docker run --rm --tty gcp-datacatalog-python \
  --volume <CREDENTIALS-FILE-FOLDER>:/credentials \
  python quickstart.py --organization-id <YOUR-ORGANIZATION-ID> --project-id <YOUR-PROJECT-ID>
```

## 4. Load Tag Templates from CSV files

### 4.1. Provide CSV files representing the Template to be created

1. A **master file** named with the Template ID — i.e., `template-abc.csv` if your Template ID is
   _template_abc_. This file may contain as many lines as needed to represent the template. The first
   line is always discarded as it's supposed to contain headers. Each field line must have 3 values:
   the first is the Field ID; second is its Display Name; third is the Type. Currently, types `BOOL`,
   `DOUBLE`, `ENUM`, `STRING`, `TIMESTAMP`, and `MULTI` are supported. _`MULTI` is not a Data Catalog
   native type, but a flag that instructs the script to create a specific template to represent
   field's predefined values (more on this below...)_.
1. If the template has **ENUM fields**, the script looks for a "display names file" for each of
   them. The files shall be named with the fields' names — i.e., `enum-field-xyz.csv` if an ENUM Field
   ID is _enum_field_xyz_. Each file must have just one value per line, representing a display name.
1. If the template has **multivalued fields**, the script looks for a "values file" for each of
   them. The files shall be named with the fields' names — i.e., `multivalued-field-xyz.csv` if a
   multivalued Field ID is _multivalued_field_xyz_. Each file must have just one value per line,
   representing a short description for the value. The script will generate Field's ID and Display
   Name based on it.
1. All Fields' IDs generated by the script will be formatted to snake case (e.g., foo_bar_baz), but
   it will do the formatting job for you. So, just provide the IDs as strings.

_TIP: keep all template-related files in the same folder ([sample-input/load-template-csv][7] for
reference)._

### 4.2. Integration tests

- pytest

```sh
export GOOGLE_CLOUD_TEST_PROJECT_ID=<YOUR-PROJECT-ID>

pytest ./tests/integration/load_template_csv_test.py
```

- docker

```sh
docker build --rm --tag gcp-datacatalog-python .
docker run --rm --tty \
  --env GOOGLE_CLOUD_TEST_PROJECT_ID=<YOUR-PROJECT-ID> \
  --volume <CREDENTIALS-FILE-FOLDER>:/credentials \
  gcp-datacatalog-python pytest ./tests/integration/load_template_csv_test.py
```

### 4.3. Run load_template_csv.py

- python

```sh
python load_template_csv.py \
  --template-id <TEMPLATE-ID> --display-name <DISPLAY-NAME> \
  --project-id <YOUR-PROJECT-ID> --files-folder <FILES-FOLDER> \
  [--delete-existing]
```

- docker

```sh
docker build --rm --tag gcp-datacatalog-python .
docker run --rm --tty gcp-datacatalog-python \
  --volume <CREDENTIALS-FILE-FOLDER>:/credentials \
  python load_template_csv.py \
  --template-id <TEMPLATE-ID> --display-name <DISPLAY-NAME> \
  --project-id <YOUR-PROJECT-ID> --files-folder <FILES-FOLDER> \
  [--delete-existing]
```

## 5. Load Tag Templates from Google Sheets

### 5.1. Enable the Google Sheets API in your GCP Project

https://console.developers.google.com/apis/library/sheets.googleapis.com

### 5.2. Provide Google Spreadsheets representing the Template to be created

1. A **master sheet** named with the Template ID — i.e., `template-abc` if your Template ID is
   _template_abc_. This sheet may contain as many lines as needed to represent the template. The first
   line is always discarded as it's supposed to contain headers. Each field line must have 3 values:
   column A is the Field ID; column B is its Display Name; column C is the Type. Currently, types
   `BOOL`, `DOUBLE`, `ENUM`, `STRING`, `TIMESTAMP`, and `MULTI` are supported. _`MULTI` is not a Data
   Catalog native type, but a flag that instructs the script to create a specific template to
   represent field's predefined values (more on this below...)_.
1. If the template has **ENUM fields**, the script looks for a "display names sheet" for each of
   them. The sheets shall be named with the fields' names — i.e., `enum-field-xyz` if an ENUM Field ID
   is _enum_field_xyz_. Each sheet must have just one value per line (column A), representing a
   display name.
1. If the template has **multivalued fields**, the script looks for a "values sheet" for each of
   them. The sheets shall be named with the fields' names — i.e., `multivalued-field-xyz` if a
   multivalued Field ID is _multivalued_field_xyz_. Each sheet must have just one value per line
   (column A), representing a short description for the value. The script will generate Field's ID and
   Display Name based on it.
1. All Fields' IDs generated by the script will be formatted to snake case (e.g., foo_bar_baz), but
   it will do the formatting job for you. So, just provide the IDs as strings.

_TIP: keep all template-related sheets in the same document ([Data Catalog Sample Tag Template][8]
for reference)._

### 5.3. Integration tests

- pytest

```sh
export GOOGLE_CLOUD_TEST_PROJECT_ID=<YOUR-PROJECT-ID>

pytest ./tests/integration/load_template_google_sheets_test.py
```

- docker

```sh
docker build --rm --tag gcp-datacatalog-python .
docker run --rm --tty \
  --env GOOGLE_CLOUD_TEST_PROJECT_ID=<YOUR-PROJECT-ID> \
  --volume <CREDENTIALS-FILE-FOLDER>:/credentials \
  gcp-datacatalog-python pytest ./tests/integration/load_template_google_sheets_test.py
```

### 5.4. Run load_template_google_sheets.py

- python

```sh
python load_template_google_sheets.py \
  --template-id <TEMPLATE-ID> --display-name <DISPLAY-NAME> \
  --project-id <YOUR-PROJECT-ID> --spreadsheet-id <SPREADSHEET-ID> \
  [--delete-existing]
```

- docker

```sh
docker build --rm --tag gcp-datacatalog-python .
docker run --rm --tty gcp-datacatalog-python \
  --volume <CREDENTIALS-FILE-FOLDER>:/credentials \
  python load_template_google_sheets.py \
  --template-id <TEMPLATE-ID> --display-name <DISPLAY-NAME> \
  --project-id <YOUR-PROJECT-ID> --spreadsheet-id <SPREADSHEET-ID> \
  [--delete-existing]
```

## 6. How to contribute

Please make sure to take a moment and read the [Code of
Conduct](https://github.com/ricardolsmendes/gcp-datacatalog-python/blob/master/.github/CODE_OF_CONDUCT.md).

### 6.1. Report issues

Please report bugs and suggest features via the [GitHub
Issues](https://github.com/ricardolsmendes/gcp-datacatalog-python/issues).

Before opening an issue, search the tracker for possible duplicates. If you find a duplicate, please
add a comment saying that you encountered the problem as well.

### 6.2. Contribute code

Please make sure to read the [Contributing
Guide](https://github.com/ricardolsmendes/gcp-datacatalog-python/blob/master/.github/CONTRIBUTING.md)
before making a pull request.

[1]: https://cloud.google.com/data-catalog
[2]: https://circleci.com/gh/ricardolsmendes/gcp-datacatalog-python.svg?style=svg
[3]: https://circleci.com/gh/ricardolsmendes/gcp-datacatalog-python
[4]: https://medium.com/google-cloud/data-catalog-hands-on-guide-a-mental-model-dae7f6dd49e
[5]: https://medium.com/google-cloud/data-catalog-hands-on-guide-search-get-lookup-with-python-82d99bfb4056
[6]: https://medium.com/google-cloud/data-catalog-hands-on-guide-templates-tags-with-python-c45eb93372ef
[7]: https://github.com/ricardolsmendes/gcp-datacatalog-python/tree/master/sample-input/load-template-csv
[8]: https://docs.google.com/spreadsheets/d/1DoILfOD_Fb1r5otEz2CUH8SKGkyV5juLakGODTTfOjY
