## folio-data-anonymization
Folio Data Anonymization is a service that anonymizes or masks patron data in library records to ensure privacy protection.

## Environment Variables 
After cloning this repository, create a local .env file and add the following variables:

```bash
export PGHOST="localhost"
export PGPORT=5432
export PGUSER="user"
export PGPASSWORD="password"
export PGDATABASE="dbname"
export TENANT="diku"
```

## Dependency Management and Packaging
To install the dependencies, run:
- `pipx install poetry` or `pip install -r requirements.txt`
- `poetry install`

## Running
- `source .env`
- `poetry run`

## Tests
To run tests, install postgresql (Mac OSX) or libpq (Ubuntu):
`brew install postgresql`
`sudo apt-get install libpq-dev`
Running the tests:
- `poetry run pytest tests`