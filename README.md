# folio-data-anonymization

Copyright (C) 2025 The Open Library Foundation

This software is distributed under the terms of the Apache License,
Version 2.0. See the file "[LICENSE](LICENSE)" for more information.

## Introduction
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

## Airflow
Create a secret.yaml file:
```
apiVersion: v1
kind: Secret
metadata:
  name: airflow-user
type: Opaque
data:
  airflow-fernet-key: {any fernet key}
  airflow-password: {password from vault}
  airflow-secret-key: {any secret key}
  airflow-jwt-secret-key: {any JWT key}
```

Instructions on generating a Fernet key can be found at [How-to Guides: Securing Connections](https://airflow.apache.org/docs/apache-airflow/1.10.4/howto/secure-connections.html?highlight=fernet)
Example:
```
poetry run python3
>>> from cryptography.fernet import Fernet
>>> fernet_key= Fernet.generate_key()
>>> decoded_fernet_key = fernet_key.decode()
echo -n $decoded_fernet_key | base64
```

Then apply it using `kubectl -n $namespace apply -f secret.yaml`

## Install Apache Airflow in a Kubernetes cluster 
#### Local development:
With [Docker Desktop](https://docs.docker.com/desktop/), [Helm](https://helm.sh/docs/intro/install/) and [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl-macos/) installed, enable [Kubernetes in Docker Desktop](https://docs.docker.com/desktop/features/kubernetes/)

Then, using Helm:
```
helm --namespace $NAMESPACE install -f airflow-values.yaml airflow oci://registry-1.docker.io/bitnamicharts/airflow
```

To upgrade airflow release, do:
```
export PASSWORD=$(kubectl get secret --namespace $NAMESPACE airflow-postgresql -o jsonpath="{.data.password}" | base64 -d)
helm --namespace $NAMESPACE upgrade --set global.postgresql.auth.password=$PASSWORD -f airflow-values.yaml airflow oci://registry-1.docker.io/bitnamicharts/airflow
```
