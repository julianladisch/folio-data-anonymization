# folio-data-anonymization

Copyright (C) 2025 The Open Library Foundation

This software is distributed under the terms of the Apache License,
Version 2.0. See the file "[LICENSE](LICENSE)" for more information.

## Introduction
Folio Data Anonymization is a service that anonymizes or masks patron data in library records to ensure privacy protection.

## Dependency Management and Packaging
To install the dependencies, run:
- `pipx install poetry` or `pip install -r requirements.txt`
- `poetry install`

## Tests
Running the tests:
- `poetry run pytest tests`

## Airflow Setup and Security
Create a secret.yaml file:
```
apiVersion: v1
kind: Secret
metadata:
  name: airflow-user
type: Opaque
data:
  airflow-fernet-key: {any fernet key}
  airflow-password: {choose a password}
  airflow-secret-key: {any secret key}
  airflow-jwt-secret-key: {any JWT key}
```

To generate the secret key and the JWT key you may refer to https://docs.python.org/3/library/secrets.html for guidance.


Instructions on generating a Fernet key can be found at [How-to Guides: Securing Connections](https://airflow.apache.org/docs/apache-airflow/1.10.4/howto/secure-connections.html?highlight=fernet)
Example:
```
poetry run python3
>>> from cryptography.fernet import Fernet
>>> fernet_key= Fernet.generate_key()
>>> decoded_fernet_key = fernet_key.decode()
echo -n $decoded_fernet_key | base64
```

Once you have generated the desired keys and password apply it to your Kunernetes cluster using:
```
export NAMESPACE=<my-namespace>
kubectl -n $NAMESPACE apply -f secret.yaml`
```

## Install and maintain Apache Airflow in a Kubernetes cluster 
### Local development:
With [Docker Desktop](https://docs.docker.com/desktop/), [Helm](https://helm.sh/docs/intro/install/) and [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl-macos/) installed, enable [Kubernetes in Docker Desktop](https://docs.docker.com/desktop/features/kubernetes/)

### Using Helm to deply Airflow in the cluster:
```
export NAMESPACE=<my-namespace>
kubectl -n $NAMESPACE apply -f pv-volume.yaml
envsubst < airflow-values.yaml > ns-airflow-values.yaml
helm -n $NAMESPACE install --version 22.7.3 -f ns-airflow-values.yaml airflow oci://registry-1.docker.io/bitnamicharts/airflow
```

Note: in the `pv-volume.yaml` file you must use a storageClass that supports ReadWriteMany. If you do not specify a storageClassName, the default storageClass for your cluster will be used.


To upgrade or to reinitialize the airflow release when configuration changes are made, do:
```
envsubst < airflow-values.yaml > ns-airflow-values.yaml
export PASSWORD=$(kubectl get secret -n $NAMESPACE airflow-postgresql -o jsonpath="{.data.password}" | base64 -d)
helm -n $NAMESPACE upgrade --install --version 22.7.3 --set global.postgresql.auth.password=$PASSWORD -f ns-airflow-values.yaml airflow oci://registry-1.docker.io/bitnamicharts/airflow
```

#### Test changes to a DAG using a ConfigMap. 
Create a configmap from your dag file:
```
kubectl -n $NAMESPACE create configmap my-dag --from-file=folio_data_anonymization/dags/my-dag.py
```
Temporarily update airflow-values.yaml to the following and then helm upgrade:
```
configuration:
  core:
    # dags_folder: "/opt/bitnami/airflow/dags/git_dags"
    dags_folder: "/opt/bitnami/airflow/dags"
...
dags:
  enabled: true
  existingConfigmap: my-dag
  # repositories:
  #   - repository: "https://github.com/folio-org/folio-data-anonymization.git"
  #     branch: "main"
  #     name: "dags"
  #     path: /folio_data_anonymization/dags
```
To re-test the dag, delete the configmap and re-apply it, then restart the deployment.
