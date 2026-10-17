# Cloud Composer demo environment (docs/ci-cd.md, "Setting up Cloud
# Composer"). Committed, so creating or deleting the environment is a
# reviewed PR whose plan is high risk and waits for approval in
# infra-approval. Snapshot before setting it back to false.
composer_enabled = false

# Must run the Airflow version pinned in airflow/Dockerfile:
# composer-3-airflow-<AIRFLOW_VERSION>-build.<N>
composer_image_version = ""
