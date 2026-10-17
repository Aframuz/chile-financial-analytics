# CI/CD

GitHub Actions tests every pull request, plans infrastructure changes
before they merge, and delivers merged code to Google Cloud: Terraform
applies gated by risk, and DAGs synced to Cloud Composer. No service
account keys exist anywhere: workflows authenticate with Workload Identity
Federation.

## Workflows

| Workflow | Trigger | What it does |
|---|---|---|
| `ci.yml` | every PR and push to `main` | Detects what changed, then runs only the relevant jobs; `CI result` aggregates them (the one required check). On `main`, calls `deploy.yml`. |
| `terraform-plan.yml` | PR touching `terraform/` | Read-only plan, risk assessment, sticky PR comment. |
| `terraform-apply.yml` | push to `main` touching `terraform/` | Re-plans, assesses risk, applies through `infra-auto` (low) or `infra-approval` (high). |
| `deploy.yml` | called by `ci.yml` on `main`, or by hand | Syncs DAGs and code to the Composer bucket. |
| `dbt-ci-cleanup.yml` | PR closed, daily, by hand | Drops the PR's `analytics_ci_<PR>` datasets; daily sweep of leftovers. |

`ci.yml` jobs:

| Job | Runs when | Checks |
|---|---|---|
| Python tests | `src/`, `tests/`, `scripts/`, deps | `ruff check`, `pytest` (unit tests, no credentials) |
| Airflow DAGs compile | `airflow/` | `python -m compileall -q airflow/dags` (syntax, seconds) |
| Airflow DAG import tests | after compile | `pytest airflow/tests` with the Airflow version pinned in `airflow/Dockerfile`, installed with its constraints: every DAG imports, expected tasks, pools, timeouts |
| dbt parse | `dbt/` | `dbt deps && dbt parse` with the CI profile: placeholders, no credentials |
| dbt build (PR datasets) | after parse, PRs only | SQLFluff on changed files, `dbt build --full-refresh`, then an incremental run + integrity tests, all into `analytics_ci_<PR>` |
| Terraform validate | `terraform/` | `fmt -check`, `init -backend=false`, `validate` (no state, no credentials) |
| CI result | always | Fails if any job above failed or was cancelled |
| Deploy to Composer | push to `main`, after CI result | `deploy.yml`; succeeds without deploying while the demo environment is deleted |

Jobs that need GCP skip themselves until the repository variables below
exist, so CI is green before any cloud setup. They also skip on pull
requests from forks, which get no OIDC token.

## One-time setup

Do these steps in order. Terraform creates the identities the workflows
use, so the first apply runs locally, as you.

Steps 1–4 cost nothing beyond normal BigQuery usage and give you the whole
CI side: tests, PR dbt builds, Terraform plans and applies. Continuous
delivery to Cloud Composer is a separate, optional step with a real
monthly cost. Evaluate it first: see [Setting up Cloud
Composer](#setting-up-cloud-composer-on-demand-demo). Until you finish that section, the
deploy job is skipped.

### 1. Apply Terraform locally

```bash
cd terraform
terraform init
terraform plan     # review: 3 service accounts, WIF bindings, IAM
terraform apply
```

This replaces the old binding that let GitHub impersonate `financial-dbt`.
That account can write `analytics_dev`, which a pull request must never
do.

### 2. Repository variables

These are variables, not secrets: none of them is a credential. Workload
Identity Federation is what makes them safe to expose.

```bash
terraform -chdir=terraform output -json github_repository_variables \
  | jq -r 'to_entries[] | "\(.key) \(.value)"' \
  | while read -r name value; do gh variable set "$name" --body "$value"; done

gh variable set TF_OPERATOR_EMAIL --body "you@example.com"   # = operator_email in terraform.tfvars
```

The output also includes `COMPOSER_ENVIRONMENT` and `COMPOSER_LOCATION`.
They stay set whether or not the environment exists: the deploy checks
for it on every run and skips when it's gone.

To check what's set: **Settings → Secrets and variables → Actions →
Variables** tab, or `gh variable list`. A missing variable doesn't produce an error. The
jobs that need it show as *skipped*.

| Variable | Example | Used by |
|---|---|---|
| `GCP_PROJECT_ID` | `chile-financial-analytics` | everything |
| `GCP_WORKLOAD_IDENTITY_PROVIDER` | `projects/123456/locations/global/workloadIdentityPools/github-actions/providers/github-oidc` | `google-github-actions/auth` |
| `GCP_CI_SERVICE_ACCOUNT` | `gha-ci@<project>.iam.gserviceaccount.com` | dbt build, cleanup, plans |
| `GCP_TERRAFORM_SERVICE_ACCOUNT` | `gha-terraform@…` | terraform apply |
| `GCP_COMPOSER_SERVICE_ACCOUNT` | `gha-composer@…` | Composer deploy |
| `BQ_LOCATION` | `southamerica-west1` | dbt CI profile |
| `TF_OPERATOR_EMAIL` | `you@example.com` | `TF_VAR_operator_email` |
| `COMPOSER_ENVIRONMENT` | `chile-financial-airflow` | Composer deploy |
| `COMPOSER_LOCATION` | `southamerica-west1` | Composer deploy |

Terraform's inputs in CI come from these variables, plus
`github.repository`. They must match your local `terraform.tfvars`, or
every plan shows the difference as changes. Variables left at their
defaults (`region`, `environment`…) need nothing.

### 3. Environments

| Environment | Deployment branches | Reviewers | Trusted by | Variables |
|---|---|---|---|---|
| `ci` | no restriction (PR refs are `refs/pull/N/merge`) | none | `gha-ci` | — |
| `infra-auto` | `main` | none | `gha-terraform` | — |
| `infra-approval` | `main` | you | `gha-terraform` | — |
| `composer` | `main` | optional | `gha-composer` | — |

GitHub enforces an environment's rules *before* the job gets an OIDC
token. The token's subject names the environment
(`repo:<owner>/<repo>:environment:infra-approval`), and each service
account trusts exact subjects only (`terraform/ci.tf`). So "requires a
reviewer" and "main only" are enforced by GitHub *and* by GCP IAM.

UI: **Settings → Environments → New environment**. Or with `gh`:

```bash
REPO=Aframuz/chile-financial-analytics
ME=$(gh api user --jq .id)

gh api -X PUT "repos/$REPO/environments/ci"

for env in infra-auto infra-approval composer; do
  gh api -X PUT "repos/$REPO/environments/$env" --input - <<EOF
{
  "deployment_branch_policy": {"protected_branches": false, "custom_branch_policies": true},
  "reviewers": $( [[ $env == infra-approval ]] && echo "[{\"type\": \"User\", \"id\": $ME}]" || echo "[]" )
}
EOF
  gh api -X POST "repos/$REPO/environments/$env/deployment-branch-policies" \
    -f name=main -f type=branch
done
```

Creating the `composer` environment now is harmless: while no Composer
environment exists, the deploy finds nothing and succeeds.

Add yourself as a reviewer on `composer` to turn continuous deployment into
continuous delivery: every merge waits for a click before it ships.

> Required reviewers and branch rules for environments are available on
> public repositories on every plan. Private repositories need GitHub
> Pro/Team or above. The same applies to branch protection below.

### 4. Branch protection

Protect `main` with a ruleset: **Settings → Rules → Rulesets → New branch
ruleset**:

1. Name `main`, enforcement **Active**, target **Include default branch**.
2. **Restrict deletions** and **Block force pushes**.
3. **Require a pull request before merging**:
   - Required approvals: `0` if you work alone (you can't approve your own
     PRs), `1`+ with collaborators.
   - **Dismiss stale approvals when new commits are pushed**.
   - **Require conversation resolution**.
   - **Require review from Code Owners** only with collaborators
     (`.github/CODEOWNERS` covers `.github/`, `terraform/`,
     `scripts/ci/`, the Dockerfile).
4. **Require status checks to pass**: add **`CI result`**, source
   **GitHub Actions**, and tick **Require branches to be up to date**.
5. Leave the bypass list empty.

Or with `gh` (15368 is the GitHub Actions app: only Actions can satisfy the
check, not a status posted through the API):

```bash
gh api -X POST "repos/$REPO/rulesets" --input - <<'EOF'
{
  "name": "main",
  "target": "branch",
  "enforcement": "active",
  "conditions": {"ref_name": {"include": ["~DEFAULT_BRANCH"], "exclude": []}},
  "rules": [
    {"type": "deletion"},
    {"type": "non_fast_forward"},
    {"type": "pull_request", "parameters": {
      "required_approving_review_count": 0,
      "dismiss_stale_reviews_on_push": true,
      "require_code_owner_review": false,
      "require_last_push_approval": false,
      "required_review_thread_resolution": true
    }},
    {"type": "required_status_checks", "parameters": {
      "strict_required_status_checks_policy": true,
      "required_status_checks": [{"context": "CI result", "integration_id": 15368}]
    }}
  ],
  "bypass_actors": []
}
EOF
```

Require only `CI result`. The individual jobs are skipped when their files
don't change, and `Terraform plan` doesn't run at all outside `terraform/`
PRs. GitHub would leave a required check that never reports pending
forever, which blocks the merge.

## Authentication: Workload Identity Federation

### How it works

1. A job with `permissions: id-token: write` asks GitHub for an OIDC token:
   a signed JWT whose claims say which repository, branch, event and
   environment the job runs for.
2. `google-github-actions/auth` exchanges it with Google's Security Token
   Service, through the provider `github-oidc` in the pool `github-actions`.
   The provider accepts tokens issued by
   `https://token.actions.githubusercontent.com` for this repository only
   (`attribute_condition`).
3. The resulting federated identity impersonates a service account, which
   it may do only if that account grants `roles/iam.workloadIdentityUser`
   to the token's exact subject.
4. The job gets short-lived (1 h) credentials as Application Default
   Credentials, which `gcloud`, `bq`, `terraform` and dbt (`method: oauth`)
   pick up.

Nothing long-lived exists to leak. The trust is "this repository, in this
context", checked on every run.

### Configuration

All of it is in `terraform/ci.tf`. For reference, these are the equivalent
`gcloud` commands:

```bash
PROJECT=chile-financial-analytics
PROJECT_NUMBER=$(gcloud projects describe $PROJECT --format='value(projectNumber)')
REPO=Aframuz/chile-financial-analytics
POOL=projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/github-actions

gcloud services enable iam.googleapis.com iamcredentials.googleapis.com sts.googleapis.com

gcloud iam workload-identity-pools create github-actions \
  --location=global --display-name="GitHub Actions"

gcloud iam workload-identity-pools providers create-oidc github-oidc \
  --location=global --workload-identity-pool=github-actions \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.ref=assertion.ref" \
  --attribute-condition="assertion.repository == '$REPO'"

gcloud iam service-accounts create gha-ci --display-name="GitHub Actions ci"

# One binding per trusted subject
gcloud iam service-accounts add-iam-policy-binding gha-ci@$PROJECT.iam.gserviceaccount.com \
  --role=roles/iam.workloadIdentityUser \
  --member="principal://iam.googleapis.com/$POOL/subject/repo:$REPO:environment:ci"
```

In a workflow:

```yaml
permissions:
  contents: read
  id-token: write   # lets the job request the OIDC token

steps:
  - uses: google-github-actions/auth@<sha> # v3
    with:
      workload_identity_provider: ${{ vars.GCP_WORKLOAD_IDENTITY_PROVIDER }}
      service_account: ${{ vars.GCP_CI_SERVICE_ACCOUNT }}
```

If authentication fails with `Permission 'iam.serviceAccounts.getAccessToken'
denied`, the token's subject doesn't match a binding. Common causes: a job
that isn't in the expected environment, the wrong branch, or different
letter case in the repository name. Subjects are case-sensitive.

Possible hardening: repository names can be reused after a rename or
deletion. To guard against that, map `attribute.repository_id =
assertion.repository_id` and condition on the numeric ID
(`gh api repos/$REPO --jq .id`) instead of the name.

## Identities

| Service account | Trust level | Subjects | Can |
|---|---|---|---|
| `gha-ci` | unreviewed code | `environment:ci`, `pull_request`, `ref:refs/heads/main` | create datasets and run BigQuery jobs (`bigquery.user`); read `raw_bcch` and `monitoring`; read the project and its IAM for plans (`viewer`, `iam.securityReviewer`); read Terraform state |
| `gha-terraform` | reviewed, on `main` | `environment:infra-auto`, `environment:infra-approval` | manage what `terraform/` manages: BigQuery, Storage, service accounts, WIF, project IAM, APIs, Composer, Secret Manager; act as `financial-composer` to create the environment |
| `gha-composer` | reviewed, on `main` | `environment:composer` | describe Composer and run Airflow CLI commands (`composer.user`); write objects in the Composer bucket only (it outlives the environment, so the grant does too) |
| `financial-ingestion`, `financial-dbt` | runtime | none (local Airflow, impersonation) | unchanged |
| `financial-composer` | runtime | none (Composer workers) | ingestion and dbt access (BigQuery jobs, `raw_bcch`, `monitoring`, `analytics_dev`, snapshots, raw bucket), the BCCh token secret, `composer.worker` |

How the PR datasets are isolated: BigQuery makes whoever creates a dataset
its owner. `gha-ci` therefore owns the `analytics_ci_*` datasets it creates
and nothing else. It has no grant on `analytics_dev`, so a PR's dbt run
can't touch production models, and one PR's datasets are never another
PR's.

`gha-terraform` holds project IAM admin, which makes it as powerful as an
owner. This is inherent: it manages IAM. The environments, the risk gate
and branch protection are what keep it to reviewed code.

## Per-PR dbt environments

```
PR #12 → analytics_ci_12, analytics_ci_12_snapshots
PR #13 → analytics_ci_13, analytics_ci_13_snapshots
```

- **Profile.** The `ci` target of
  `dbt/chile_financial_analytics/profiles/profiles.yml` reads everything
  from the environment (`DBT_GCP_PROJECT`, `DBT_CI_DATASET`,
  `DBT_BQ_LOCATION`) and falls back to placeholders, so `dbt parse` runs
  with no credentials at all.
- **Sources.** Sources still read the real `raw_bcch` and `monitoring`,
  read-only.
- **Operational tests.** `tag:operational` tests (freshness, stuck runs,
  gaps) are excluded. They judge today's production data, not the change,
  and they run in Airflow.
- **Cleanup.** The datasets are dropped when the PR closes. A daily sweep
  drops any `analytics_ci_<N>` whose PR is closed or merged, which covers
  cancelled runs and failed cleanups. The sweep only deletes on a definite
  answer from the GitHub API.
- **Not yet done: slim CI.** With a production `manifest.json` available,
  `dbt build --select state:modified+ --defer --state prod/` would build
  only the changed models and read the rest from `analytics_dev`.

## Risk-gated infrastructure changes

`scripts/ci/tf_plan_risk.py` reads `terraform show -json` and classifies
the plan. The rules are an allowlist, so anything not known to be safe is
high risk:

| Risk | When | Path |
|---|---|---|
| `none` | no resource changes (imports with no changes included) | nothing to apply |
| `low` | only creates of datasets, tables or APIs, and in-place updates touching only labels, description or display names | `infra-auto`: applied on merge |
| `high` | any delete or replacement, anything IAM, service accounts, WIF, KMS or secrets, other attribute updates (e.g. table expiration), unknown types or actions | `infra-approval`: waits for a reviewer |

The PR comment shows the risk before merge. On `main`, the plan job
classifies the plan, and the apply job applies that exact saved plan. If
state changed while it waited for approval, Terraform refuses the stale
plan; re-run the workflow. `prevent_destroy` on the raw bucket and
`raw_bcch` still applies on top of all this.

Widen the low-risk lists in the script only with tests
(`tests/test_tf_plan_risk.py`). `.github/CODEOWNERS` puts the script under
owner review.

## Local Airflow and Composer: one codebase

The same DAGs, `src/` and dbt project run unchanged on both. Everything
that differs comes from the environment and is resolved at run time
(`airflow/dags/bcch_common.py`):

| | Local (`docker-compose.airflow.yml`) | Cloud Composer |
|---|---|---|
| Airflow | `apache/airflow:<AIRFLOW_VERSION>-python<PYTHON_VERSION>` image + constraints | `composer-3-airflow-<AIRFLOW_VERSION>-build.N`. The deploy and Terraform refuse any other version |
| Project code | mounted at `/opt/airflow/project` | synced to `data/project/`, i.e. `/home/airflow/gcs/data/project` |
| `src/` packages | `airflow/requirements.txt` in the image | the same file, as `pypi_packages` (`terraform/composer.tf`) |
| dbt | venv baked into the image (`DBT_BIN`) | venv built on the worker by the first dbt task, from the same lock (`DBT_REQUIREMENTS`) |
| dbt profile | `profiles/profiles.yml`, target `airflow` | the same file and target |
| GCP identity | your ADC | the `financial-composer` service account |
| BCCh token | `BCCH_API_TOKEN` in `.env` | Airflow Variable `bcch_api_token`, from Secret Manager |
| `bcch_bigquery` pool | `airflow-init` | the deploy (idempotent) |
| Task logs | `airflow/logs`, copied to GCS | Cloud Logging and the environment bucket |

Local Airflow no longer mounts `~/.dbt`. Interactive dbt on your machine
still uses `~/.dbt/profiles.yml`.

## Continuous delivery to Cloud Composer

`deploy.yml` runs after every check passes on `main` (or by hand):

1. **Find the environment.** If it doesn't exist (the demo is deleted), the
   deploy logs a notice and **succeeds without deploying**. Any other error
   (permissions, API) fails.
2. **Version check.** It fails if Composer's Airflow version differs from
   `ARG AIRFLOW_VERSION` in `airflow/Dockerfile`.
3. **dbt packages.** It runs `dbt deps`; packages aren't in git.
4. **Code sync.** It syncs `src/`, `config/`, `metadata/`, the dbt project
   (packages included) and the dbt lock to `gs://<bucket>/data/project/`.
5. **DAG sync.** It then syncs `airflow/dags/` to `gs://<bucket>/dags/`.
   Code goes first, so a new DAG never runs against old code. Both syncs
   delete files that were removed from the repository.
6. **Pool.** It ensures the `bcch_bigquery` pool exists.
7. **Import check.** It waits for a re-parse and fails if
   `airflow dags list-import-errors` reports anything.

## Setting up Cloud Composer (on-demand demo)

> **Nothing in this section has been run.** Each step is a command or
> change to review before you run it.

Composer here is a demo environment: created when needed, deleted after.
The repository is built so that its absence breaks nothing:

- **Only the environment toggles.** `google_composer_environment` exists
  only while `composer_enabled = true` in
  `terraform/composer.auto.tfvars`. That file is committed, so creating and
  deleting are both reviewed PRs, high risk, approved in `infra-approval`.
- **Everything around it is permanent.** The bucket, the
  `financial-composer` identity and its grants, the token secret and the
  APIs stay, and cost cents a month. Recreating keeps the same bucket name
  and IAM, with nothing to reconfigure.
- **CI doesn't notice.** While the environment is deleted, CI stays green:
  the deploy finds nothing and skips. `COMPOSER_ENVIRONMENT` stays set.
- **Recreation is self-configuring.** A recreated environment gets its
  packages, environment variables and secrets backend from Terraform, and
  its code, dbt packages and pool from the first deploy.

Google now markets Cloud Composer as *Managed Service for Apache Airflow*.
The `gcloud composer` commands and the `google_composer_environment`
Terraform resource are unchanged.

### Step 0: Budget

Composer 3 bills **DCU-hours** (Data Compute Units, roughly 1 vCPU-hour or
1 GB-hour of RAM) for every Airflow component, plus database and
environment storage. Three things matter for a budget:

- **It runs 24/7.** The scheduler, web server, DAG processor and database
  never scale to zero.
- **It can't be paused.** The only way to stop paying is to delete it.
- **Workers are the only part that scales with load.** This pipeline
  barely uses them.

Rough order of magnitude, from third-party reports (Google doesn't
publish a per-size figure):

| Item | Estimate |
|---|---|
| DCU price | ~$0.06 per DCU-hour (Iowa list price) |
| Small environment | ~9–12 DCU per hour → **~$13–17 per day**, ~$390–520 per month |
| A 3-day demo | **~$40–55**, plus extras |
| `southamerica-west1` | Santiago is usually priced above Iowa: expect more |
| Extras | Cloud SQL storage (10 GiB minimum), logs, network egress |
| While deleted | bucket storage and one secret version: cents |

`terraform/composer.tf` sizes each component below the small preset's
defaults (`workloads_config`), which lowers the DCU count. Price that
configuration in the [pricing
calculator](https://cloud.google.com/products/calculator) for your region.

**Create a budget with alerts before creating anything.** It doesn't cap
spending, it only notifies you:

```bash
gcloud billing accounts list    # find the account ID
gcloud billing budgets create \
  --billing-account=<BILLING_ACCOUNT_ID> \
  --display-name="chile-financial-analytics" \
  --budget-amount=<MONTHLY_LIMIT>USD \
  --filter-projects=projects/chile-financial-analytics \
  --threshold-rule=percent=0.5 \
  --threshold-rule=percent=0.9 \
  --threshold-rule=percent=1.0
```

The biggest cost risk is forgetting to delete the environment. Put the
teardown (step 5) in your calendar when you create it.

Cheaper alternatives, if a demo isn't enough:

- a Compute Engine VM running `docker-compose.airflow.yml`: ~$10–50 a
  month, but a different deploy job;
- staying local: $0.

### Step 1: Check prerequisites

- **Region.** Check that Composer 3 is available in `southamerica-west1`
  ([locations](https://cloud.google.com/composer/docs/composer-3/environment-locations)).
  If it isn't, set `region` for Composer to the nearest one. BigQuery and
  GCS are reached over the API.
- **Image.** On the [versions
  page](https://cloud.google.com/composer/docs/composer-versions), find a
  Composer 3 image with the Airflow version pinned in `airflow/Dockerfile`
  (currently **3.3.1**: `composer-3-airflow-3.3.1-build.<N>`).
  If none exists, change the pin in its own PR first:
  1. Edit `ARG AIRFLOW_VERSION` in `airflow/Dockerfile`.
  2. Run `scripts/lock-deps.sh`.
  3. Let CI's DAG import tests prove the DAGs work on that version.

  Terraform refuses an image that doesn't match the pin.
- **Python version.** In the image's package list, check its Python
  version, and set `ARG PYTHON_VERSION` in `airflow/Dockerfile` to the
  same major.minor (currently 3.11). The locks, CI, and the dbt venv
  Composer's workers build all follow that pin. Workers build the venv
  with the image's own Python, so the two must match.
- **Package conflicts.** Composer preinstalls its own versions of pandas,
  google-cloud-storage, google-cloud-bigquery and db-dtypes. If
  `airflow/requirements.txt` pins different ones, the environment update
  may fail. If it does, move our pin to Composer's version and let the
  tests run on it.

### Step 2: Lay the foundation (no environment yet)

With `composer_enabled = false`, apply as usual: through a PR, or locally
for the first time. This creates:

- the `composer` and `secretmanager` APIs;
- the bucket `aframuz-<project>-composer`;
- `financial-composer` and its BigQuery and GCS grants;
- the Composer service agent role;
- the secret `airflow-variables-bcch_api_token`, empty;
- `gha-terraform`'s rights to create environments (`composer.admin`,
  acting as `financial-composer`).

The plan is high risk (IAM), so it waits for approval. Then:

```bash
# The token's value never goes through Terraform or git
printf '%s' "$BCCH_API_TOKEN" | gcloud secrets versions add \
  airflow-variables-bcch_api_token --data-file=-

# Refresh the repository variables: COMPOSER_ENVIRONMENT and
# COMPOSER_LOCATION are now in the output (setup step 2)
terraform -chdir=terraform output -json github_repository_variables \
  | jq -r 'to_entries[] | "\(.key) \(.value)"' \
  | while read -r name value; do gh variable set "$name" --body "$value"; done
```

Nothing is billed by the hour yet. From now on, every merge that touches
DAGs or code runs the deploy, which skips cleanly.

### Step 3: Create the environment

1. **Open a PR.** In `terraform/composer.auto.tfvars`:

   ```hcl
   composer_enabled       = true
   composer_image_version = "composer-3-airflow-3.3.1-build.<N>"
   ```

2. **Review the plan comment.** It should show one
   `google_composer_environment` to create, with `Risk: high`.
3. **Merge, then approve the apply** in `infra-approval`. Creation takes
   about 25 minutes.

If creation fails on the bucket already holding objects from a previous
environment, clear its working folders and apply again. Keep
`snapshots/`; the deploy repopulates the rest:

```bash
gcloud storage rm -r "gs://aframuz-chile-financial-analytics-composer/dags/**" \
  "gs://aframuz-chile-financial-analytics-composer/data/**" \
  "gs://aframuz-chile-financial-analytics-composer/logs/**"
```

### Step 4: Deploy and demo

1. **Deploy.** Go to **Actions → Deploy to Cloud Composer → Run workflow**
   on `main`, and approve it in the `composer` environment if it has a
   reviewer. It fetches dbt packages, syncs code and DAGs, creates the
   pool and checks for import errors.
2. **Open the Airflow UI.** Get the URL with
   `terraform -chdir=terraform output composer_airflow_uri`. Both DAGs are
   listed, paused.
3. **Run the pipeline.** Unpause and trigger `bcch_financial_pipeline`. The
   first dbt task on a worker builds dbt's venv (a few extra minutes), and
   later tasks reuse it. Check that:
   - raw files land in GCS;
   - `raw_bcch` and `monitoring.ingestion_runs` update;
   - dbt builds `analytics_dev`.

   Composer writes to the same datasets as local Airflow, so don't run
   both at once.
4. **Watch the cost.** After a day, open **Billing → Reports**, filter by
   the Composer service, and compare the daily cost with step 0.

### Step 5: Tear down

1. **Snapshot (optional).** This keeps run history and Variables for the
   next demo:

   ```bash
   gcloud composer environments snapshots save chile-financial-airflow \
     --location southamerica-west1 \
     --snapshot-location gs://aframuz-chile-financial-analytics-composer/snapshots
   ```

2. **Delete.** Open a PR with `composer_enabled = false`, then merge and
   approve. Terraform deletes the environment and nothing else.
3. **Check.** Hourly billing stops. The next merge's deploy logs
   "doesn't exist: nothing to deploy" and CI stays green.

### Recreating

Repeat step 3, which needs the same image or a newer one with the same
Airflow version. Then run the deploy. To restore the previous run history:

```bash
gcloud composer environments snapshots load chile-financial-airflow \
  --location southamerica-west1 \
  --snapshot-path gs://aframuz-chile-financial-analytics-composer/snapshots/<snapshot>
```

## Dependencies and reproducibility

| File | Contents | Used by |
|---|---|---|
| `airflow/requirements.txt` | runtime deps of `src/`, pinned to Airflow's constraint versions | Airflow image, Composer, DAG tests |
| `requirements-dev.in` → `requirements-dev.txt` | the above + pytest + ruff, **full lock** resolved against Airflow's constraints | Python tests |
| `requirements-dbt-ci.in` → `requirements-dbt-ci.txt` | dbt + SQLFluff + dbt templater, full lock | dbt jobs |
| `airflow/requirements-dbt.in` → `airflow/requirements-dbt.txt` | dbt only, constrained to the CI lock's versions | Airflow image dbt venv |

- **Runtime pins.** `airflow/Dockerfile` holds the only two:
  `ARG AIRFLOW_VERSION` and `ARG PYTHON_VERSION` (major.minor: Airflow
  publishes one constraints file per minor version). The image installs
  Airflow with
  `constraints-<AIRFLOW_VERSION>/constraints-<PYTHON_VERSION>.txt`. These
  all read the same two `ARG`s:
  - `scripts/lock-deps.sh`;
  - every CI job's Python, and the DAG tests' Airflow install;
  - the deploy;
  - `terraform/composer.tf`;
  - ruff's `target-version`, which has to be edited by hand.
- **Constraint conflicts.** A package that is both in our requirements and
  in Airflow's constraints must use the constraint's version, or pip
  refuses to install. That is how `airflow/requirements.txt` gets its
  versions of pandas, python-dotenv, google-cloud-storage and
  google-cloud-bigquery.
- **dbt versions.** dbt and SQLFluff are resolved together, and the image's
  dbt venv is then constrained to those versions. CI lints and builds with
  exactly the dbt that Airflow runs.
- **Regenerating locks.** After changing a `*.in` file,
  `airflow/requirements.txt` or a runtime pin, run `scripts/lock-deps.sh`
  (needs `uv`) and commit the results.
- **Pinned tools.** Python (from the Dockerfile), uv, Terraform (in
  `.github/actions/setup-terraform`) and every action, by commit SHA.
  Dependabot proposes action and provider bumps weekly.
- **Caching.** Cache keys are derived from the lock files, so a cache can
  never serve versions other than the locked ones:
  - uv's cache, keyed on each job's lock file;
  - Terraform providers, keyed on `.terraform.lock.hcl`;
  - `dbt_packages/`, keyed on `package-lock.yml`.

### Upgrading Airflow or Python

1. **Edit the pins.** In `airflow/Dockerfile`, set `ARG AIRFLOW_VERSION`
   and/or `ARG PYTHON_VERSION` (major.minor, e.g. `3.11`, never
   `3.11.8`). If Python changed, also set `target-version` in
   `pyproject.toml`.
2. **Regenerate the locks** with `scripts/lock-deps.sh`. If it fails with
   `you require <pkg>==A and <pkg>==B`, a pin in
   `airflow/requirements.txt` disagrees with the new constraints. Look up
   the constraint's version and use it:

   ```bash
   AIRFLOW=$(sed -n 's/^ARG AIRFLOW_VERSION=//p' airflow/Dockerfile)
   PYTHON=$(sed -n 's/^ARG PYTHON_VERSION=//p' airflow/Dockerfile)
   curl -fsSL "https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW/constraints-$PYTHON.txt" \
     | grep -iE '^(pandas|python-dotenv|pyyaml|google-cloud-storage|google-cloud-bigquery|db-dtypes)=='
   ```

   Then run the script again.
3. **Open a PR.** A Dockerfile change re-runs every CI job on the new
   versions. If Composer exists, update `composer_image_version` to an
   image with the new Airflow in the same PR, or Terraform rejects the
   plan.

The root `requirements.txt` is a local freeze, with extras such as
metricflow and aiplatform, and CI no longer uses it.

## SQLFluff

`dbt/chile_financial_analytics/.sqlfluff`: dbt templater, BigQuery dialect,
core rules, lowercase keywords and functions.

- **Where it runs.** The dbt templater compiles through dbt, which warms
  its relation cache and therefore needs a live BigQuery connection. That
  is why linting runs in the authenticated `dbt build` job, not in the
  credential-free `dbt parse`.
- **What it lints.** Only `.sql` files the PR changes. The existing models
  mostly fail LT02 (indentation), so code converges as files are touched,
  without one restyling commit.

Locally, with ADC:

```bash
cd dbt/chile_financial_analytics
DBT_GCP_PROJECT=chile-financial-analytics DBT_CI_DATASET=analytics_dev \
  sqlfluff lint models/marts/core/dim_series.sql
sqlfluff fix models/marts/core/dim_series.sql   # applies the fixes
```

## Avoiding unnecessary runs

- **`ci.yml`** has no workflow-level `paths`, because its `CI result` is
  required, and a workflow skipped by path filters never reports, which
  blocks the PR. Instead, a 10-second `changes` job (`dorny/paths-filter`)
  enables only the affected jobs. A README-only PR runs `changes` and
  `CI result` and installs nothing. A change to `ci.yml` itself runs every
  job.
- **`terraform-plan.yml` and `terraform-apply.yml`** use workflow-level
  `paths`. Neither is a required check.
- **Concurrency.** A new push to a PR cancels that PR's previous run. Runs
  on `main`, applies and deploys are queued, never cancelled.

## CI badge

GitHub serves a status badge per workflow:

```
https://github.com/<owner>/<repo>/actions/workflows/<workflow-file>/badge.svg
```

Add it to the README as a link to the workflow's runs:

```markdown
[![CI](https://github.com/Aframuz/chile-financial-analytics/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Aframuz/chile-financial-analytics/actions/workflows/ci.yml?query=branch%3Amain)
```

- **`?branch=main`.** Shows `main`'s status. Without it, the badge shows
  the latest run on any branch, so one failing PR turns it red.
- **`?event=push`.** Optional: restricts the badge to push runs.
- **Generating it.** **Actions → CI → ⋯ → Create status badge** produces
  the same Markdown.
- **Other workflows.** Each workflow gets its own badge; swap the file
  name, e.g. `terraform-apply.yml`.
- **Private repositories.** The badge only renders for viewers who can
  access the repository.
