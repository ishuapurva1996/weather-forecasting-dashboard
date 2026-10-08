# Weather dashboard operations

The public weather dashboard uses the approved static frontend in `web_dashboard/`. Only allowlisted HTML/CSS/JavaScript and validated chart-ready weather JSON enter the Pages artifact. Private warehouse credentials, Airflow run identifiers, the S3 pointer, raw files, and dbt artifacts stay private.

## Current state

Snapshot publication uses the existing real August 3, 2026 export, with actual weather through August 2. Capture and warehouse completion timestamps were not recorded in that export and remain `null`; reassembly does not change its original export date. The browser shows a stale-data warning.

A new complete ingestion was attempted on October 7, 2026. Open-Meteo extraction succeeded, but the Snowflake load failed because the weather account's trial expired and its warehouses were suspended. Scheduled ingestion is paused until the connection is restored. No automatic weather export or deployment has been verified. Contract and failure-path checks passed. The [October 7, 2026 snapshot Pages deployment](https://github.com/ishuapurva1996/weather-forecasting-dashboard/actions/runs/37707861863) succeeded on commit `70a9559`; deployment and public checksum verification steps both ran. The served JSON matched the selected snapshot, and all 14 charts, city/theme controls, provenance panel, desktop/mobile layout, and repository base path were checked on the live site without console errors. The README preview is a screenshot of this verified public page, stored as a required product asset.

## Two mutually exclusive publication routes

Keep GitHub Pages set to **GitHub Actions**. Restrict the `github-pages` environment to `main`. Both publishers use `weather-dashboard-pages` concurrency with cancellation disabled.

With `DASHBOARD_PUBLICATION_MODE` unset or `snapshot`, `deploy-dashboard-snapshot.yml` publishes the reviewed export and checksum under `web_dashboard/snapshot/`. It resolves current `main`, checks the real weather contract, packages only public assets, and verifies the public JSON SHA-256 after deployment. It needs no AWS or Snowflake credentials. Replacing the snapshot is a deliberate data update; a frontend redeploy does not run the warehouse pipeline.

With `DASHBOARD_PUBLICATION_MODE=airflow`, `deploy-dashboard.yml` uses the latest validated private S3 bundle. It reads current `main` and the current pointer after acquiring the publication lock. It checks both again after uploading the artifact and rebuilds at most twice if inputs changed. Repeated changes fail safely before deployment. Both publication routes use the URL returned by `actions/deploy-pages` and verify the public bundle identity.

Do not enable automatic mode until one complete weather run has produced a verified first private bundle and the scoped AWS reader is configured. A manually dispatched deployment proves that handoff, not that a later scheduled run occurred.

## Success-only upstream export

The daily ingestion schedule remains `20 19 * * *` in UTC: 12:20 PM PDT / 11:20 AM PST. Weather API dates use `America/Los_Angeles`. The computer, Docker, PostgreSQL, scheduler, and Airflow webserver must be available for scheduled refresh. This is a local pipeline, not an always-on hosted scheduler.

Ingestion, forecast, and dbt DAGs carry explicit parent run IDs. Parent trigger tasks now wait for their children, and each DAG permits one active run. Warehouse-writing tasks and each dbt command return an attempt-specific receipt only after their real work succeeds. A manually marked success cannot create a matching receipt.

`export_dashboard_bundle` checks all required predecessors and receipts through the Airflow 2.10 stable REST API. Receipt identity and attempt must match, and receipt start/completion must lie inside the task's recorded interval. It scans warehouse-writing tasks across all three weather DAGs and rejects active, overlapping, or later writes from another run. It repeats the evidence check after extraction and before pointer publication. Changes made directly in Snowflake outside these DAGs are not covered by this guard; require a fresh complete pipeline after such writes.

The export validates explicit weather fields for San Jose and Los Angeles, 50–60 history days and exactly seven subsequent forecast days per city, forecast bounds, WMO codes, numeric ranges, derived KPI consistency, chart coverage, and a 2 MiB maximum. Accuracy includes only predictions made 1–7 days ahead; an empty accuracy population is legitimate and displays unavailable metrics. Revisions are bounded to dates in the last 60 days. Oversized datasets fail rather than silently truncate. Unknown historical provenance stays `null`; automatic exports require known capture/build timestamps and actual weather no more than three days old.

The publisher first writes and byte-verifies `bundles/<sha256>.json`, then conditionally updates `latest-success.json`. An older build cannot replace a newer one, and compare-and-swap prevents a concurrent writer from silently replacing the pointer. Extraction, validation, or upload failure leaves the prior pointer intact. The S3 pointer contains private run and object identifiers; it is never packaged into the site.

The dispatch task submits only after a verified private export. HTTP 204 means queued; verify that the deployment job actually ran and succeeded. No Snowflake connection is needed in GitHub Actions or the browser.

## Exact settings

Airflow uses the existing `snowflake_conn`. Configure the following in the checkout's ignored `.env`, then recreate the weather Airflow service so it receives changed values.

| Local setting | Purpose |
| --- | --- |
| `DASHBOARD_S3_BUCKET` | Existing private export bucket. |
| `DASHBOARD_S3_PREFIX` | Dedicated weather prefix, `dashboard/weather-v1`. |
| `DASHBOARD_AWS_REGION` | Bucket region; currently `us-west-1`. |
| `AWS_CREDENTIALS_DIR` | Absolute directory for the existing AWS credential profile, mounted read-only. |
| `DASHBOARD_GITHUB_TOKEN` | Expiring fine-grained token for this weather repository only, Actions read/write. |
| `DASHBOARD_AIRFLOW_USERNAME`, `DASHBOARD_AIRFLOW_PASSWORD` | Existing metadata reader login for the weather Airflow API. A dedicated reader limited to these DAGs, task instances, run configuration, and required XCom receipts is preferred. |

Compose sets `DASHBOARD_AIRFLOW_API_URL=http://localhost:8080` inside the weather container. Keep it local, or use HTTPS for a remote service. The initial local configuration uses the existing Airflow login; no new user or credential was created. Never copy the movie project's GitHub token into weather or paste tokens into chat.

Configure repository Actions settings for **ishuapurva1996/weather-forecasting-dashboard**, not account settings:

| Name | Storage | Consumer |
| --- | --- | --- |
| `DASHBOARD_AWS_ROLE_ARN` | [Actions secret](https://github.com/ishuapurva1996/weather-forecasting-dashboard/settings/secrets/actions) | Weather automatic Pages workflow. |
| `DASHBOARD_S3_BUCKET` | Actions secret | Weather automatic Pages workflow. |
| `DASHBOARD_S3_PREFIX` | Actions secret | Weather automatic Pages workflow; `dashboard/weather-v1`. |
| `DASHBOARD_AWS_REGION` | [Actions variable](https://github.com/ishuapurva1996/weather-forecasting-dashboard/settings/variables/actions) | AWS authentication; `us-west-1`. |
| `DASHBOARD_PUBLICATION_MODE` | Actions variable | Both publishers; set to `airflow` only after setup verification. |

A variable does not satisfy a workflow secret lookup. Legacy `SNOWFLAKE_*` Actions secrets are no longer consumed by these workflows.

## AWS reader permissions

Create a separate `WeatherDashboardPagesReader` role. Trust GitHub OIDC with audience `sts.amazonaws.com` and the weather repository's actual `github-pages` environment subject. This repository was created before July 15, 2026, but verify whether immutable or custom subjects have been enabled before applying the trust condition. Enforce the `main` branch through the GitHub environment.

Grant only `s3:GetObject` for the weather prefix's `latest-success.json` and `bundles/*`. The Pages reader needs no writes, deletes, bucket listing, or warehouse permissions. The upstream writer needs the corresponding prefix's GetObject/PutObject and first-pointer existence detection. Keep the bucket private; public access comes through Pages. Do not broaden the movie reader's trust or reuse its dispatch token by default.

IAM inspection was denied to the current local AWS principal. An authorized AWS administrator must configure the weather reader. The repository does not claim that this permission is installed.

## Recovery and rotation

- If Snowflake load fails, restore the weather connection and execute a fresh complete ingestion. Never mark failed tasks successful to bypass receipts.
- If export eligibility fails, inspect the predecessor attempt and overlapping runs; require a fresh complete chain when evidence changed.
- If export validation or S3 write fails, inspect the first failed step. Retry export only while its source build remains eligible; do not publish partial or sample data.
- If export succeeded but dispatch failed, repair the weather-only token and recreate the service, then retry only dispatch or manually run the enabled Pages workflow.
- If OIDC fails, check the exact repository/environment subject, audience, and role trust. If authentication succeeds but S3 reads fail, check the exact prefix, object existence, region, GetObject permission, and encryption.
- If assembly fails, the last Pages deployment remains. If the public verification step fails after deployment, new content may already be visible; this workflow does not provide automatic rollback.

Rotate the local dispatch token and metadata login before expiry, then recreate Airflow. Preserve the latest pointer, its immutable target, the deployed bundle, and evidence for active publication. Do not apply blanket object expiration that could remove the last good export during an outage. Preserve required Airflow task/XCom history; scans fail at 10,000 rows rather than ignoring old mutations. After metadata cleanup, require a fresh complete build.

Weather data attribution: [Open-Meteo](https://open-meteo.com/) under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Historical API values are archived weather-model estimates; the dashboard calls these actuals to distinguish them from the project's Snowflake forecasts. Predictions and bounds come from Snowflake ML. Public values are rounded to four decimal places, while KPI summaries use two decimal places.
