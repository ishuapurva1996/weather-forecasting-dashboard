# Weather dashboard operations

The public weather dashboard uses the approved static frontend in `web_dashboard/`. Only allowlisted HTML/CSS/JavaScript and validated chart-ready weather JSON enter the Pages artifact. Private warehouse credentials, Airflow run identifiers, the S3 pointer, raw files, and dbt artifacts stay private.

## Current state

On October 8, 2026, the owner authorized moving weather into the active Snowflake account. Weather Airflow now uses `WEATHER_FORECASTING.ANALYTICS` there. Its connection is separate from movie Airflow's connection; a before/after fingerprint verified that the movie connection was unchanged. Only the weather database and schemas were created.

Fresh ingestion and Snowflake training/prediction succeeded. dbt seed, snapshot, run, and all tests passed, followed by the real `export_dashboard_bundle` task. The private immutable bytes and latest-success pointer were independently checked. The export was created at `2026-10-08T08:37:35.944572Z` from a warehouse build completed at `2026-10-08T08:31:02.876101Z`; source capture began at `2026-10-08T08:24:27.720633Z`. It contains 120 historical weather rows through October 7, 14 forecast rows for October 8–14, matching rolling/category coverage, and 14 initial forecast snapshot rows. The accuracy dataset is legitimately empty because no prediction in the new account yet has a later matching actual day. The old account's forecast revision archive was not reconstructed or fabricated.

The snapshot under `web_dashboard/snapshot/` contains the exact validated private bundle bytes and checksum, with original source/build/export timestamps preserved. Fresh-data desktop/mobile, city/theme, base-path, and empty-accuracy checks passed locally without console errors.

The dispatch task explicitly reports missing `DASHBOARD_GITHUB_TOKEN`. Automatic public refresh remains disabled while this weather-only token and the AWS Pages reader role are pending. No complete automatic publication chain or later scheduled run is claimed successful. The warehouse and validated export are available independently of that missing dispatch configuration.

Airflow initially reused a cached exporter module after a code edit. The weather service was reloaded with `AIRFLOW__CORE__EXECUTE_TASKS_NEW_PYTHON_INTERPRETER=true`; the real export task then succeeded. No upstream warehouse step was rerun during the export correction, and no task was manually marked successful. The receipt parser supports Airflow 2.10's string representation of XCom dictionaries using bounded literal parsing, never executable evaluation.

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

Airflow uses the existing weather `snowflake_conn`, pointing to `WEATHER_FORECASTING.ANALYTICS` in the active account. Set an explicit account, database, warehouse, and role in its extras; the weather dbt runner requires these values even when Snowflake can infer a default role. Configure the following in the checkout's ignored `.env`, then recreate the weather Airflow service so it receives changed values.

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
