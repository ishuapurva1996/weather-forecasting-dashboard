"""Success-only weather pipeline evidence and immutable private export handoff."""
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from urllib.parse import quote

import requests

ROOT_DAG = 'WeatherData_multiple_cities_data'
FORECAST_DAG = 'forecast_model_temp_max'
DBT_DAG = 'weather_dbt_pipeline'
# Parent trigger tasks wait for this dbt DAG and are legitimately still running.
# Eligibility requires the completed data producers, not those waiting parents.
PREDECESSORS = {
    ROOT_DAG: ('extract_past_60_days_weather_city', 'extract_past_60_days_weather_city__1',
               'transform_past_60_days_weather_city', 'transform_past_60_days_weather_city__1',
               'combine_rec_of_2_cities', 'load'),
    FORECAST_DAG: ('train_model', 'predict'),
    DBT_DAG: ('dbt_seed', 'dbt_snapshot', 'dbt_run', 'dbt_test'),
}
MUTATORS = {ROOT_DAG: {'load'}, FORECAST_DAG: {'train_model', 'predict'},
            DBT_DAG: {'dbt_seed', 'dbt_snapshot', 'dbt_run'}}
ACTIVE = {'running', 'queued', 'scheduled', 'up_for_retry', 'restarting', 'deferred'}


def required(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f'Missing private configuration: {name}.')
    return value


def now():
    return datetime.now(timezone.utc).isoformat()


def timestamp(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise RuntimeError('Missing timezone in pipeline evidence.')
    return result


def success_receipt(started_at, **extra):
    from airflow.operators.python import get_current_context
    ti = get_current_context()['ti']
    return dict(dag_id=ti.dag_id, run_id=ti.run_id, task_id=ti.task_id,
                try_number=ti.try_number, started_at=started_at, completed_at=now(), **extra)


def run_dbt(command):
    """Emit a receipt only when the real dbt subprocess has exited successfully."""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    connection = SnowflakeHook(snowflake_conn_id='snowflake_conn').get_connection('snowflake_conn')
    extra = connection.extra_dejson
    env = os.environ.copy()
    env.update(DBT_USER=connection.login, DBT_PASSWORD=connection.password,
               DBT_ACCOUNT=extra['account'], DBT_SCHEMA=connection.schema or 'ANALYTICS',
               DBT_ROLE=extra['role'], DBT_DATABASE=extra['database'],
               DBT_WAREHOUSE=extra['warehouse'], DBT_TYPE='snowflake')
    started = now()
    subprocess.run(['/opt/dbt_venv/bin/dbt', command, '--project-dir', '/opt/airflow/dbt',
                    '--profiles-dir', '/opt/airflow/dbt'], env=env, check=True, timeout=600)
    return success_receipt(started)


class MetadataAPI:
    def __init__(self, base=None, auth=None):
        self.base = (base or required('DASHBOARD_AIRFLOW_API_URL')).rstrip('/') + '/api/v1'
        self.auth = auth or (required('DASHBOARD_AIRFLOW_USERNAME'), required('DASHBOARD_AIRFLOW_PASSWORD'))

    def get(self, path, params=None):
        response = requests.get(self.base + path, auth=self.auth, params=params, timeout=(10, 30))
        if response.status_code != 200:
            raise RuntimeError(f'Cannot verify weather pipeline metadata (HTTP {response.status_code}).')
        return response.json()

    def tasks(self, dag, run='~'):
        path = f'/dags/{dag}/dagRuns/{quote(run, safe="")}/taskInstances'
        rows = []
        for offset in range(0, 10000, 100):
            page = self.get(path, {'limit': 100, 'offset': offset})
            rows.extend(page['task_instances'])
            if len(rows) >= page['total_entries']:
                return rows
        raise RuntimeError('Weather metadata exceeds the scan limit; retain publication evidence and review cleanup.')

    def run(self, dag, run):
        return self.get(f'/dags/{dag}/dagRuns/{quote(run, safe="")}')

    def receipt(self, dag, run, task):
        result = self.get(f'/dags/{dag}/dagRuns/{quote(run, safe="")}/taskInstances/{task}/xcomEntries/return_value')['value']
        return json.loads(result) if isinstance(result, str) else result


def check_receipt(receipt, task):
    for field in ('dag_id', 'task_id', 'try_number'):
        if receipt.get(field) != task[field]:
            raise RuntimeError('Success receipt does not match this task attempt; execute a fresh full pipeline.')
    if receipt.get('run_id') != task['dag_run_id']:
        raise RuntimeError('Success receipt belongs to another pipeline run.')
    if not timestamp(task['start_date']) <= timestamp(receipt['started_at']) <= timestamp(receipt['completed_at']) <= timestamp(task['end_date']):
        raise RuntimeError('Success receipt is outside the real successful task interval.')


def check_eligibility(api, dbt_run, expected=None):
    conf = api.run(DBT_DAG, dbt_run)['conf']
    if set(conf) != {'weather_root_run_id', 'weather_forecast_run_id'}:
        raise RuntimeError('Dashboard publication requires a complete chained weather run.')
    runs = {ROOT_DAG: conf['weather_root_run_id'], FORECAST_DAG: conf['weather_forecast_run_id'], DBT_DAG: dbt_run}
    forecast_conf = api.run(FORECAST_DAG, runs[FORECAST_DAG])['conf']
    if forecast_conf != {'weather_root_run_id': runs[ROOT_DAG]}:
        raise RuntimeError('Weather pipeline lineage changed.')
    receipts, evidence = {}, []
    for dag, run in runs.items():
        rows = {r['task_id']: r for r in api.tasks(dag, run)}
        for name in PREDECESSORS[dag]:
            task = rows.get(name)
            if not task or task['state'] != 'success' or not task.get('start_date') or not task.get('end_date'):
                raise RuntimeError('A required weather predecessor did not complete successfully.')
            evidence.append({k: task[k] for k in ('dag_id', 'dag_run_id', 'task_id', 'state', 'try_number', 'start_date', 'end_date')})
            if name in MUTATORS[dag] or (dag == DBT_DAG and name == 'dbt_test'):
                receipt = api.receipt(dag, run, name)
                check_receipt(receipt, task)
                receipts[(dag, name)] = receipt
    root = receipts[(ROOT_DAG, 'load')]
    cutoff = timestamp(root['started_at'])
    # Reject another run that is writing now, overlapped this build, or wrote later.
    # Scan all three DAGs, because models are replaced by separate tasks/DAGs.
    for dag, run in runs.items():
        for task in api.tasks(dag):
            if task['task_id'] not in MUTATORS[dag] or task['dag_run_id'] == run:
                continue
            if task['state'] in ACTIVE or (task.get('end_date') and timestamp(task['end_date']) >= cutoff):
                raise RuntimeError('Another weather run may have changed the warehouse; execute a fresh complete pipeline.')
    result = {
        'fingerprint': hashlib.sha256(json.dumps(evidence, sort_keys=True).encode()).hexdigest(),
        'source_captured_at': root['source_captured_at'],
        'warehouse_completed_at': receipts[(DBT_DAG, 'dbt_test')]['completed_at'],
    }
    if expected is not None and expected != result:
        raise RuntimeError('Weather task attempts changed during export; no new pointer was published.')
    return result


def prefix(value):
    if not re.fullmatch(r'dashboard/[A-Za-z0-9/_-]+', value) or '..' in value or value.endswith('/'):
        raise RuntimeError('Use a dedicated private dashboard/<name> prefix.')
    return value


def read_object(s3, bucket, key, limit):
    result = s3.get_object(Bucket=bucket, Key=key)
    stream = result['Body']
    try:
        body = stream.read(limit + 1)
    finally:
        stream.close()
    if len(body) > limit:
        raise RuntimeError('Private export object exceeds its size limit.')
    return body, result.get('ETag')


def publish_bundle(s3, bucket, path, body, metadata, run_id, guard):
    path = prefix(path)
    digest = hashlib.sha256(body).hexdigest()
    key = f'{path}/bundles/{digest}.json'
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType='application/json', IfNoneMatch='*')
    except Exception as exc:
        if getattr(exc, 'response', {}).get('Error', {}).get('Code') not in {'PreconditionFailed', '412'}:
            raise
    if read_object(s3, bucket, key, 2 * 1024 * 1024)[0] != body:
        raise RuntimeError('Stored immutable bundle failed byte verification.')
    pointer_key = path + '/latest-success.json'
    try:
        old_body, etag = read_object(s3, bucket, pointer_key, 8192)
        old = json.loads(old_body)
        if timestamp(old['warehouse_completed_at']) > timestamp(metadata['warehouse_completed_at']):
            raise RuntimeError('A newer successful weather export already exists.')
        if not etag:
            raise RuntimeError('No conditional pointer identity available.')
        condition = {'IfMatch': etag}
    except Exception as exc:
        if getattr(exc, 'response', {}).get('Error', {}).get('Code') not in {'NoSuchKey', '404'}:
            raise
        condition = {'IfNoneMatch': '*'}
    pointer = dict(schema_version=1, sha256=digest, bundle_key=key, run_id=run_id,
                   warehouse_completed_at=metadata['warehouse_completed_at'], exported_at=metadata['exported_at'])
    guard()
    pointer_bytes = json.dumps(pointer, sort_keys=True).encode()
    try:
        s3.put_object(Bucket=bucket, Key=pointer_key, Body=pointer_bytes, ContentType='application/json', **condition)
    except Exception:
        if read_object(s3, bucket, pointer_key, 8192)[0] != pointer_bytes:
            raise RuntimeError('Pointer update was not confirmed; inspect its identity before retrying.') from None
    return {'sha256': digest}


def export_and_publish(**context):
    import boto3
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    sys.path.insert(0, os.environ.get('DASHBOARD_SOURCE_DIR', '/opt/airflow/web_dashboard'))
    from export_data import extract_bundle
    from validate_data import encode_bundle
    api = MetadataAPI()
    run = context['run_id']
    evidence = check_eligibility(api, run)
    guard = lambda: check_eligibility(api, run, expected=evidence)
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    connection_info = hook.get_connection('snowflake_conn')
    os.environ['SNOWFLAKE_DATABASE'] = connection_info.extra_dejson['database']
    os.environ['SNOWFLAKE_SCHEMA'] = connection_info.schema or 'ANALYTICS'
    conn = hook.get_conn()
    try:
        conn.cursor().execute('ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 120')
        bundle = extract_bundle(conn, evidence['source_captured_at'], evidence['warehouse_completed_at'])
        body, _ = encode_bundle(bundle)
        guard()
        return publish_bundle(boto3.client('s3'), required('DASHBOARD_S3_BUCKET'), required('DASHBOARD_S3_PREFIX'),
                              body, bundle['metadata'], run, guard)
    finally:
        conn.close()


def dispatch_dashboard(**context):
    publication = context['ti'].xcom_pull(task_ids='export_dashboard_bundle')
    if not publication or not re.fullmatch(r'[0-9a-f]{64}', publication.get('sha256', '')):
        raise RuntimeError('A verified private export is required before dispatch.')
    response = requests.post('https://api.github.com/repos/ishuapurva1996/weather-forecasting-dashboard/actions/workflows/deploy-dashboard.yml/dispatches',
        headers={'Authorization': 'Bearer ' + required('DASHBOARD_GITHUB_TOKEN'),
                 'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28'},
        json={'ref': 'main'}, timeout=(10, 30))
    if response.status_code != 204:
        raise RuntimeError(f'Weather Pages dispatch rejected (HTTP {response.status_code}); check token scope and expiry.')
    return {'sha256': publication['sha256'], 'submitted': True}
