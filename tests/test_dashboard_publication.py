import copy
import hashlib
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'web_dashboard'), str(ROOT / 'scripts'), str(ROOT / 'plugins')]
from validate_data import decode_json, validate_bundle
from build_dashboard_site import decode_bundle, SiteError, inputs_match
from weather_publication import check_receipt, publish_bundle, check_eligibility, PREDECESSORS, MUTATORS, ROOT_DAG, FORECAST_DAG, DBT_DAG

class MissingObject(Exception):
    response = {'Error': {'Code': 'NoSuchKey'}}

class S3:
    def __init__(self):
        self.objects = {}
    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise MissingObject()
        return {'Body': io.BytesIO(self.objects[Key]), 'ETag': 'old-etag'}
    def put_object(self, Bucket, Key, Body, **kwargs):
        self.objects[Key] = Body

class PublicationTests(unittest.TestCase):
    def test_real_snapshot_and_tampered_data(self):
        body = (ROOT / 'web_dashboard/snapshot/dashboard.json').read_bytes()
        checksum = (ROOT / 'web_dashboard/snapshot/dashboard.sha256').read_text().strip()
        bundle = decode_bundle(body, checksum)
        for mutate in (
            lambda b: b['datasets']['daily_weather'][0].update(password='private'),
            lambda b: b['datasets']['daily_weather'].pop(),
            lambda b: b['datasets']['kpis']['by_city'][0].update(mean_absolute_error=99),
            lambda b: b['datasets']['daily_weather'][0].update(city='unexpected'),
        ):
            changed = copy.deepcopy(bundle)
            mutate(changed)
            with self.assertRaises((ValueError, TypeError, KeyError)):
                validate_bundle(changed)
        with self.assertRaises(ValueError):
            validate_bundle(bundle, require_fresh=True)

    def test_eligibility_rejects_fake_success_and_overlapping_mutation(self):
        api = Mock()
        runs = {ROOT_DAG: 'root', FORECAST_DAG: 'forecast', DBT_DAG: 'dbt'}
        def run(dag, rid):
            return {'conf': {'weather_root_run_id': 'root', 'weather_forecast_run_id': 'forecast'} if dag == DBT_DAG else {'weather_root_run_id': 'root'}}
        rows = {dag: [dict(dag_id=dag, dag_run_id=runs[dag], task_id=name, state='success', try_number=1,
                         start_date='2026-10-01T00:00:00Z', end_date='2026-10-01T00:05:00Z')
                      for name in names] for dag, names in PREDECESSORS.items()}
        def receipt(dag, rid, name):
            return dict(dag_id=dag, run_id=rid, task_id=name, try_number=1,
                        started_at='2026-10-01T00:00:01Z', completed_at='2026-10-01T00:04:59Z',
                        source_captured_at='2026-10-01T00:00:00Z')
        api.run.side_effect = run
        api.tasks.side_effect = lambda dag, rid='~': rows[dag]
        api.receipt.side_effect = receipt
        first = check_eligibility(api, 'dbt')
        check_eligibility(api, 'dbt', expected=first)
        api.receipt.side_effect = lambda *args: {}
        with self.assertRaises(RuntimeError):
            check_eligibility(api, 'dbt')
        api.receipt.side_effect = receipt
        rows[ROOT_DAG].append(dict(dag_id=ROOT_DAG, dag_run_id='another-run', task_id='load', state='running', try_number=1,
                                  start_date='2026-10-01T00:01:00Z', end_date=None))
        with self.assertRaises(RuntimeError):
            check_eligibility(api, 'dbt')

    def test_invalid_checksum_stops_before_parsing(self):
        with self.assertRaises(SiteError):
            decode_bundle(b'{}', '0' * 64)
    def test_unexpected_fields_and_sample_data_rejected(self):
        with self.assertRaises(ValueError):
            validate_bundle({'sample': True})
    def test_duplicate_fields_and_nan_rejected(self):
        for body in (b'{"schema_version":1,"schema_version":1}', b'{"value":NaN}'):
            with self.assertRaises(ValueError):
                decode_json(body)
    def test_input_change_requires_new_assembly(self):
        selection = {'commit': 'a', 'pointer_sha256': 'one'}
        self.assertTrue(inputs_match(selection, 'a', 'one'))
        self.assertFalse(inputs_match(selection, 'b', 'one'))
        self.assertFalse(inputs_match(selection, 'a', 'two'))
    def test_receipt_requires_actual_attempt_and_contained_interval(self):
        task = dict(dag_id='weather_dbt_pipeline', dag_run_id='run', task_id='dbt_test', try_number=2,
                    start_date='2026-10-01T00:00:00Z', end_date='2026-10-01T00:05:00Z')
        receipt = dict(dag_id=task['dag_id'], run_id='run', task_id='dbt_test', try_number=2,
                       started_at='2026-10-01T00:00:01Z', completed_at='2026-10-01T00:04:59Z')
        check_receipt(receipt, task)
        for change in ({'try_number': 1}, {'started_at': '2026-09-30T23:59:59Z'}, {'run_id': 'old-run'}):
            with self.assertRaises(RuntimeError):
                check_receipt(receipt | change, task)
    def test_failed_guard_preserves_last_pointer(self):
        s3 = S3()
        old = dict(warehouse_completed_at='2026-10-01T00:00:00Z')
        s3.objects['dashboard/weather-v1/latest-success.json'] = json.dumps(old).encode()
        guard = Mock(side_effect=RuntimeError('overlapping warehouse write'))
        with self.assertRaises(RuntimeError):
            publish_bundle(s3, 'private', 'dashboard/weather-v1', b'validated-real-export',
                           dict(warehouse_completed_at='2026-10-02T00:00:00Z', exported_at='2026-10-02T00:01:00Z'), 'run', guard)
        self.assertEqual(json.loads(s3.objects['dashboard/weather-v1/latest-success.json']), old)
    def test_newer_pointer_cannot_be_replaced_by_older_build(self):
        s3 = S3()
        old = dict(warehouse_completed_at='2026-10-03T00:00:00Z')
        s3.objects['dashboard/weather-v1/latest-success.json'] = json.dumps(old).encode()
        guard = Mock()
        with self.assertRaises(RuntimeError):
            publish_bundle(s3, 'private', 'dashboard/weather-v1', b'validated-real-export',
                           dict(warehouse_completed_at='2026-10-02T00:00:00Z', exported_at='2026-10-02T00:01:00Z'), 'run', guard)
        guard.assert_not_called()
    def test_partial_or_synthetic_bundle_never_packages(self):
        for value in ({}, {'schema_version': 1, 'metadata': {}, 'datasets': {}}):
            body = json.dumps(value).encode()
            with self.assertRaises(SiteError):
                decode_bundle(body, hashlib.sha256(body).hexdigest())

if __name__ == '__main__':
    unittest.main()
