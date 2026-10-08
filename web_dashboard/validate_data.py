"""Strict public weather contract shared by export, handoff, and Pages assembly."""
import hashlib
import json
import math
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

MAX_BUNDLE_BYTES = 2 * 1024 * 1024
CITIES = {'San Jose', 'Los Angeles'}
FIELDS = {
    'daily_weather': 'weather_date city actual_temp_max actual_temp_min actual_temp_mean weather_code forecast_temp_max forecast_lower_bound forecast_upper_bound record_type',
    'forecast_accuracy': 'forecast_made_on forecast_for_date city predicted_temp_max predicted_lower predicted_upper actual_temp_max error absolute_error days_ahead actual_in_interval',
    'forecast_revisions': 'forecast_made_on forecast_for_date city predicted_temp_max predicted_lower predicted_upper valid_to',
    'weather_categories': 'weather_date city weather_code weather_description weather_category severity_score actual_temp_max actual_temp_min actual_temp_mean',
    'rolling_weather': 'city weather_date temp_max temp_min temp_mean temp_mean_7day_avg temp_max_7day temp_min_7day days_in_window',
}
LIMITS = {'daily_weather': 134, 'forecast_accuracy': 4000, 'forecast_revisions': 4000,
          'weather_categories': 120, 'rolling_weather': 120}
KPI_FIELDS = set('city latest_forecast_date latest_forecast_temp_max latest_actual_date latest_actual_temp_max mean_absolute_error interval_hit_rate_pct'.split())
META_FIELDS = set('source source_captured_at warehouse_completed_at exported_at latest_actual_date expected_refresh_hours'.split())


def timestamp(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('A provenance timestamp must include its timezone.')
    return result


def decode_json(body):
    if len(body) > MAX_BUNDLE_BYTES:
        raise ValueError('Weather export exceeds 2 MiB.')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('Duplicate JSON field.')
            result[key] = value
        return result
    def invalid(_):
        raise ValueError('Non-finite JSON number.')
    return json.loads(body, object_pairs_hook=pairs, parse_constant=invalid)


def validate_bundle(bundle, *, require_fresh=False):
    if set(bundle) != {'schema_version', 'metadata', 'datasets'} or bundle['schema_version'] != 1:
        raise ValueError('Unsupported public weather bundle.')
    metadata, datasets = bundle['metadata'], bundle['datasets']
    if set(metadata) != META_FIELDS or metadata['source'] != 'Open-Meteo / Snowflake ML / dbt':
        raise ValueError('Unexpected provenance fields or synthetic source.')
    if metadata['expected_refresh_hours'] != 24:
        raise ValueError('Expected daily refresh cadence.')
    if require_fresh and any(metadata[k] is None for k in ('source_captured_at', 'warehouse_completed_at')):
        raise ValueError('Automatic publication requires known capture and warehouse completion times.')
    times = [timestamp(metadata[k]) for k in ('source_captured_at', 'warehouse_completed_at', 'exported_at') if metadata[k] is not None]
    if times != sorted(times) or times[-1] > datetime.now(timezone.utc):
        raise ValueError('Invalid source/build/export timestamp order.')
    if set(datasets) != set(FIELDS) | {'kpis'}:
        raise ValueError('Unexpected or missing weather dataset.')
    for name, fields in FIELDS.items():
        rows = datasets[name]
        if not isinstance(rows, list) or len(rows) > LIMITS[name]:
            raise ValueError(f'Invalid row count for {name}.')
        for row in rows:
            if set(row) != set(fields.split()) or row['city'] not in CITIES:
                raise ValueError(f'Unexpected public fields or city in {name}.')
            for key, value in row.items():
                if value is None:
                    continue
                if key.endswith('_date') or key in {'forecast_made_on', 'valid_to'}:
                    if date.fromisoformat(value).isoformat() != value:
                        raise ValueError(f'Invalid date in {name}.')
                elif key in {'city', 'record_type', 'weather_description', 'weather_category'}:
                    if not isinstance(value, str) or len(value) > 120:
                        raise ValueError('Invalid public weather label.')
                elif isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                    raise ValueError(f'Invalid numeric field {key}.')
                elif key == 'weather_code' and (int(value) != value or not 0 <= value <= 99):
                    raise ValueError('Invalid WMO weather code.')
                elif key == 'actual_in_interval' and value not in {0, 1}:
                    raise ValueError('Invalid prediction interval flag.')
                elif key == 'days_in_window' and not 1 <= value <= 7:
                    raise ValueError('Invalid rolling window size.')
                elif key == 'days_ahead' and not 1 <= value <= 7:
                    raise ValueError('Invalid forecast horizon.')
                elif key == 'severity_score' and not 0 <= value <= 10:
                    raise ValueError('Invalid weather severity.')
                elif key == 'absolute_error' and value < 0:
                    raise ValueError('Absolute forecast error cannot be negative.')
                elif key not in {'weather_code', 'actual_in_interval', 'days_in_window', 'days_ahead', 'severity_score'} and not -150 <= value <= 150:
                    raise ValueError('Weather value is outside the public contract range.')
            for lower, point, upper in [('forecast_lower_bound', 'forecast_temp_max', 'forecast_upper_bound'),
                                        ('predicted_lower', 'predicted_temp_max', 'predicted_upper')]:
                if row.get(point) is not None and not row[lower] <= row[point] <= row[upper]:
                    raise ValueError('Forecast lies outside its prediction interval.')
            if name == 'forecast_accuracy':
                error = row['actual_temp_max'] - row['predicted_temp_max']
                if abs(error - row['error']) > 0.001 or abs(abs(error) - row['absolute_error']) > 0.001:
                    raise ValueError('Forecast error disagrees with the actual and prediction.')
                horizon = (date.fromisoformat(row['forecast_for_date']) - date.fromisoformat(row['forecast_made_on'])).days
                hit = int(row['predicted_lower'] <= row['actual_temp_max'] <= row['predicted_upper'])
                if horizon != row['days_ahead'] or hit != row['actual_in_interval']:
                    raise ValueError('Forecast horizon or interval flag is inconsistent.')
    daily = datasets['daily_weather']
    seen = set()
    latest_dates = []
    for city in CITIES:
        history = [r for r in daily if r['city'] == city and r['record_type'] == 'history']
        forecast = [r for r in daily if r['city'] == city and r['record_type'] == 'forecast']
        if not 50 <= len(history) <= 60 or len(forecast) != 7:
            raise ValueError('Each city requires recent weather history and seven forecast days.')
        last = max(r['weather_date'] for r in history)
        latest_dates.append(last)
        expected_dates = {(date.fromisoformat(last) + timedelta(days=i)).isoformat() for i in range(1, 8)}
        if {r['weather_date'] for r in forecast} != expected_dates:
            raise ValueError('Forecast must cover the seven days following actual weather history.')
        history_keys = {(r['city'], r['weather_date']) for r in history}
        for name in ('weather_categories', 'rolling_weather'):
            keys = [(r['city'], r['weather_date']) for r in datasets[name] if r['city'] == city]
            if len(keys) != len(set(keys)) or set(keys) != history_keys:
                raise ValueError('Required chart coverage does not match weather history.')
        if require_fresh:
            today = datetime.now(ZoneInfo('America/Los_Angeles')).date()
            if not 0 <= (today - date.fromisoformat(last)).days <= 3:
                raise ValueError('Actual weather is stale; run a complete upstream pipeline.')
    for row in daily:
        key = (row['city'], row['weather_date'], row['record_type'])
        if key in seen or row['record_type'] not in {'history', 'forecast'}:
            raise ValueError('Duplicate or invalid daily weather record.')
        seen.add(key)
        required = ['actual_temp_max', 'actual_temp_min', 'actual_temp_mean', 'weather_code'] if row['record_type'] == 'history' else ['forecast_temp_max', 'forecast_lower_bound', 'forecast_upper_bound']
        if any(row[k] is None for k in required):
            raise ValueError('Incomplete daily weather.')
    if len(set(latest_dates)) != 1 or metadata['latest_actual_date'] != latest_dates[0]:
        raise ValueError('City data or provenance disagree about the latest actual date.')
    kpis = datasets['kpis']
    if set(kpis) != {'generated_at', 'by_city'} or kpis['generated_at'] != metadata['exported_at']:
        raise ValueError('KPI export timestamp does not match the bundle.')
    if len(kpis['by_city']) != 2 or {r['city'] for r in kpis['by_city']} != CITIES:
        raise ValueError('Missing city KPI coverage.')
    # Recompute KPIs so edited summaries cannot disagree with chart rows.
    from export_data import build_kpis
    expected = build_kpis(daily, datasets['forecast_accuracy'], generated_at=metadata['exported_at'])
    if kpis != expected or any(set(r) != KPI_FIELDS for r in kpis['by_city']):
        raise ValueError('KPI values do not match the exported chart data.')
    return metadata


def encode_bundle(bundle):
    validate_bundle(bundle, require_fresh=True)
    body = (json.dumps(bundle, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()
    if len(body) > MAX_BUNDLE_BYTES:
        raise ValueError('Weather export exceeds 2 MiB.')
    return body, hashlib.sha256(body).hexdigest()
