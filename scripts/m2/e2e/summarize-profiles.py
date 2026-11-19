#!/usr/bin/env python3
"""Summarize generator sizing pilots, never turn them into a full P2 pass."""
import datetime
import json
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parents[3]
rows = []
for vus in (100, 500, 1000):
    prefix = ROOT / f'.e2e/direct-profile-{vus}'
    resources_path = Path(str(prefix) + '-generator-resources.json')
    summary_path = Path(str(prefix) + '-summary.json')
    if not resources_path.exists():
        continue
    resources = json.loads(resources_path.read_text())
    summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}
    metrics = summary.get('metrics', {})
    hold = [x for x in resources.get('resource_samples', []) if 30 <= x['seconds'] < 150]
    rows.append(dict(
        vus=vus, ran_at=resources['ran_at'], script_thresholds_passed=resources['passed'],
        actual_max_vus=metrics.get('vus', {}).get('max'),
        elapsed_seconds=resources['elapsed_seconds'],
        peak_rss_mib=round(resources['peak_rss_mib'], 2),
        hold_median_rss_mib=round(statistics.median(x['rss_mib'] for x in hold), 2) if hold else None,
        hold_median_cpu_cores=round(statistics.median(x['cpu_percent_of_one_core'] for x in hold)/100, 3) if hold else None,
        peak_cpu_cores=round(resources.get('peak_cpu_percent_of_one_core', 0)/100, 3),
        generator_gomaxprocs=resources.get('generator_gomaxprocs'),
        request_rps=metrics.get('http_reqs', {}).get('rate'),
        unexpected_error_rate=metrics.get('unexpected_failures', {}).get('value'),
        http_error_rate_including_controlled=metrics.get('http_req_failed', {}).get('value'),
        response_p95_ms=metrics.get('http_req_duration', {}).get('p(95)'),
        received_mbps=(metrics['data_received']['rate']*8/1_000_000) if 'data_received' in metrics else None,
        lowest_windows_physical_mib=resources.get('lowest_windows_physical_available_mib'),
        error=resources.get('error'), safety_stop_reason=resources['safety_stop_reason'],
    ))
result = dict(ran_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), pilots=rows,
              scope='Generator resource profiling only. Same load.js requests and thresholds, 30s ramp + 120s hold + 30s ramp-down. No scaling or full P2/P5 compliance proof.',
              limitations=['Generator shares the laptop/WSL CPU and network with the target cluster.',
                           'RPS includes controlled rejections; HTTP 200 completeness is not checked by this script.',
                           'GOMAXPROCS=2 constrains the generator; these are not dedicated-VPS CPU capacity measurements.',
                           'RSS excludes operating-system memory, kernel socket buffers and VPN overhead.'])
complete = [x for x in rows if x['actual_max_vus'] == x['vus'] and x['elapsed_seconds'] >= 180 and not x['safety_stop_reason']]
if complete:
    largest = max(complete, key=lambda x: x['vus'])
    result['rough_10000_vu_generator_rss_mib'] = round(largest['peak_rss_mib']*10000/largest['vus'], 2)
    result['projection_basis_vus'] = largest['vus']
    result['projection_warning'] = 'Simple linear extrapolation, not a measured requirement or guaranteed upper bound. Do not extrapolate RPS/latency or treat script failure as an application pass.'
destination = ROOT / '.e2e/k6-sizing-results.json'
destination.write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2))
