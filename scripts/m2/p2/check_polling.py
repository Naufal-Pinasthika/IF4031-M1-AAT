#!/usr/bin/env python3
"""Cross-check supplied upstream JSON logs with exported durable poll metadata.

This is a bounded evidence checker, NOT a proof of all P2 criteria/no data loss.
Unattributed business requests and incomplete evidence fail closed.
"""
import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import re

PATHS = {'BMKG': {'/seismic-events', '/tsunami-warnings'},
         'PVMBG': {'/volcanic-reports'},
         'SENSOR_PANTAI': {'/v1/stations', '/v1/observations'}}
REQUIRED_PATHS = {**PATHS, 'SENSOR_PANTAI': {'/v1/observations'}}
POLL = re.compile(r'^poll-(BMKG|PVMBG|SENSOR_PANTAI)-(\d+)$')


def check(ledger_pages, records, required=('BMKG', 'PVMBG')):
    errors = []
    ledger = {}
    for page in ledger_pages:
        for item in page['items']:
            identifier = item['poll_id']
            if identifier in ledger and any(ledger[identifier].get(k) != item.get(k)
                                           for k in ('source', 'holder_id', 'fencing_token')):
                errors.append('Conflicting metadata for poll '+str(identifier))
            ledger[identifier] = item
    counts = Counter()
    cycles = defaultdict(set)
    complete = defaultdict(set)
    for identifier, item in ledger.items():
        if item.get('status') == 'completed' and item.get('source') in PATHS:
            complete[item['source']].add(identifier)
    statuses = defaultdict(list)
    ignored = 0
    for record in records:
        if record.get('msg') != 'http_request':
            ignored += 1
            continue
        service = str(record.get('service', '')).upper()
        path = record.get('path')
        match = POLL.fullmatch(record.get('correlation_id', ''))
        if not match:
            if service in required and path in PATHS.get(service, set()):
                errors.append('Unattributed upstream business request: '+service+' '+path)
            else:
                ignored += 1
            continue
        source, identifier = match[1], int(match[2])
        if service != source or path not in PATHS[source]:
            errors.append('Unexpected service/path for poll '+str(identifier))
            continue
        item = ledger.get(identifier)
        if item is None or item.get('source') != source:
            errors.append('Missing/mismatched ledger for poll '+str(identifier))
            continue
        key = (source, identifier, path)
        counts[key] += 1
        statuses[key].append(record.get('status'))
        cycles[source].add(identifier)
    for (source, identifier, path), count in counts.items():
        if count > 1:
            errors.append(f'Duplicate HTTP: {source} poll={identifier} path={path} count={count}')
    for source, identifiers in complete.items():
        for identifier in identifiers:
            for path in REQUIRED_PATHS[source]:
                key = (source, identifier, path)
                if counts[key] != 1 or any(not isinstance(s, int) or not 200 <= s < 300 for s in statuses[key]):
                    errors.append(f'Incomplete successful upstream evidence: {source} poll={identifier} path={path}')
    for source in required:
        if not complete[source]:
            errors.append('No completed cycle with upstream evidence for '+source)
    return dict(passed=not errors, errors=errors[:100], total_errors=len(errors),
                required_sources=list(required), ignored_records=ignored,
                observed_cycles={s: len(ids) for s, ids in cycles.items()},
                completed_cycles={s: len(ids) for s, ids in complete.items()},
                upstream_requests=sum(counts.values()),
                scope='Only supplied logs and ledger; does not establish full coverage, no-loss, clock/freeze safety or all P2 criteria.')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--ledger', type=Path, nargs='+', required=True, help='Exports of /internal/polling or Ops /polling')
    p.add_argument('--upstream', type=Path, nargs='+', required=True, help='Raw JSONL HTTP logs from upstream, not Aggregator logs')
    p.add_argument('--require-source', action='append', choices=tuple(PATHS))
    p.add_argument('--output', type=Path)
    args = p.parse_args()
    pages = [json.loads(path.read_text(encoding='utf-8-sig')) for path in args.ledger]
    records = []
    non_json = 0
    for path in args.upstream:
        for line in path.read_text(encoding='utf-8-sig').splitlines():
            try:
                value = json.loads(line)
                if isinstance(value, dict):
                    records.append(value)
                else:
                    non_json += 1
            except ValueError:
                non_json += 1
    result = check(pages, records, args.require_source or ('BMKG', 'PVMBG'))
    result['non_json_lines'] = non_json
    if non_json:
        result['passed'] = False
        result['errors'].append('Input contains non-JSON lines; export raw unprefixed upstream logs')
        result['total_errors'] += 1
    content = json.dumps(result, indent=2)
    print(content)
    if args.output:
        args.output.write_text(content, encoding='utf-8')
    raise SystemExit(0 if result['passed'] else 1)


if __name__ == '__main__':
    main()
