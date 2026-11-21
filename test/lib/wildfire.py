import datetime as dt
import math
import random


def key(d):
    return '|'.join(str(d[k]) for k in ('satellite', 'acq_date', 'acq_time', 'latitude', 'longitude'))


def detected(d):
    return dt.datetime.fromisoformat(d['acq_date']).replace(hour=d['acq_time'] // 100, minute=d['acq_time'] % 100)


def distance(a, b):
    lat1, lat2 = math.radians(a['latitude']), math.radians(b['latitude'])
    dl, dn = lat2 - lat1, math.radians(b['longitude'] - a['longitude'])
    return 12742.0176 * math.asin(math.sqrt(math.sin(dl / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dn / 2) ** 2))


def oracle(events):
    detections, active, late = {}, {}, set()
    watermark = dt.datetime(1970, 1, 1)
    for event in events:
        kind, body = event['type'], event['data']
        if kind == 'detection':
            identity = key(body)
            if identity in detections:
                continue
            item = dict(body)
            is_late = (watermark - detected(item)).total_seconds() > 21600
            watermark = max(watermark, detected(item))
            detections[identity] = item
            active[identity] = not is_late and item['confidence'] not in ('l', 'low')
            if is_late:
                late.add(identity)
        elif kind == 'update':
            old = body['key']
            item = {**detections.pop(old), **body['changes']}
            active.pop(old)
            identity = key(item)
            detections[identity] = item
            active[identity] = item['confidence'] not in ('l', 'low')
        elif kind == 'retract':
            active[body['key']] = False
    remaining = {k for k in detections if active[k]}
    groups, singles = [], 0
    while remaining:
        identity = min(remaining)
        remaining.remove(identity)
        component, pending = [identity], [identity]
        while pending:
            current = detections[pending.pop()]
            neighbors = [k for k in remaining if abs((detected(current) - detected(detections[k])).total_seconds()) <= 21600
                         and distance(current, detections[k]) <= 1]
            remaining.difference_update(neighbors)
            pending.extend(neighbors)
            component.extend(neighbors)
        if len(component) < 2:
            singles += 1
            continue
        items = [detections[k] for k in component]
        frp = round(sum(d['frp'] for d in items), 2)
        groups.append({'keys': sorted(component), 'count': len(items), 'frp': frp,
                       'lat': sum(d['latitude'] for d in items) / len(items),
                       'lon': sum(d['longitude'] for d in items) / len(items),
                       'time': min(detected(d) for d in items).isoformat() + 'Z',
                       'satellites': sorted({d['satellite'] for d in items}),
                       'confidence': max((d['confidence'] for d in items), key={'n': 2, 'h': 3}.get),
                       'severity': 'AWAS' if len(items) >= 20 or frp >= 100 else ('SIAGA' if len(items) >= 6 or frp >= 30 else 'WASPADA')})
    return sorted(groups, key=lambda g: g['keys']), len(late), singles, len(detections)


def fixture(seed, groups=80, members=20):
    rng = random.Random(seed)
    def detection(sat, lat, lon, hhmm=1200, frp=10, confidence='h', date='2026-11-21'):
        return {'satellite': sat, 'latitude': lat, 'longitude': lon, 'acq_date': date,
                'acq_time': hhmm, 'frp': frp, 'confidence': confidence}
    def emit(d):
        return {'type': 'detection', 'data': d}
    a = detection('N20', -2.0001, 110.0001, 1200, 20)
    b = detection('N21', -1.9941, 110.0001, 1100, 20)
    c = detection('N22', -1.9881, 110.0001, 1000, 20)
    e = detection('S1', -6.001, 105.001)
    f = detection('S2', -6.002, 105.002)
    low = detection('LOW', -7.001, 106.001, confidence='l')
    single = detection('SOLO', -7.0011, 106.0011)
    initial = [emit(d) for d in (a, b, c, e, f, low, single)] + [emit(a), {'type': 'heartbeat', 'data': {}}]
    changed = initial + [{'type': 'update', 'data': {'key': key(a), 'changes': {'frp': 0}}},
                         {'type': 'retract', 'data': {'key': key(f)}},
                         emit(detection('LATE', -1.993, 110.0001, date='2026-11-20'))]
    burst = []
    for group in range(groups):
        lat, lon = -5 + (group // 20) * .1, 114 + (group % 20) * .1
        for member in range(members):
            d = detection('S' + str(member % 3), round(lat + rng.uniform(-.0008, .0008), 6),
                          round(lon + rng.uniform(-.0008, .0008), 6), 1159 - (member % 5), rng.randint(0, 15),
                          'n' if member % 3 else 'h')
            burst.append(emit(d))
            if member == 2:
                burst.append(emit(d))
        burst.append({'type': 'heartbeat', 'data': {}})
    return initial, changed, changed + burst


def verify(h, events, actual, state):
    expected, late, singles, unique = oracle(events)
    by_keys = {tuple(e['attributes']['detection_keys']): e for e in actual}
    h.check(len(by_keys) == len(actual) == len(expected), 'Exactly one canonical wildfire per expected connected component')
    for group in expected:
        event = by_keys.get(tuple(group['keys']))
        h.check(event is not None, 'Expected component has all and only its unique detection keys')
        attrs = event['attributes']
        h.check(event['hazard_type'] == 'WILDFIRE' and event['source'] == 'SATELIT', 'Canonical wildfire source/type match')
        h.check(attrs['detection_count'] == group['count'] and attrs['satellites'] == group['satellites']
                and attrs['max_confidence'] == group['confidence'] and math.isclose(attrs['total_frp_mw'], group['frp'], abs_tol=1e-8),
                'Detection count, multi-satellite membership, confidence and FRP match the independent oracle')
        h.check(math.isclose(event['latitude'], group['lat'], abs_tol=1e-9)
                and math.isclose(event['longitude'], group['lon'], abs_tol=1e-9)
                and event['occurred_at'] == group['time'] and event['severity'] == group['severity'],
                'Centroid, satellite event time and severity match the independent oracle')
    h.check(state['late_detections'] == late and state['singleton_detections'] == singles,
            'Late detections are reported and low-confidence/singleton detections are not fires')
    h.check(h.sql('SELECT to_json(count(*)) FROM satellite_detections') == unique, 'No unique detection was lost or duplicated')
    return sorted([{k: v for k, v in event.items() if k != 'ingested_at'} for event in actual], key=lambda e: e['source_ref_id'])
