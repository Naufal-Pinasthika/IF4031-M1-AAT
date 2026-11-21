import http from 'k6/http';
import { sleep } from 'k6';
import encoding from 'k6/encoding';
import { Counter, Rate, Trend } from 'k6/metrics';

// New P5 scenarios, not a relaxation/replacement of the P2 10k test.
const priority = __ENV.MODE === 'priority';
const duration = __ENV.DURATION || '75s';
const bmkgLatency = new Trend('bmkg_complete_latency_ms', true);
const successLatency = new Trend('complete_latency_ms', true);
const sensorLatency = new Trend('sensor_read_complete_latency_ms', true);
const sensorFailures = new Rate('sensor_read_failures');
const statusCodes = new Counter('business_status');
const unexpected = new Rate('unexpected_errors');
const fieldFailures = new Rate('field_failures');
const mediaRejected = new Counter('media_429');
const invalidTiming = new Rate('invalid_timing');
const privacyFailures = new Rate('media_privacy_failures');
const complete = new Counter('complete_responses');
const authResponses = new Counter('auth_responses');
const sessions = {};

export const options = {
  scenarios: priority ? {
    media: { executor: 'constant-vus', vus: Number(__ENV.MEDIA_VUS || 300), duration, exec: 'media' },
    field: { executor: 'constant-arrival-rate', rate: 10, timeUnit: '1s', preAllocatedVUs: 20, maxVUs: 20, duration, exec: 'field' },
  } : {
    bmkg: { executor: 'constant-vus', vus: 25, duration, exec: 'bmkg' },
    pvmbg: { executor: 'constant-vus', vus: 25, duration, exec: 'pvmbg' },
    sensor: { executor: 'constant-arrival-rate', rate: 5, timeUnit: '1s', preAllocatedVUs: 5, maxVUs: 5, duration, exec: 'sensor' },
  },
  thresholds: {
    unexpected_errors: ['rate<0.01'], invalid_timing: ['rate==0'],
    media_privacy_failures: ['rate==0'],
    bmkg_complete_latency_ms: ['p(95)<300'],
    sensor_read_complete_latency_ms: ['p(95)<300'],
    sensor_read_failures: ['rate==0'],
    ...(priority ? { field_failures: ['rate==0'], media_429: ['count>0'], dropped_iterations: ['count==0'] }
      : { complete_responses: ['count>0'], dropped_iterations: ['count==0'] }),
  },
  summaryTrendStats: ['avg','min','med','p(50)','p(95)','p(99)','max'],
};

function credential(kind) {
  return `Basic ${encoding.b64encode(`${__ENV[`${kind}_CLIENT_ID`]}:${__ENV[`${kind}_CLIENT_SECRET`]}`)}`;
}

function access(kind) {
  if (sessions[kind] && Date.now() < sessions[kind].until) return sessions[kind].access_token;
  const old = sessions[kind];
  for (let i = 0; i < 10; i++) {
    const url = `${__ENV.AUTH_BASE_URL}/v1/${old ? 'token/refresh' : 'token'}`;
    const response = http.post(url, old ? JSON.stringify({refresh_token: old.refresh_token}) : null, {
      headers: { Authorization: credential(kind), 'Content-Type': 'application/json' }, timeout: '5s', tags: { name: 'auth-session' },
    });
    authResponses.add(1, { operation: old ? 'refresh' : 'issue', status: String(response.status) });
    if (response.status === 200) {
      const pair = response.json();
      sessions[kind] = {...pair, until: Date.now()+Math.max(1,pair.expires_in-10)*1000};
      return pair.access_token;
    }
    // Sessions are per VU: never race shared refresh-token rotation.
    if (response.status === 401 && old) break;
    sleep(0.1+Math.random()*0.3);
  }
  throw new Error(`Unable to obtain/refresh ${kind} session`);
}

function run(kind, source) {
  let token;
  try { token = access(kind); } catch (_) {
    unexpected.add(true);
    if (kind === 'FIELD') fieldFailures.add(true);
    return;
  }
  const path = `/events?limit=10${source ? '&source='+source : ''}`;
  const response = http.get(__ENV.CLIENT_API_BASE_URL+path, {
    headers: { Authorization: `Bearer ${token}`, 'X-Correlation-ID': `p5-${kind}-${__VU}-${__ITER}` },
    timeout: '5s', tags: { name: source || kind },
  });
  statusCodes.add(1, {client: kind, source: source || 'ALL', status: String(response.status)});
  let full = false, leaked = false;
  if (response.status === 200) {
    try {
      const body = response.json();
      full = body.partial === false && Array.isArray(body.events) && body.events.length > 0 &&
        body.events.every(x => !source || x.source === source);
      leaked = kind === 'MEDIA' && body.events.some(x => ['source_ref_id','latitude','longitude','attributes'].some(k => k in x));
    } catch (_) { /* a 200 malformed body is a failure, not successful service */ }
  }
  invalidTiming.add(response.timings.duration < 0);
  if (kind === 'MEDIA') privacyFailures.add(leaked);
  unexpected.add(response.status !== 429 && (!full || leaked));
  if (kind === 'FIELD' && priority) fieldFailures.add(!full || leaked);
  if (source === 'SENSOR_PANTAI') sensorFailures.add(!full || leaked);
  if (kind === 'MEDIA' && response.status === 429) mediaRejected.add(1);
  if (full && !leaked) {
    complete.add(1);
    successLatency.add(response.timings.duration);
    if (source === 'BMKG') bmkgLatency.add(response.timings.duration);
    if (source === 'SENSOR_PANTAI') sensorLatency.add(response.timings.duration);
  }
}

export function bmkg() { run('MEDIA', 'BMKG'); }
export function pvmbg() { run('FIELD', 'PVMBG'); }
export function sensor() { run('FIELD', 'SENSOR_PANTAI'); }
export function media() { run('MEDIA', ''); }
export function field() { run('FIELD', __ITER % 2 ? 'BMKG' : 'SENSOR_PANTAI'); }

export function handleSummary(data) {
  return { [__ENV.SUMMARY_PATH || 'p5-load-summary.json']: JSON.stringify(data,null,2) };
}
