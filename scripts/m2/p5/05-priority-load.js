import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';
import encoding from 'k6/encoding';

const fieldFailures = new Rate('field_failures');
const mediaRejected = new Rate('media_rejected');
const tokens = {};

export const options = {
  scenarios: {
    media: { executor: 'constant-vus', vus: Number(__ENV.MEDIA_VUS || 200), duration: __ENV.DURATION || '120s', exec: 'media' },
    field: { executor: 'constant-arrival-rate', rate: Number(__ENV.FIELD_RATE || 20), timeUnit: '1s', preAllocatedVUs: 50, maxVUs: 50, duration: __ENV.DURATION || '120s', exec: 'field' },
  },
  thresholds: { field_failures: ['rate<0.01'], media_rejected: ['rate>0'], dropped_iterations: ['count==0'] },
};

export function setup() {
  const initial = {};
  for (const prefix of ['media', 'field']) {
    accessToken(prefix);
    if (tokens[prefix.toUpperCase()]) initial[prefix.toUpperCase()] = tokens[prefix.toUpperCase()];
  }
  return initial;
}

function accessToken(prefix, initial = {}) {
  const kind = prefix.toUpperCase();
  if (__ENV[`${kind}_ACCESS_TOKEN`]) return __ENV[`${kind}_ACCESS_TOKEN`];
  if (!tokens[kind] && initial[kind]) tokens[kind] = initial[kind];
  if (tokens[kind] && Date.now() < tokens[kind].until) return tokens[kind].value;
  for (let attempt = 0; attempt < 10; attempt++) {
    const response = http.post(`${__ENV.AUTH_BASE_URL || 'http://127.0.0.1:8084'}/v1/token`, null, {
      headers: { Authorization: `Basic ${encoding.b64encode(`${__ENV[`${kind}_CLIENT_ID`]}:${__ENV[`${kind}_CLIENT_SECRET`]}`)}` },
      timeout: '5s',
    });
    if (response.status === 200) {
      const pair = response.json();
      tokens[kind] = { value: pair.access_token, until: Date.now() + Math.max(1, pair.expires_in - 10) * 1000 };
      return pair.access_token;
    }
    sleep(0.1 + Math.random() * 0.5);
  }
  throw new Error(`Unable to authenticate ${prefix} client`);
}

function request(prefix, initial) {
  const token = accessToken(prefix, initial);
  return http.get(`${__ENV.CLIENT_API_BASE_URL || 'http://127.0.0.1:8080'}/events?limit=10`, {
    headers: { Authorization: `Bearer ${token}`, 'X-Correlation-ID': `${prefix}-${__VU}-${__ITER}` },
    timeout: '5s',
  });
}

export function media(initial) {
  const response = request('media', initial);
  mediaRejected.add(response.status === 429);
  check(response, { 'media controlled': (r) => r.status === 200 || r.status === 429 });
}

export function field(initial) {
  const response = request('field', initial);
  fieldFailures.add(response.status !== 200);
  check(response, { 'field served': (r) => r.status === 200 });
}
