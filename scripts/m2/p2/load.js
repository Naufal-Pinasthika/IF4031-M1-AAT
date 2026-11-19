import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const unexpectedFailures = new Rate('unexpected_failures');
// Observation only: request, pacing and acceptance threshold stay unchanged.
const successfulResponses = new Counter('responses_2xx');
const controlledResponses = new Counter('responses_429');
const otherResponses = new Counter('responses_other');
const completeResponses = new Counter('responses_complete');
const partialResponses = new Counter('responses_partial');
const malformedResponses = new Counter('responses_malformed');
const vus = Number(__ENV.VUS || 10000);

export const options = {
  stages: [
    { duration: '30s', target: vus },
    { duration: __ENV.HOLD_DURATION || '120s', target: vus },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    unexpected_failures: ['rate<0.01'],
    http_req_duration: ['p(95)<2000'],
  },
};

export default function () {
  const base = __ENV.AGGREGATOR_BASE_URL || 'http://127.0.0.1:8083';
  const response = http.get(`${base}/internal/events?limit=20`, {
    headers: {
      'X-Aggregator-Key': __ENV.AGGREGATOR_API_KEY,
      'X-Correlation-ID': `p2-load-${__VU}-${__ITER}`,
    },
    timeout: '5s',
  });
  unexpectedFailures.add(!((response.status >= 200 && response.status < 300) || response.status === 429));
  if (response.status >= 200 && response.status < 300) {
    successfulResponses.add(1);
    try {
      const body = response.json();
      if (body && body.partial === false) completeResponses.add(1);
      else if (body && body.partial === true) partialResponses.add(1);
      else malformedResponses.add(1);
    } catch (_) {
      malformedResponses.add(1);
    }
  } else if (response.status === 429) {
    controlledResponses.add(1);
  } else {
    otherResponses.add(1);
  }
  check(response, { 'success or controlled rejection': (r) => (r.status >= 200 && r.status < 300) || r.status === 429 });
}
