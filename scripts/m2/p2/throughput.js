import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const successfulRequests = new Counter('successful_requests');
const unexpectedFailures = new Rate('unexpected_failures');
const completeResponses = new Counter('responses_complete');
const partialResponses = new Counter('responses_partial');
const rejectedResponses = new Counter('responses_429');
const otherResponses = new Counter('responses_other');
const malformedResponses = new Counter('responses_malformed');
const degradedResponses = new Counter('responses_replication_degraded');

export const options = {
  scenarios: {
    read_capacity: {
      executor: 'constant-vus',
      vus: Number(__ENV.VUS || 1000),
      duration: __ENV.DURATION || '60s',
    },
  },
  thresholds: {
    unexpected_failures: ['rate<0.01'],
  },
};

export default function () {
  const base = __ENV.AGGREGATOR_BASE_URL || 'http://127.0.0.1:8083';
  const response = http.get(`${base}/internal/events?limit=20`, {
    headers: {
      'X-Aggregator-Key': __ENV.AGGREGATOR_API_KEY,
      'X-Correlation-ID': `p2-throughput-${__VU}-${__ITER}`,
    },
    timeout: '5s',
  });
  let complete = false;
  if (response.status === 200) {
    try {
      const body = response.json();
      const valid = body && Array.isArray(body.events) && body.events.every((e) => typeof e.hazard_id === 'string');
      complete = valid && body.partial === false;
      if (complete) completeResponses.add(1);
      else if (valid && body.partial === true) partialResponses.add(1);
      else malformedResponses.add(1);
      if (body && body.replication_degraded === true) degradedResponses.add(1);
    } catch (_) { malformedResponses.add(1); }
  } else if (response.status === 429) {
    rejectedResponses.add(1);
  } else {
    otherResponses.add(1);
  }
  successfulRequests.add(complete ? 1 : 0);
  unexpectedFailures.add(!complete && response.status !== 429);
  check(response, { 'complete non-partial response': () => complete });
}
