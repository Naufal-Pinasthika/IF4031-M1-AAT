import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const servingBuilds = new Counter('responses_by_build');
const buildsSeen = new Set();

export const options = {
  vus: Number(__ENV.VUS || 50),
  duration: __ENV.DURATION || '120s',
  thresholds: {
    http_req_failed: ['rate==0'],
    checks: ['rate==1'],
  },
};

export default function () {
  const response = http.get(`${__ENV.AGGREGATOR_BASE_URL}/internal/events?limit=20`, {
    headers: {
      'X-Aggregator-Key': __ENV.AGGREGATOR_API_KEY,
      'X-Correlation-ID': `rolling-${__VU}-${__ITER}`,
    },
    timeout: '5s',
  });
  let complete = false;
  let partial = null;
  if (response.status === 200) {
    try {
      const body = response.json();
      partial = body.partial;
      complete = partial === false && Array.isArray(body.events) && body.events.every((e) => typeof e.hazard_id === 'string');
    } catch (_) { /* not a complete JSON response */ }
  }
  const build = response.headers['X-Aat-Build-Version'] || response.headers['X-AAT-Build-Version'] || 'unversioned';
  servingBuilds.add(1, {build});
  if (response.status === 200 && !buildsSeen.has(build)) {
    buildsSeen.add(build);
    console.info(JSON.stringify({kind: 'serving_build', build, vu: __VU}));
  }
  check(response, { 'complete HTTP 200 during rollout': () => complete });
  if (!complete) {
    console.warn(JSON.stringify({status: response.status, partial,
      correlation_id: `rolling-${__VU}-${__ITER}`, duration_ms: response.timings.duration,
      error: response.error || null}));
  }
  sleep(0.1);
}
