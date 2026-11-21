#!/usr/bin/env python3
"""Print sanitized runtime diagnostics, never the configured credentials."""
import json
import smoke
from resilience import sql

for port, path, headers in [
    (30083, '/internal/events?limit=2', {'X-Aggregator-Key':smoke.VALUES['AGGREGATOR_API_KEY']}),
    (30083, '/internal/storage-stats', {'X-Aggregator-Key':smoke.VALUES['AGGREGATOR_API_KEY']}),
    (30083, '/internal/source-status', {'X-Aggregator-Key':smoke.VALUES['AGGREGATOR_API_KEY']}),
    (30090, '/v1/observations?limit=5', {'Authorization':'Bearer '+smoke.VALUES['PANTAI_AUTH_TOKEN']}),
    (30083, '/internal/audit/unprocessed?source=SENSOR_PANTAI&limit=5', {'Authorization':'Bearer '+smoke.VALUES['P1_AUDIT_TOKEN']}),
]:
    status, data, elapsed = smoke.request(port, path, headers)
    print(json.dumps({'path':path,'status':status,'latency_ms':elapsed,'data':data}), flush=True)
print(sql("SELECT source,holder_id,fencing_token,lease_until,clock_timestamp() FROM source_leases"))
print(sql("SELECT source,count(*),count(*) FILTER (WHERE delivered_at IS NULL),max(last_error) FROM ingestion_outbox GROUP BY source"))
