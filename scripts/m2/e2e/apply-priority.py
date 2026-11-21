#!/usr/bin/env python3
"""Apply only the Client API reserve from .env, without restarting databases."""
import json
import subprocess
import smoke

reserve = int(smoke.VALUES['CLIENT_API_FIELD_RESERVED_SLOTS'])
maximum = int(smoke.VALUES['CLIENT_API_MAX_IN_FLIGHT'])
assert 0 < reserve < maximum
subprocess.run(['kubectl','-n',smoke.NAMESPACE,'patch','configmap/client-api-config','--type=merge','-p',
                json.dumps({'data':{'CLIENT_API_FIELD_RESERVED_SLOTS':str(reserve)}})],check=True)
subprocess.run(['kubectl','-n',smoke.NAMESPACE,'rollout','restart','deployment/client-api'],check=True)
subprocess.run(['kubectl','-n',smoke.NAMESPACE,'rollout','status','deployment/client-api','--timeout=180s'],check=True)
