import http from 'k6/http';
import { sleep } from 'k6';
import encoding from 'k6/encoding';
import { Counter, Rate, Trend } from 'k6/metrics';

const priority = __ENV.MODE === 'priority';
const errors = new Rate('unexpected_errors');
const fieldErrors = new Rate('field_errors');
const mediaRejected = new Counter('media_rejected');
const latency = new Trend('bmkg_success_ms');
const statuses = new Counter('business_status');
const sessions = {};
let diagnostics = 0;
const duration = __ENV.DURATION || '60s';

export const options = {
  scenarios: priority ? {
    media: { executor:'constant-vus',vus:Number(__ENV.MEDIA_VUS || 300),duration,exec:'media' },
    field: { executor:'constant-arrival-rate',rate:10,timeUnit:'1s',preAllocatedVUs:20,maxVUs:20,duration,exec:'field' },
  } : {
    bmkg: {executor:'constant-vus',vus:25,duration,exec:'bmkg'},
    pvmbg: {executor:'constant-vus',vus:25,duration,exec:'pvmbg'},
  },
  thresholds: {
    unexpected_errors:['rate<0.01'], dropped_iterations:['count==0'],
    ...(priority ? {field_errors:['rate==0'],media_rejected:['count>0']} : {bmkg_success_ms:['p(95)<300']}),
  },
  summaryTrendStats:['avg','min','med','p(95)','p(99)','max'],
};

function access(role) {
  if (sessions[role] && Date.now()<sessions[role].until) return sessions[role].access_token;
  const old = sessions[role];
  const correlation=`p5-${__ENV.ROUND}-${role}-auth-${__VU}-${__ITER}`;
  const credential = encoding.b64encode(`${__ENV[role+'_CLIENT_ID']}:${__ENV[role+'_CLIENT_SECRET']}`);
  const result = http.post(__ENV.AUTH_BASE_URL+'/v1/token'+(old?'/refresh':''),old?JSON.stringify({refresh_token:old.refresh_token}):null,
    {headers:{Authorization:'Basic '+credential,'Content-Type':'application/json','X-Correlation-ID':correlation},timeout:'5s',tags:{name:'auth',client:role}});
  if(role==='FIELD') console.log(JSON.stringify({kind:'field_auth',status:result.status,operation:old?'refresh':'issue',
    correlation_id:correlation,duration_ms:result.timings.duration}));
  if (result.status!==200) {
    if(role==='FIELD' || diagnostics++<2) console.error(JSON.stringify({kind:'auth_failure',role,status:result.status,
      operation:old?'refresh':'issue',duration_ms:result.timings.duration}));
    return null;
  }
  const pair=result.json();
  sessions[role]={...pair,until:Date.now()+Math.max(1,pair.expires_in-5)*1000};
  return pair.access_token;
}

function request(role,source) {
  const token=access(role);
  if (!token) { errors.add(true); if(role==='FIELD') fieldErrors.add(true); sleep(.1); return; }
  const result=http.get(__ENV.CLIENT_API_BASE_URL+'/events?limit=10'+(source?'&source='+source:''),{
    headers:{Authorization:'Bearer '+token,'X-Correlation-ID':`p5-${__ENV.ROUND}-${role}-${__VU}-${__ITER}`},
    timeout:'5s',tags:{name:source || role},
  });
  statuses.add(1,{client:role,source:source || 'ALL',status:String(result.status)});
  if(priority && role==='MEDIA' && result.status===429){mediaRejected.add(1);errors.add(false);return;}
  let valid=false;
  let diagnostic={};
  if(result.status!==200){
    try {
      const body=result.json();
      diagnostic={error:body.error};
    } catch (_) { diagnostic={transport_error:result.error}; }
  }
  if(result.status===200){
    try {
      const page=result.json();
      diagnostic={partial:page.partial,unavailable_units:page.unavailable_units,count:Array.isArray(page.events)?page.events.length:null};
      valid=page.partial===false && Array.isArray(page.events) && page.events.length>0 && page.events.every(x=>!source || x.source===source);
      if(role==='MEDIA') valid=valid && page.events.every(x=>!['source_ref_id','latitude','longitude','attributes'].some(k=>k in x));
    } catch (_) { valid=false; }
  }
  if(!valid && (role==='FIELD' || diagnostics++<2)) {
    console.error(JSON.stringify({kind:'business_failure',role,source,status:result.status,
      correlation_id:`p5-${__ENV.ROUND}-${role}-${__VU}-${__ITER}`,duration_ms:result.timings.duration,...diagnostic}));
  }
  errors.add(!valid);
  if(role==='FIELD') fieldErrors.add(!valid);
  if(valid && source==='BMKG') latency.add(result.timings.duration);
}

export function media(){request('MEDIA','');}
export function field(){request('FIELD',__ITER%2?'BMKG':'SENSOR_PANTAI');}
export function bmkg(){request('MEDIA','BMKG');}
export function pvmbg(){request('FIELD','PVMBG');}
export function handleSummary(data){return {[__ENV.SUMMARY_PATH]:JSON.stringify(data,null,2)};}
