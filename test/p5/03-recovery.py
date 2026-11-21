from pathlib import Path
import copy
import datetime as dt
import json
import shlex
import subprocess
import sys
import time
import urllib.parse
import uuid
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'lib'))
from common import NS, parser, run
from sandbox import Sandbox


def scenario(h):
    h.changes()
    h.check(bool(h.hosts),'Notifier recovery scenario requires --vps')
    h.check(h.args.count>0 and h.args.count%2==0,'Message count is positive and even for the half-critical fixture')
    suffix=uuid.uuid4().hex[:10]
    name='aat-p5-'+suffix
    labels={'app':name,'aat-p5':suffix}
    original=json.loads(h.kube('get','statefulset/notifier-consumer','-o','json'))
    config=json.loads(h.kube('get','configmap/notifier-consumer-config','-o','json'))['data']
    config.update(KAFKA_TOPIC=name,KAFKA_CRITICAL_TOPIC='',NOTIFIER_CONSUMER_GROUP=name)
    spec=copy.deepcopy(original['spec'])
    spec['serviceName']=name
    spec['selector']={'matchLabels':{'app':name}}
    spec['template']['metadata']={'labels':labels}
    spec['template']['spec']['containers'][0]['envFrom']=[{'configMapRef':{'name':name}}]
    if h.args.notifier_image_tag:
        spec['template']['spec']['containers'][0]['image']='aat/notifier-consumer:'+h.args.notifier_image_tag
    for claim in spec['volumeClaimTemplates']:
        claim['metadata']={'name':claim['metadata']['name'],'labels':{'aat-p5':suffix}}
        claim.pop('status',None)
    def resource(kind,**fields):
        return {'apiVersion':'apps/v1' if kind=='StatefulSet' else 'v1','kind':kind,
                'metadata':{'name':name,'namespace':NS,'labels':labels},**fields}
    objects=[resource('ConfigMap',data=config),resource('StatefulSet',spec=spec),
             resource('Service',spec={'selector':{'app':name},'ports':[{'port':8086,'targetPort':8086}]})]
    lab=Sandbox(h)
    expected=[]
    topic=False
    try:
        h.kube('exec','kafka-0','--','env','KAFKA_HEAP_OPTS=-Xms32m -Xmx128m','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','localhost:9092','--create','--topic',name,'--partitions','3','--replication-factor','1')
        topic=True
        lab.apply(objects)
        h.kube('rollout','status','statefulset/'+name,'--timeout=180s')
        ip=h.kube('get','service',name,'-o','jsonpath={.spec.clusterIP}').strip()
        lab.open_tunnel({'p5notifier':ip+':8086'})
        for ordinal in range(h.args.repeat):
            now=dt.datetime.now(dt.timezone.utc).isoformat().replace('+00:00','Z')
            messages=[]
            refs=[]
            for i in range(h.args.count):
                identity=f'{name}-{ordinal}-{i}'
                refs.append(identity)
                messages.append({'event_type':'hazard.event.upserted','schema_version':1,'published_at':now,'correlation_id':identity,
                    'data':{'hazard_id':identity,'source':'SENSOR_PANTAI','source_ref_id':identity,'hazard_type':'COASTAL','severity':'AWAS' if i%2 else 'NORMAL',
                            'area_name':'controlled-e2e','occurred_at':now,'ingested_at':now,'attributes':{'critical':bool(i%2)}}})
            # A duplicate domain event at a different Kafka offset must not
            # create a second user notification, including after a hard crash.
            payload='\n'.join(json.dumps(m,separators=(',',':')) for m in messages+messages)+'\n'
            args=['kubectl','-n',NS,'exec','-i','kafka-0','--','env','KAFKA_HEAP_OPTS=-Xms32m -Xmx128m','/opt/kafka/bin/kafka-console-producer.sh','--bootstrap-server','localhost:9092',
                  '--topic',name,'--producer-property','acks=all']
            h.hosts.ssh(h.server,'sudo -n k3s '+shlex.join(args),payload.encode(),capture=True)
            pod=json.loads(h.kube('get','pod',name+'-0','-o','json'))
            h.check(pod['metadata']['labels'].get('aat-p5')==suffix,'Crash target belongs to this test run')
            status=pod['status']['containerStatuses'][0]
            before=status['restartCount']
            container=status['containerID'].split('://',1)[1]
            node=next(node for node in h.hosts.nodes if node['name']==pod['spec']['nodeName'])
            inspection=json.loads(h.hosts.ssh(node,'sudo -n k3s crictl inspect '+shlex.quote(container),capture=True))
            h.check(inspection['status']['labels'].get('io.kubernetes.pod.uid')==pod['metadata']['uid'],
                    'Host runtime container identity matches the run-owned Kubernetes pod')
            pid=int(inspection['info']['pid'])
            h.check(pid>1,'Fault target is a container process, never host PID 1')
            h.hosts.ssh(node,'sudo -n kill -KILL '+str(pid),capture=True)
            expected.extend(refs)
            started=time.monotonic()
            def recovered():
                pod=json.loads(h.kube('get','pod',name+'-0','-o','json'))
                status=pod['status'].get('containerStatuses',[])
                return status and status[0]['restartCount']>before and status[0]['ready']
            h.wait(recovered,'Run-owned notifier did not restart automatically')
            stopped=json.loads(h.kube('get','pod',name+'-0','-o','json'))['status']['containerStatuses'][0]['lastState'].get('terminated',{})
            h.check(stopped.get('exitCode')==137,'The injected failure was a real SIGKILL, not a graceful stop')
            def caught_up():
                code,stats,_=h.request('p5notifier','/stats')
                return stats if code==200 and stats['processed_count']==2*len(expected) else None
            stats=h.wait(caught_up,'Durable Kafka catch-up did not complete')
            h.check(stats['notification_count']==len(expected),'Domain duplicates do not create duplicate notifications after crash')
            h.check(stats['coastal_critical_count']==len(expected)//2,'Every generated critical notification survives restart')
            actual=[]
            cursor=''
            while True:
                code,page,_=h.request('p5notifier','/notifications?'+urllib.parse.urlencode({'limit':1000,**({'cursor':cursor} if cursor else {})}))
                h.check(code==200,'Notification page remains accessible after recovery')
                actual.extend(item['hazard_id'] for item in page['notifications'])
                cursor=page.get('next_cursor')
                if not cursor: break
            h.check(len(actual)==len(set(actual)) and set(actual)==set(expected),'Per-ID reconciliation proves zero missing/duplicate notifications')
            h.note({'round':ordinal+1,'accepted_records':2*len(refs),'unique_notifications':len(expected),'recovery_seconds':time.monotonic()-started,
                    'stats':stats,'scope':'Kafka-to-notifier durability only; not source-availability latency'})
    finally:
        if lab.tunnel:
            lab.tunnel.terminate()
            lab.tunnel.wait(timeout=10)
        h.kube('delete','statefulset,service,configmap','-l','aat-p5='+suffix,'--ignore-not-found=true','--wait=true','--timeout=90s')
        claims=json.loads(h.kube('get','pvc','-l','aat-p5='+suffix,'-o','json'))['items']
        for claim in claims:
            h.check(claim['metadata']['name']=='data-'+name+'-0','Only the exact run-owned notifier PVC may be removed')
            h.kube('delete','pvc',claim['metadata']['name'],'--wait=true','--timeout=90s')
        if topic:
            h.kube('exec','kafka-0','--','env','KAFKA_HEAP_OPTS=-Xms32m -Xmx128m','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','localhost:9092','--delete','--topic',name,'--if-exists')


if __name__=='__main__':
    p=parser('Reconcile notifier delivery, deduplication and automatic hard-crash recovery on a run-owned VPS topic/PVC')
    p.add_argument('--count',type=int,default=180)
    p.add_argument('--notifier-image-tag')
    p.set_defaults(timeout=120)
    raise SystemExit(run('p5/03-recovery',['P5.1','P5.6 (component)'],p,scenario))
