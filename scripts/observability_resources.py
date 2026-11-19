"""Private Prometheus + read-only operations UI, bounded by Appendix B."""
def obj(kind, name, spec=None, **extra):
    result = {'apiVersion': 'apps/v1' if kind in ('StatefulSet', 'Deployment') else 'v1',
              'kind': kind, 'metadata': {'name': name}}
    if spec is not None:
        result['spec'] = spec
    return dict(result, **extra)


def resources(prefix,tag):
    ns='sistem-informasi-kebencanaan-bnpb'
    result=[obj('ServiceAccount','observability')]
    result += [dict(apiVersion='rbac.authorization.k8s.io/v1',kind='Role',metadata={'name':'observability-read'},
                    rules=[{'apiGroups':[''],'resources':['pods','pods/log'],'verbs':['get','list','watch']}]),
               dict(apiVersion='rbac.authorization.k8s.io/v1',kind='RoleBinding',metadata={'name':'observability-read'},
                    roleRef={'apiGroup':'rbac.authorization.k8s.io','kind':'Role','name':'observability-read'},
                    subjects=[{'kind':'ServiceAccount','name':'observability','namespace':ns}])]
    # Prometheus discovery is namespace-scoped. No host filesystem mounts or
    # permissions to mutate workloads, read Secrets, or bypass storage ownership.
    config='''global:
  scrape_interval: 5s
  evaluation_interval: 5s
scrape_configs:
  - job_name: applications
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names: [sistem-informasi-kebencanaan-bnpb]
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app]
        regex: aggregator|aggregator-gateway|auth|client-api|bmkg|pvmbg|dashboard-consumer|notifier-consumer
        action: keep
      - source_labels: [__meta_kubernetes_pod_container_port_name]
        regex: http
        action: keep
      - source_labels: [__meta_kubernetes_pod_label_app]
        target_label: app
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: pod
      - source_labels: [__meta_kubernetes_pod_node_name]
        target_label: node
'''
    result.append(obj('ConfigMap','prometheus-config',data={'prometheus.yml':config}))
    for name,image,port,extra in (
        ('prometheus','prom/prometheus:v3.2.1',9090,{
            'args':['--config.file=/etc/prometheus/prometheus.yml','--storage.tsdb.path=/prometheus',
                    '--storage.tsdb.retention.time=6h','--storage.tsdb.retention.size=256MB','--query.max-concurrency=4',
                    '--query.max-samples=100000'],
            'volumeMounts':[{'name':'config','mountPath':'/etc/prometheus','readOnly':True},{'name':'data','mountPath':'/prometheus'}]}),
        ('observability',prefix+'/observability:'+tag,8090,{}),
    ):
        result.append(obj('Service',name,{'selector':{'app':name},'ports':[{'name':'http','port':port,'targetPort':port}]}))
        result.append(obj('Deployment',name,{'replicas':1,'selector':{'matchLabels':{'app':name}},
            'template':{'metadata':{'labels':{'app':name,'role':'other'}},'spec':{
                'serviceAccountName':'observability','securityContext':{'runAsNonRoot':True,'runAsUser':1000,'fsGroup':1000},
                'containers':[dict(name=name,image=image,ports=[{'name':'http','containerPort':port}],
                    resources={'requests':{'memory':'128Mi'},'limits':{'memory':'256Mi'}},
                    readinessProbe={'httpGet':{'path':'/-/ready' if name=='prometheus' else '/health','port':port},'periodSeconds':5},**extra)],
                **({'volumes':[{'name':'config','configMap':{'name':'prometheus-config'}},{'name':'data','emptyDir':{'sizeLimit':'512Mi'}}]} if name=='prometheus' else {})}}}))
    # UI and Prometheus never published to the Internet. Operator uses SSH
    # port-forward; arbitrary other namespaces do not gain log/metric access.
    result.append({'apiVersion':'networking.k8s.io/v1','kind':'NetworkPolicy','metadata':{'name':'observability-private'},
        'spec':{'podSelector':{'matchExpressions':[{'key':'app','operator':'In','values':['observability','prometheus']}]},
                'policyTypes':['Ingress'],'ingress':[{'from':[{'podSelector':{}}]},
                    {'from':[{'namespaceSelector':{'matchLabels':{'kubernetes.io/metadata.name':'tester-agent'}}}]}]}})
    return result
