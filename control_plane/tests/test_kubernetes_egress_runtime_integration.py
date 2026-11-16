"""Double-opt-in formal HTTP Egress traffic and original-UID cleanup on the existing Calico cluster."""

import asyncio
import ipaddress
import json
import os
import secrets
import socket
import subprocess
import time
import threading
from collections import deque
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from kubernetes import client

from control_plane.app import create_app
from control_plane.core.errors import SandboxError
from control_plane.runtime.kubernetes import kubernetes_runtime
from control_plane.runtime.kubernetes.pod_query import sandbox_pod_name
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from test_kubernetes_proxy_integration import _RedactedSecret, _RedactedText
from test_kubernetes_workspace_resource_integration import pytestmark as storage_marks

pytestmark = [mark for mark in storage_marks if mark.name != 'timeout'] + [pytest.mark.timeout(360)]
PREFIX = 'io.agent-sandbox.'
IMAGE = 'docker.io/library/agent-sandbox-python@sha256:f4b9e2321c89babc0bd5435e4765416a1a3cfd49fa23f2d06999d1b77209dbff'
DOMAIN = 'example.com'


class Originals:
    """Keep only independent acknowledged identities; never derive delete authority from later candidates."""

    def __init__(self, api, path):
        """Own a separate fixture client and a private noncredential POST receipt journal."""
        self.api, self.path, self.entries = api, path, []
        self.pvs = {}
        path.touch(mode=0o600)

    def record(self, kind, body):
        """Capture full original labels, stable lifecycle metadata and exact target before POST."""
        entry = dict(kind=kind, name=body.metadata.name, namespace=body.metadata.namespace,
            labels=dict(body.metadata.labels), uid=None,
            annotations={key:value for key,value in body.metadata.annotations.items()
                if key in {PREFIX+'created-at',PREFIX+'expires-at',PREFIX+'workspace-image-id'}})
        self.entries.append(entry)
        self.path.write_text(json.dumps(self.entries), encoding='utf-8')
        return entry

    def verify(self, resource, entry):
        """Require exact original namespace/name/labels/times and acknowledged UID before fallback mutation."""
        meta=resource.metadata
        assert meta.name==entry['name'] and meta.namespace==entry['namespace']
        assert meta.labels==entry['labels'] and meta.owner_references in (None,[])
        assert all((meta.annotations or {}).get(k)==v for k,v in entry['annotations'].items())
        assert str(UUID(meta.uid))==meta.uid and meta.resource_version
        if entry['uid'] is not None:
            assert meta.uid==entry['uid'], 'original object replaced; fallback refused'

    def read(self, entry):
        """Confirm namespace accessibility before interpreting exact object absence."""
        facade=self.api.networking if entry['kind']=='network_policy' else self.api
        try:
            return getattr(facade,'read_namespaced_'+entry['kind'])(entry['name'],entry['namespace'],_request_timeout=(0.5,1))
        except client.ApiException as error:
            if error.status!=404:
                raise
            assert self.api.read_namespace(entry['namespace'],_request_timeout=(0.5,1)).metadata.name==entry['namespace']
            return None

    def observe_volumes(self):
        """Record only PVs bound to acknowledged own PVC UIDs; observation grants no PV delete permission."""
        for entry in self.entries:
            if entry['kind']=='persistent_volume_claim' and entry['uid'] is not None:
                pvc=self.read(entry)
                if pvc is not None:
                    self.verify(pvc,entry)
                    if pvc.spec.volume_name:
                        self.pvs[pvc.spec.volume_name]=entry['uid']

    def cleanup(self):
        """Stop original proxy/Agent consumers before Egress, then dependencies/PVC; use UID/current RV and normal grace."""
        self.observe_volumes()
        priorities={'proxy':0,'container':1,'egress':2}
        ordered=sorted(self.entries,key=lambda e: (priorities.get(e['labels'].get(PREFIX+'resource'),
            4 if e['kind']=='persistent_volume_claim' else 3)))
        for entry in ordered:
            observed=self.read(entry)
            if observed is None:
                continue
            assert entry['uid'] is not None,'lost original acknowledgement; refusing candidate adoption'
            self.verify(observed,entry)
            if observed.metadata.deletion_timestamp is None:
                facade=self.api.networking if entry['kind']=='network_policy' else self.api
                getattr(facade,'delete_namespaced_'+entry['kind'])(entry['name'],entry['namespace'],
                    body=client.V1DeleteOptions(preconditions=client.V1Preconditions(uid=entry['uid'],
                        resource_version=observed.metadata.resource_version)),_request_timeout=(0.5,1))
            deadline=time.monotonic()+45
            while time.monotonic()<deadline:
                remaining=self.read(entry)
                if remaining is None:
                    break
                self.verify(remaining,entry)
                time.sleep(0.1)
            else:
                raise AssertionError('original cleanup not confirmed; dependencies retained')


        for name,pvc_uid in self.pvs.items():
            deadline=time.monotonic()+45
            while time.monotonic()<deadline:
                try:
                    pv=self.api.read_persistent_volume(name,_request_timeout=(0.5,1))
                except client.ApiException as error:
                    if error.status==404:
                        break
                    raise
                assert pv.spec.claim_ref is not None and pv.spec.claim_ref.uid==pvc_uid
                assert pv.spec.persistent_volume_reclaim_policy=='Delete'
                time.sleep(0.2)
            else:
                raise AssertionError('Delete-reclaim PV not observed absent; no direct PV deletion attempted')

        discovery=client.DiscoveryV1Api(self.api.api_client)
        for entry in self.entries:
            if entry['kind']!='service':
                continue
            deadline=time.monotonic()+10
            while time.monotonic()<deadline:
                page=discovery.list_namespaced_endpoint_slice(entry['namespace'],
                    label_selector='kubernetes.io/service-name='+entry['name'],limit=3,_request_timeout=(0.5,1))
                assert page.metadata._continue in (None,'')
                if not page.items:
                    break
                assert all(any(owner.uid==entry['uid'] for owner in item.metadata.owner_references or []) for item in page.items)
                time.sleep(0.1)
            else:
                raise AssertionError('original Service EndpointSlice cleanup not confirmed')


class Recording:
    """Borrow each Runtime worker pool and capture only actual original POST responses independently."""

    def __init__(self,api,registry,*,networking=False):
        """Share Core/Networking ownership; the Runtime worker closes its original client."""
        self.api,self.registry=api,registry
        if not networking:
            self.networking=Recording(api.networking,registry,networking=True)

    def __getattr__(self,name):
        """Intercept POST acknowledgements and redact Secret diagnostic objects without changing requests."""
        operation=getattr(self.api,name)
        if not name.startswith('create_namespaced_') and 'secret' not in name:
            return operation

        def call(*args,**kwargs):
            """Persist successful original UIDs; never record Secret data or token/digest fields."""
            entry=None
            if name.startswith('create_namespaced_'):
                body=kwargs.get('body') if 'body' in kwargs else args[1]
                if name.endswith('_secret'):
                    body.__class__=_RedactedSecret
                entry=self.registry.record(name.removeprefix('create_namespaced_'),body)
            try:
                result=operation(*args,**kwargs)
            except client.ApiException as error:
                raise client.ApiException(status=error.status,reason='integration operation failed') from None
            if 'secret' in name:
                if isinstance(result,client.V1Secret):
                    result.__class__=_RedactedSecret
                if isinstance(result,client.V1SecretList):
                    for item in result.items or []:
                        item.__class__=_RedactedSecret
            if entry is not None:
                self.registry.verify(result,entry)
                entry['uid']=result.metadata.uid
                self.registry.path.write_text(json.dumps(self.registry.entries),encoding='utf-8')
            return result
        return call


@pytest.fixture
def egress_cluster(tmp_path,monkeypatch):
    """Default marker skips before any configuration/cache/client access; all real resources have independent receipts."""
    path=Path(os.environ.get('KUBERNETES_TEST_KUBECONFIG',str(Path.home()/'.kube'/'config')))
    context,namespace=os.environ['KUBERNETES_TEST_CONTEXT'],os.environ['KUBERNETES_TEST_NAMESPACE']
    images=(IMAGE,'docker.io/library/agent-sandbox-basic:dev','docker.io/library/agent-sandbox-egress:dev',
            'docker.io/library/agent-sandbox-proxy-upstream-test:dev')
    for node in ('agent-sandbox-k8s','agent-sandbox-k8s-m02','agent-sandbox-k8s-m03'):
        for image in images:
            checked=subprocess.run(['minikube','-p','agent-sandbox-k8s','ssh','--node='+node,'--',
                'sudo','crictl','inspecti','--output','go-template','--template','{{.status.id}}',image],
                capture_output=True,text=True,timeout=20)
            assert checked.returncode==0,'required image not cached; no pull/build attempted'
    factory=kubernetes_runtime.create_probe_client
    api=factory(kubeconfig_path=path,context=context)
    registry=Originals(api,tmp_path/'original-post-uids.json')
    processes=[]
    locations=deque(maxlen=64)
    previous_trace=threading.gettrace()

    def trace(frame,event,arg):
        """Retain fixed code locations/classes only when diagnosing a worker failure, never exception content."""
        if '/runtime/kubernetes/' not in frame.f_code.co_filename:
            return None
        frame.f_trace_lines=False
        if event=='exception':
            category=arg[0].__name__
            if isinstance(arg[1],client.ApiException):
                if arg[1].status==404:
                    return trace
                category=category+':'+str(arg[1].status)
            if category not in {'GeneratorExit','StopIteration'}:
                locations.append((frame.f_code.co_filename.rsplit('/',1)[-1],frame.f_code.co_name,frame.f_lineno,category))
        return trace

    threading.settrace(trace)
    spawn=asyncio.create_subprocess_exec

    async def counted(*args,**kwargs):
        """Retain exact self-owned port-forward handles, never tokens or raw subprocess output."""
        process=await spawn(*args,**kwargs)
        processes.append(process)
        return process

    def recording(**kwargs):
        """Keep construction lazy and worker-owned while retaining original acknowledgements."""
        return Recording(factory(**kwargs),registry)

    monkeypatch.setattr(kubernetes_runtime,'create_probe_client',recording)
    monkeypatch.setattr(asyncio,'create_subprocess_exec',counted)
    try:
        assert api.read_namespace(namespace,_request_timeout=(0.5,1)).metadata.name==namespace
        yield dict(path=path,context=context,namespace=namespace,api=api,registry=registry,processes=processes,locations=locations)
    finally:
        try:
            registry.cleanup()
            assert all(p.returncode is not None for p in processes),'owned entry process still alive'
        finally:
            threading.settrace(previous_trace)
            api.api_client.close()


def network_location_and_dns(h, sid):
    """Observe only own Pod scheduling and bounded Egress DNS diagnostics, never change cluster DNS or policies."""
    entries=[e for e in h['registry'].entries if e['labels'].get(PREFIX+'sandbox-id')==sid and e['kind']=='pod']
    facts={}
    for entry in entries:
        pod=h['registry'].read(entry)
        h['registry'].verify(pod,entry)
        role=entry['labels'][PREFIX+'resource']
        facts[role]=dict(node=pod.spec.node_name,ip=pod.status.pod_ip)
        if role=='egress':
            try:
                result=subprocess.run(['kubectl','--kubeconfig',str(h['path']),'--context',h['context'],
                    '--namespace',h['namespace'],'--request-timeout=5s','exec',pod.metadata.name,
                    '--container','egress','--','/bin/busybox','nslookup',DOMAIN],
                    capture_output=True,text=True,timeout=6)
                facts['egress_dns_exit']=result.returncode
                web=subprocess.run(['kubectl','--kubeconfig',str(h['path']),'--context',h['context'],
                    '--namespace',h['namespace'],'--request-timeout=5s','exec',pod.metadata.name,
                    '--container','egress','--','/bin/busybox','wget','-q','-T','4','-O','/dev/null','http://'+DOMAIN+'/'],
                    capture_output=True,text=True,timeout=6)
                facts['egress_direct_http_exit']=web.returncode
            except subprocess.TimeoutExpired:
                facts['egress_dns_exit']='timeout'
    if 'egress' in facts:
        facts['same_node_control']=same_node_public_control(h,facts['egress']['node'])
    return facts


def same_node_public_control(h,node):
    """Compare a single own non-isolated Pod on the same node; never loosen the sandbox policies."""
    sid='sbx_'+secrets.token_hex(16)
    now=datetime.now(UTC)
    labels={PREFIX+'managed':'true',PREFIX+'project':'agent-sandbox',PREFIX+'sandbox-id':sid,
            PREFIX+'transaction-id':'txn_'+secrets.token_hex(16),PREFIX+'resource':'test-network-control'}
    pod=client.V1Pod(api_version='v1',kind='Pod',metadata=client.V1ObjectMeta(
        namespace=h['namespace'],name=sandbox_pod_name(sid)+'-control',labels=labels,
        annotations={PREFIX+'created-at':now.isoformat(),PREFIX+'expires-at':(now+timedelta(minutes=2)).isoformat()}),
        spec=client.V1PodSpec(restart_policy='Never',node_selector={'kubernetes.io/hostname':node},
            automount_service_account_token=False,enable_service_links=False,host_network=False,host_pid=False,host_ipc=False,
            security_context=client.V1PodSecurityContext(run_as_non_root=True,run_as_user=10001,run_as_group=10001,
                seccomp_profile=client.V1SeccompProfile(type='RuntimeDefault')),
            containers=[client.V1Container(name='control',image=IMAGE,image_pull_policy='Never',
                command=['python','-c','import time;time.sleep(90)'],
                resources=client.V1ResourceRequirements(requests={'cpu':'100m','memory':'64Mi'},limits={'cpu':'100m','memory':'64Mi'}),
                security_context=client.V1SecurityContext(allow_privilege_escalation=False,read_only_root_filesystem=True,
                    capabilities=client.V1Capabilities(drop=['ALL'])))]))
    original=Recording(h['api'],h['registry']).create_namespaced_pod(h['namespace'],body=pod,_request_timeout=(0.5,1))
    entry=h['registry'].entries[-1]
    deadline=time.monotonic()+20
    while time.monotonic()<deadline:
        live=h['registry'].read(entry)
        h['registry'].verify(live,entry)
        if live.status and live.status.phase=='Running':
            break
        time.sleep(0.1)
    else:
        return {'started':False}
    source="""import http.client,json,ssl
out={}
for scheme in ('http','https'):
 try:
  c=http.client.HTTPConnection('example.com',80,timeout=4) if scheme=='http' else http.client.HTTPSConnection('example.com',443,timeout=4,context=ssl.create_default_context())
  c.request('GET','/');r=c.getresponse();out[scheme]=r.status;r.read(2048);c.close()
 except Exception:out[scheme]=0
print(json.dumps(out))
"""
    result=subprocess.run(['kubectl','--kubeconfig',str(h['path']),'--context',h['context'],
        '--namespace',h['namespace'],'--request-timeout=5s','exec',original.metadata.name,
        '--container','control','--','python','-c',source],capture_output=True,text=True,timeout=12)
    return json.loads(result.stdout) if result.returncode==0 else {'exec_failed':True}


async def command(url,token,source):
    """Execute through the returned authenticated loopback endpoint and return only fixed JSON observations."""
    async with httpx.AsyncClient(trust_env=False,follow_redirects=False,timeout=40) as http:
        response=await http.post(url+'/v1/commands:run',headers={'Authorization':'Bearer '+token},
            json={'argv':['python','-c',source],'timeout_ms':30000})
        assert response.status_code==200
        payload=response.json()
        assert payload['exit_code']==0, 'bounded network probe did not finish normally'
        return json.loads(payload['stdout'])


def network_probe(domain,public_ip,dns_ip):
    """Use explicit HTTP/CONNECT sockets, validated TLS and numeric direct probes; no automatic redirects or DNS proxies."""
    return '''import http.client,json,os,socket,ssl,time,urllib.parse
proxy=urllib.parse.urlsplit(os.environ['HTTP_PROXY'])
out={}
for name,target,method in [('http','http://DOMAIN/','GET'),('denied','http://example.org/','GET'),
 ('private','http://10.0.0.1/','GET'),('link_local','http://169.254.1.1/','GET'),
 ('port','http://DOMAIN:8088/','GET'),('connect_port','DOMAIN:8443','CONNECT')]:
 for attempt in range(2 if name=='http' else 1):
  try:
   c=http.client.HTTPConnection(proxy.hostname,proxy.port,timeout=4)
   c.request(method,target,headers={'Host': 'DOMAIN'})
   r=c.getresponse();out[name]=r.status;r.read(2048);c.close()
  except Exception: out[name]=0
  if name!='http' or 200<=out[name]<400:break
  time.sleep(0.25)
for attempt in range(2):
 try:
  c=http.client.HTTPSConnection(proxy.hostname,proxy.port,timeout=6,context=ssl.create_default_context())
  c.set_tunnel('DOMAIN',443);c.request('GET','/');r=c.getresponse();out['https']=r.status;r.read(2048);c.close()
 except Exception:out['https']=0
 if 200<=out['https']<400:break
 time.sleep(0.25)
try:
 s=socket.create_connection(('PUBLIC_IP',80),timeout=1.5);s.close();out['direct_public']=True
except Exception:out['direct_public']=False
try:
 s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);s.settimeout(1.5)
 s.sendto(bytes.fromhex('123401000001000000000000')+b'\\x07example\\x03com\\x00\\x00\\x01\\x00\\x01',('DNS_IP',53))
 s.recv(1024);out['direct_dns']=True;s.close()
except Exception:out['direct_dns']=False
print(json.dumps(out))
'''.replace('DOMAIN',domain).replace('PUBLIC_IP',public_ip).replace('DNS_IP',dns_ip)


@pytest.mark.parametrize('workspace',[False,True])
@pytest.mark.anyio
async def test_formal_http_egress_positive_denials_rebuild_and_cleanup(egress_cluster,tmp_path,monkeypatch,workspace):
    """Public HTTP/HTTPS must actually succeed; denials or Ready alone never satisfy positive acceptance."""
    h=egress_cluster
    for key,value in {
        'RUNTIME':'kubernetes','K8S_KUBECONFIG':str(h['path']),'K8S_CONTEXT':h['context'],'K8S_NAMESPACE':h['namespace'],
        'K8S_AGENT_IMAGE':'agent-sandbox-basic:dev','K8S_PYTHON_IMAGE':IMAGE,
        'K8S_PROXY_IMAGE':'agent-sandbox-proxy-upstream-test:dev','K8S_WORKSPACE_STORAGE_CLASS':'agent-sandbox-workspace',
        'K8S_EGRESS_IMAGE':'agent-sandbox-egress:dev','K8S_EGRESS_DNS_NAMESPACE':'kube-system',
        'K8S_EGRESS_DNS_POD_LABELS':json.dumps({'k8s-app':'kube-dns'}),
        'K8S_EGRESS_CLUSTER_CIDRS':json.dumps(['10.244.0.0/16','10.96.0.0/12','192.168.49.0/24']),
    }.items():
        monkeypatch.setenv('AGENT_SANDBOX_'+key,value)
    if not workspace:
        monkeypatch.delenv('AGENT_SANDBOX_K8S_WORKSPACE_STORAGE_CLASS',raising=False)
    store=SQLiteLifecycleStore(tmp_path/'egress-http.sqlite3')
    admin=_RedactedText(secrets.token_urlsafe(32))
    app=create_app(api_key=str(admin),store=store,reaper_enabled=False,sandbox_query_timeout=10)
    backend,manager=app.state.runtime,app.state.kubernetes_entry_manager
    failures=[]
    create=backend.create

    async def observed(spec):
        """Capture only fixed failure categories/evidence, without raw API responses or credentials."""
        try:
            return await create(spec)
        except SandboxError as error:
            info=error.creation_failure
            failures.append((type(error).__name__,info.phase if info else None,info.resource_state.value if info else None,tuple(h["locations"])))
            journal=tmp_path/'creation-error-locations.json'
            journal.touch(mode=0o600)
            journal.write_text(json.dumps(failures),encoding='utf-8')
            raise

    monkeypatch.setattr(backend,'create',observed)
    headers={'Authorization':'Bearer '+admin}
    try:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://control',trust_env=False,timeout=120) as http:
                wid=None
                if workspace:
                    made=await http.post('/v1/workspaces',headers=headers,json={'environment':'python'})
                    assert made.status_code==201
                    wid=made.json()['workspace_id']
                options={'environment':'python','network':{'allow_domains':[DOMAIN]}}
                if wid:
                    options['workspace_id']=wid
                made=await http.post('/v1/sandboxes',headers=headers,json=options)
                assert made.status_code==201,failures
                sid=made.json()['sandbox_id']
                endpoint=await http.get(f'/v1/sandboxes/{sid}/endpoint',headers=headers)
                assert endpoint.status_code==200 and endpoint.headers['cache-control']=='no-store'
                value=endpoint.json()
                url,token=value['url'],_RedactedText(value['access_token'])
                original=await store.get(sid)
                from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime
                rebuilt=KubernetesSandboxRuntime(kubeconfig_path=h['path'],context=h['context'],namespace=h['namespace'])
                assert (await rebuilt.get(sid)).status=='ready'
                assert (await rebuilt.inspect_resources(sid,expected_transaction_id=original.transaction_id,
                    expected_workspace=await app.state.service._workspace_binding(original))).classification=='all_present'
                dns=h['api'].read_namespaced_service('kube-dns','kube-system',_request_timeout=(0.5,1)).spec.cluster_ip
                public=socket.gethostbyname(DOMAIN)
                assert ipaddress.ip_address(public).is_global
                h['registry'].observe_volumes()
                facts=await command(url,token,network_probe(DOMAIN,public,dns))
                observations=tmp_path/'network-observations.json'
                observations.touch(mode=0o600)
                observations.write_text(json.dumps(facts),encoding='utf-8')
                # Evaluate denials even when the independent positive internet
                # path is blocked; report that as a failure, never a skip/pass.
                assert facts['denied']==403 and facts['private'] in {400,403} and facts['link_local'] in {400,403},facts
                assert facts['port'] in {400,403} and facts['connect_port'] in {400,403}
                assert facts['direct_public'] is False and facts['direct_dns'] is False,facts
                if not (200<=facts['http']<400 and 200<=facts['https']<400):
                    facts['location_and_dns']=await asyncio.to_thread(network_location_and_dns,h,sid)
                assert 200<=facts['http']<400 and 200<=facts['https']<400, 'positive egress blocked: '+json.dumps(facts)
                other=await http.post('/v1/sandboxes',headers=headers,json={'environment':'python'})
                assert other.status_code==201
                bid=other.json()['sandbox_id']
                entry=(await http.get(f'/v1/sandboxes/{bid}/endpoint',headers=headers)).json()
                agent=h['api'].read_namespaced_pod(sandbox_pod_name(sid),h['namespace'],_request_timeout=(0.5,1))
                proxy_url=next(v.value for v in agent.spec.containers[0].env if v.name=='HTTP_PROXY')
                cross=await command(entry['url'],_RedactedText(entry['access_token']),
                    "import http.client,json,urllib.parse\np=urllib.parse.urlsplit("+repr(proxy_url)+")\n"
                    "try:\n c=http.client.HTTPConnection(p.hostname,p.port,timeout=2);c.request('GET','http://example.com/');r=c.getresponse();ok=True\n"
                    "except Exception:ok=False\nprint(json.dumps({'connected':ok}))")
                assert cross=={'connected':False}
                h['registry'].observe_volumes()
                assert (await http.delete(f'/v1/sandboxes/{bid}',headers=headers,params={'delete_workspace':'true'})).status_code==204
                # Leave A managed and alive across application shutdown. The
                # lifespan closes its entry without deleting Kubernetes data.
                assert await store.get(sid) is not None
    finally:
        await manager.close_all()
    assert manager.session_count==0 and all(process.returncode is not None for process in h['processes'])
    for field in ('EGRESS_IMAGE','EGRESS_DNS_NAMESPACE','EGRESS_DNS_POD_LABELS','EGRESS_CLUSTER_CIDRS','PYTHON_IMAGE'):
        monkeypatch.delenv('AGENT_SANDBOX_K8S_'+field,raising=False)
    reopened_store=SQLiteLifecycleStore(tmp_path/'egress-http.sqlite3')
    reopened=create_app(api_key=str(admin),store=reopened_store,reaper_enabled=False,sandbox_query_timeout=10)
    assert reopened.state.runtime.egress_configuration is None
    async with reopened.router.lifespan_context(reopened):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=reopened),base_url='http://control',trust_env=False,timeout=120) as http:
            assert (await http.get(f'/v1/sandboxes/{sid}',headers=headers)).status_code==200
            live=await reopened_store.get(sid)
            binding=await reopened.state.service._workspace_binding(live)
            inspected=await reopened.state.runtime.inspect_resources(sid,expected_transaction_id=live.transaction_id,
                                                                    expected_workspace=binding)
            assert inspected.classification=='all_present'
            assert (await http.delete(f'/v1/sandboxes/{sid}',headers=headers,
                params={} if workspace else {'delete_workspace':'true'})).status_code==204
            if wid:
                assert (await http.get(f'/v1/workspaces/{wid}',headers=headers)).status_code==200
                assert (await http.delete(f'/v1/workspaces/{wid}',headers=headers)).status_code==204
            assert await reopened_store.get(sid) is None
    await asyncio.to_thread(h['registry'].cleanup)
    assert all(h['registry'].read(entry) is None for entry in h['registry'].entries)
    discovery=client.DiscoveryV1Api(h['api'].api_client)
    for entry in h['registry'].entries:
        if entry['kind']=='service':
            deadline=time.monotonic()+10
            while time.monotonic()<deadline:
                page=discovery.list_namespaced_endpoint_slice(h['namespace'],
                    label_selector='kubernetes.io/service-name='+entry['name'],limit=3,_request_timeout=(0.5,1))
                assert page.metadata._continue in (None,'')
                if not page.items:
                    break
                await asyncio.sleep(0.1)
            else:
                raise AssertionError('owned Service EndpointSlice cleanup not confirmed')
