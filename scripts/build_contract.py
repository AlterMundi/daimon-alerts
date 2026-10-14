"""Generate the versioned schema and fictional examples from explicit definitions."""
import json
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
ID = {'type':'string','pattern':'^[A-Za-z0-9_-]{1,128}$'}
TIME = {'type':'integer','minimum':0}
CODES = ['payment_required','quota_exhausted','authentication_failed','access_denied','rate_limited','service_unavailable','timeout','transport_error','invalid_request','response_incomplete','dispatch_unresolved','provider_error','worker_stopped','worker_stalled','observation_stale','observation_unreadable','notification_unavailable']
def enum(*values): return {'type':'string','enum':list(values)}
def ref(name): return {'$ref':'#/$defs/'+name}
def obj(fields, optional=()): return {'type':'object','additionalProperties':False,'properties':fields,'required':[k for k in fields if k not in optional]}
def array(name): return {'type':'array','items':ref(name)}
def text(bound): return {'type':'string','minLength':1,'maxLength':bound,'pattern':'^[^\\u0000-\\u001f\\u007f]*$'}
input_fields = dict(schema={'const':'daimon-alerts/v1'},producer_id=ID,incident_id=ID,operation_id=ID,revision={'type':'integer','minimum':1,'maximum':9223372036854775807},receiving_scope=ID,phase=ID,resource_ref=ID,severity=enum('info','warning','error','critical'),problem_code=enum(*CODES),summary=text(280),action=text(500),source_at=TIME,updated_at=TIME,processing_state=enum('open','resolved'),work_preserved={'type':'boolean'})
defs = {'IncidentInput':obj(input_fields), 'ClaimInput':obj({'request_id':ID}), 'AckInput':obj({}), 'ReceiptInput':obj({'request_id':ID,'outcome':enum('delivered','uncertain','failed'),'message_ref':ID,'destination_ref':ID},('message_ref','destination_ref'))}
defs['Acceptance']=obj(dict(schema={'const':'daimon-alerts/v1'},producer_id=ID,incident_id=ID,revision=TIME,accepted_at=TIME,idempotent={'type':'boolean'}))
defs['Acknowledgment']=obj(dict(actor_id=ID,request_id=ID,acknowledged_at=TIME))
defs['Delivery']=obj(dict(delivery_id=ID,producer_id=ID,incident_id=ID,receiving_scope=ID,adapter_id=ID,created_at=TIME,state=enum('pending','uncertain','delivered','failed'),projection=ref('IncidentInput'),request_id={'anyOf':[ID,{'type':'null'}]},claimed_by={'anyOf':[ID,{'type':'null'}]},claimed_at={'anyOf':[TIME,{'type':'null'}]},receipt={'anyOf':[ref('ReceiptInput'),{'type':'null'}]}))
defs['Claim']=obj(dict(delivery_id=ID,request_id=ID,dispatch_allowed={'type':'boolean'},state=enum('uncertain','delivered','failed')))
defs['Incident']=obj(dict(projection=ref('IncidentInput'),accepted_at=TIME,observed_at=TIME,freshness=enum('fresh','stale'),acknowledgments=array('Acknowledgment'),deliveries=array('Delivery')))
for name,item in [('IncidentPage','Incident'),('DeliveryPage','Delivery')]:
    defs[name]=obj(dict(items=array(item),next_cursor={'type':['string','null']}))
defs['Observer']=obj(dict(observer_id=ID,receiving_scope=ID,observed_at=TIME,source_at={'anyOf':[TIME,{'type':'null'}]},source_state=enum('healthy','worker_stopped','worker_stalled','observation_stale','observation_unreadable'),freshness=enum('fresh','stale')))
defs['ObserverRetirementInput']=obj(dict(receiving_scope=ID))
defs['Status']=obj(dict(schema={'const':'daimon-alerts/v1'},service_time=TIME,state=enum('healthy','degraded','unobserved'),observers=array('Observer')))
defs['Error']=obj(dict(schema={'const':'daimon-alerts/v1'},error=enum('unauthorized','forbidden','not_found','invalid_request','stale_revision','revision_conflict','provenance_conflict','resolved_incident','claim_conflict','receipt_conflict','capacity_exceeded','internal_error','unavailable','payload_too_large','unsupported_media_type')))
schema={'$schema':'https://json-schema.org/draft/2020-12/schema','$id':'https://github.com/AlterMundi/daimon-alerts/schemas/daimon-alerts-v1.json','title':'Daimon Alerts v1','$defs':defs}
(ROOT/'schemas/daimon-alerts-v1.json').write_text(json.dumps(schema,indent=2)+'\n')
projection=dict(schema='daimon-alerts/v1',producer_id='fictional-hmk',incident_id='credit-failure-001',operation_id='batch-001',revision=1,receiving_scope='fictional-human',phase='index',resource_ref='fictional-memory-worker',severity='error',problem_code='payment_required',summary='Fictional memory index pending: provider credit required.',action='Restore provider credit, then explicitly resume the pending request.',source_at=1791880000,updated_at=1791880000,processing_state='open',work_preserved=True)
fixtures={'open':projection,'resolved':dict(projection,revision=2,updated_at=1791880010,processing_state='resolved',severity='info',summary='Fictional pending batch completed after explicit recovery.'),'success':dict(schema='daimon-alerts/v1',producer_id='fictional-hmk',incident_id='credit-failure-001',revision=1,accepted_at=1791880001,idempotent=False),'uncertain':dict(delivery_id='fictional-intent',request_id='attempt-001',dispatch_allowed=False,state='uncertain')}
for name,code in [('denial','forbidden'),('stale','stale_revision'),('conflict','revision_conflict'),('unavailable','unavailable')]: fixtures[name]=dict(schema='daimon-alerts/v1',error=code)
for name,value in fixtures.items(): (ROOT/f'fixtures/{name}.json').write_text(json.dumps(value,indent=2)+'\n')
