import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('matrix_receiving_health', Path(__file__).parents[1]/'scripts/matrix_receiving_health.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class CompatibleHealthTests(unittest.TestCase):
    def setUp(self):
        self.server = {'embodiment_id': 'body'}
        self.required = [('being', 'body')]
        reader = dict(schema='dm.receiving.reader/v2', being_ref='being', embodiment_id='body',
                      credential_id='credential', validity={'mode':'until-revoked','not_before_ms':0},
                      authority_epochs=['epoch'], loaded_view_sha256='old-view',
                      receiving_selection_sha256='old-package',
                      wire_read=['dm.tribe.conversation/v1','dm.tribe.direct-conversation/v1'])
        self.status = dict(loaded_runtime=dict(local_origin=self.server, process_id=12),
            loaded_application=dict(loaded_view_sha256='new-view'), receiving=dict(
                channels=dict(tribe=dict(controlled=True, phase='select', process_id=12, readers=[reader])),
                rosters=dict(tribe=dict(audience=[dict(being_ref='being', embodiment_id='body', credential_id='credential')], required_authority_epochs=['epoch']))))

    def test_compatible_local_recovery_keeps_historical_software_provenance(self):
        self.assertTrue(m.ready(self.status, self.server, self.required, 10))

    def test_added_member_requires_its_reader_without_replacing_protected_bodies(self):
        status=copy.deepcopy(self.status)
        reader=dict(status['receiving']['channels']['tribe']['readers'][0],
                    being_ref='new',embodiment_id='new-body',credential_id='new-credential')
        status['receiving']['rosters']['tribe']['audience'].append(dict(
            being_ref='new',embodiment_id='new-body',credential_id='new-credential'))
        self.assertFalse(m.ready(status,self.server,self.required,10))
        status['receiving']['channels']['tribe']['readers'].append(reader)
        self.assertTrue(m.ready(status,self.server,self.required,10))
        self.assertFalse(m.ready(status,self.server,self.required+[('being','protected-sibling')],10))

    def test_actual_readiness_requirements_still_refuse(self):
        changes = [lambda s: s['receiving']['channels']['tribe'].update(phase='revoke'),
                   lambda s: s['receiving']['channels']['tribe'].update(process_id=13),
                   lambda s: s['loaded_runtime'].update(local_origin={}),
                   lambda s: s['receiving']['channels']['tribe']['readers'][0].update(wire_read=[]),
                   lambda s: s['receiving']['channels']['tribe']['readers'][0].update(authority_epochs=[]),
                   lambda s: s['receiving']['channels']['tribe']['readers'][0].update(validity={'mode':'finite','not_before_ms':0,'not_after_ms':5}),
                   lambda s: s['receiving']['rosters']['tribe'].update(audience=[])]
        for change in changes:
            with self.subTest(change=change):
                status=copy.deepcopy(self.status);change(status)
                self.assertFalse(m.ready(status, self.server, self.required, 10))
        self.assertFalse(m.ready(self.status,self.server,self.required+[('missing','body2')],10))



class UnavailableRosterTests(unittest.TestCase):
    def setUp(self):
        CompatibleHealthTests.setUp(self)

    def unavailable(self,retryable=True,error='reader_roster_unavailable'):
        status=copy.deepcopy(self.status)
        status['receiving']['rosters']['tribe']=dict(
            schema='dm.runtime.tribe-roster-unavailable/v1',error=error,retryable=retryable)
        return status

    def test_native_unavailable_contract_is_not_an_audience_key_error(self):
        for retryable in (False,True):
            self.assertFalse(m.ready(self.unavailable(retryable),self.server,self.required,10))

    def test_retryable_status_recovers_or_exhausts_finite_backoff(self):
        for next_status in (self.status,self.unavailable()):
            replies=[self.unavailable()]*4+[next_status]
            calls=[]
            request={'method':'runtime.status','params':{'view':'receiving'}}
            class Client:
                def send(_,value):
                    self.assertIs(value,request)
                    calls.append(value)
                    return dict(ok=True,result=replies[len(calls)-1],server=self.server)
            diagnostic={}
            response,count=m.read_status(Client(),request,{'pid':12},diagnostic)
            self.assertEqual(count,5)
            self.assertEqual(len(calls),5)
            self.assertEqual(m.ready(response['result'],self.server,self.required,10),next_status==self.status)

    def test_permanent_refusal_and_bad_auth_or_process_never_retry(self):
        for change in ('permanent','invalid','auth','process'):
            status=self.unavailable(False)
            if change=='invalid':status=self.unavailable(True,error='reader_roster_invalid')
            if change=='process':status['loaded_runtime']['process_id']=99
            calls=[]
            class Client:
                def send(_,request):
                    calls.append(request)
                    return dict(ok=change!='auth',result=status,server=self.server)
            if change in ('auth','process'):
                with self.assertRaises(ValueError):m.read_status(Client(),{}, {'pid':12},{})
            else:
                _,count=m.read_status(Client(),{}, {'pid':12},{})
                self.assertEqual(count,1)
            self.assertEqual(len(calls),1)

if __name__ == '__main__': unittest.main()
