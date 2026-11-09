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


if __name__ == '__main__': unittest.main()
