import unittest
from typing import Set, Optional, Tuple

from pygissim.engine import *
from tests.test_network import *
from tests.test_compute import *
from tests.test_queue import *

class TestWorkflowDefStep(unittest.TestCase):
    def test_create(self):
        wds: WorkflowDefStep = TestWorkflowDefStep.sample_browser_wds()
        self.assertEqual(2134, wds.response_size_kb)
        wds: WorkflowDefStep = TestWorkflowDefStep.sample_cachemap_wds()
        wds: WorkflowDefStep = TestWorkflowDefStep.sample_dbms_wds()
        wds: WorkflowDefStep = TestWorkflowDefStep.sample_dynmap_wds()
        wds: WorkflowDefStep = TestWorkflowDefStep.sample_file_wds()
        wds: WorkflowDefStep = TestWorkflowDefStep.sample_host_wds()
        wds: WorkflowDefStep = TestWorkflowDefStep.sample_mobile_wds()
        wds: WorkflowDefStep = TestWorkflowDefStep.sample_portal_wds()
        wds: WorkflowDefStep = TestWorkflowDefStep.sample_pro_wds()
        wds: WorkflowDefStep = TestWorkflowDefStep.sample_relds_wds()
        wds: WorkflowDefStep = TestWorkflowDefStep.sample_vdi_wds()
        wds: WorkflowDefStep = TestWorkflowDefStep.sample_web_wds()

    @classmethod
    def sample_pro_wds(cls) -> WorkflowDefStep:
        return WorkflowDefStep(name='Pro Client Step', description='Sample Pro Step', service_type='pro',
                               service_time=831, chatter=500, request_size_kb=1000, response_size_kb=13340, 
                               data_source_type=DataSourceType.DBMS, cache_pct=0)

    @classmethod
    def sample_browser_wds(cls) -> WorkflowDefStep:
        return WorkflowDefStep(name='Browser Client Step', description='Sample Browser Step', service_type='browser',
                               service_time=20, chatter=10, request_size_kb=100, response_size_kb=2134, 
                               data_source_type=DataSourceType.NONE, cache_pct=20)

    @classmethod
    def sample_mobile_wds(cls) -> WorkflowDefStep:
        return WorkflowDefStep(name='Mobile Client Step', description='Sample Mobile Step', service_type='mobile',
                               service_time=20, chatter=10, request_size_kb=100, response_size_kb=2134, 
                               data_source_type=DataSourceType.NONE, cache_pct=20)

    @classmethod
    def sample_vdi_wds(cls) -> WorkflowDefStep:
        return WorkflowDefStep(name='VDI Client Step', description='Sample VDI Step', service_type='vdi',
                               service_time=831, chatter=10, request_size_kb=100, response_size_kb=3691, 
                               data_source_type=DataSourceType.DBMS, cache_pct=0)


    @classmethod
    def sample_web_wds(cls) -> WorkflowDefStep:
        return WorkflowDefStep(name='Web Service Step', description='Sample Web Step', service_type='web',
                               service_time=18, chatter=10, request_size_kb=100, response_size_kb=2134, 
                               data_source_type=DataSourceType.NONE, cache_pct=0)

    @classmethod
    def sample_portal_wds(cls) -> WorkflowDefStep:
        return WorkflowDefStep(name='Portal Service Step', description='Sample Portal Step', service_type='portal',
                               service_time=19, chatter=10, request_size_kb=100, response_size_kb=2134, 
                               data_source_type=DataSourceType.FILE, cache_pct=0)

    @classmethod
    def sample_dynmap_wds(cls) -> WorkflowDefStep:
        return WorkflowDefStep(name='Dynamic Map Service Step', description='Sample Dynamic Map Step', service_type='map',
                               service_time=141, chatter=10, request_size_kb=100, response_size_kb=2134, 
                               data_source_type=DataSourceType.DBMS, cache_pct=0)

    @classmethod
    def sample_cachemap_wds(cls) -> WorkflowDefStep:
        return WorkflowDefStep(name='Cached Map Service Step', description='Sample Cached Map Step', service_type='map',
                               service_time=1, chatter=10, request_size_kb=100, response_size_kb=2134, 
                               data_source_type=DataSourceType.FILE, cache_pct=100)

    @classmethod
    def sample_host_wds(cls) -> WorkflowDefStep:
        return WorkflowDefStep(name='Hosted Service Step', description='Sample Hosted Step', service_type='feature',
                               service_time=70, chatter=10, request_size_kb=100, response_size_kb=4000, 
                               data_source_type=DataSourceType.RELATIONAL, cache_pct=0)

    @classmethod
    def sample_dbms_wds(cls) -> WorkflowDefStep:
        return WorkflowDefStep(name='DBMS Service Step', description='Sample DBMS Step', service_type='dbms',
                               service_time=24, chatter=500, request_size_kb=500, response_size_kb=13340, 
                               data_source_type=DataSourceType.FILE, cache_pct=75)

    @classmethod
    def sample_file_wds(cls) -> WorkflowDefStep:
        return WorkflowDefStep(name='File Service Step', description='Sample File Step', service_type='file',
                               service_time=24, chatter=500, request_size_kb=1000, response_size_kb=13340, 
                               data_source_type=DataSourceType.FILE, cache_pct=0)

    @classmethod
    def sample_relds_wds(cls) -> WorkflowDefStep:
        return WorkflowDefStep(name='Relational Service Step', description='Sample Relational Step', service_type='relational',
                               service_time=24, chatter=10, request_size_kb=1000, response_size_kb=13340, 
                               data_source_type=DataSourceType.FILE, cache_pct=0)


class TestWorkflowDef(unittest.TestCase):
    def test_create(self):
        wfd: WorkflowDef = TestWorkflowDef.sample_web_wfdef()
        self.assertEqual('Web Map Definition', wfd.name)
        self.assertEqual('Sample Web Map', wfd.description)
        self.assertEqual(2, len(wfd.chains))
        self.assertEqual('map', wfd.chains[0].steps[3].service_type)
        self.assertEqual('dbms', wfd.chains[0].steps[4].service_type)
        self.assertEqual('map', wfd.chains[1].steps[3].service_type)
        self.assertSetEqual(set(["portal", "file", "browser", "map", "dbms", "web"]), wfd.all_required_service_types())
        wfd = TestWorkflowDef.sample_mobile_wfdef()
        wfd = TestWorkflowDef.sample_vdi_wfdef()
        wfd = TestWorkflowDef.sample_workstation_wfdef()

    def test_add_chain(self):
        wfd: WorkflowDef = TestWorkflowDef.sample_web_wfdef()
        overlay: WorkflowChain = WorkflowChain(name="Hosted Features", desc='', 
                                               service_providers=dict(), 
                                               steps=[
                                                   TestWorkflowDefStep.sample_browser_wds(),
                                                   TestWorkflowDefStep.sample_web_wds(),
                                                   TestWorkflowDefStep.sample_portal_wds(),
                                                   TestWorkflowDefStep.sample_host_wds(),
                                                   TestWorkflowDefStep.sample_relds_wds()
                                               ])
        wfd.chains.insert(0, overlay)
        self.assertEqual(3, len(wfd.chains))
        self.assertEqual('feature', wfd.chains[0].steps[3].service_type)
        self.assertEqual('relational', wfd.chains[0].steps[4].service_type)
        self.assertSetEqual(set(["portal", "file", "browser", "map", "dbms", "web", "feature", "relational"]), wfd.all_required_service_types())
    
    def test_remove_chain(self):
        wfd: WorkflowDef = TestWorkflowDef.sample_web_wfdef()
        wfd.chains.pop(0)
        self.assertEqual(1, len(wfd.chains))
        self.assertEqual('map', wfd.chains[0].steps[3].service_type)
        self.assertEqual('file', wfd.chains[0].steps[4].service_type)
        self.assertSetEqual(set(["portal", "file", "browser", "map", "web"]), wfd.all_required_service_types())

    def test_swap_clients(self):
        dyn_chain: WorkflowChain = TestWorkflowDef.sample_dynmap_chain(TestWorkflowDefStep.sample_browser_wds())
        self.assertEqual('browser', dyn_chain.steps[0].service_type)
        self.assertEqual(5, len(dyn_chain.steps))
        dyn_chain.update_client_step(TestWorkflowDefStep.sample_mobile_wds())
        self.assertEqual('mobile', dyn_chain.steps[0].service_type)
        self.assertEqual(5, len(dyn_chain.steps))


    # Sample Chains
    @classmethod
    def sample_dynmap_chain(cls, client: WorkflowDefStep) -> WorkflowChain:
        steps: list[WorkflowDefStep] = [TestWorkflowDefStep.sample_web_wds(),
                                        TestWorkflowDefStep.sample_portal_wds(),
                                        TestWorkflowDefStep.sample_dynmap_wds(),
                                        TestWorkflowDefStep.sample_dbms_wds()]
        return WorkflowChain(name='Dynamic Map Image', desc='', steps=steps, service_providers=dict(), additional_client_step=client)

    @classmethod
    def sample_basemap_chain(cls, client: WorkflowDefStep) -> WorkflowChain:
        steps: list[WorkflowDefStep] = [TestWorkflowDefStep.sample_web_wds(),
                                        TestWorkflowDefStep.sample_portal_wds(),
                                        TestWorkflowDefStep.sample_cachemap_wds(),
                                        TestWorkflowDefStep.sample_file_wds()]
        return WorkflowChain(name='Cached Map Image', desc='', steps=steps, service_providers=dict(), additional_client_step=client)

    @classmethod
    def sample_hosted_chain(cls, client: WorkflowDefStep) -> WorkflowChain:
        steps: list[WorkflowDefStep] = [TestWorkflowDefStep.sample_web_wds(),
                                        TestWorkflowDefStep.sample_portal_wds(),
                                        TestWorkflowDefStep.sample_host_wds(),
                                        TestWorkflowDefStep.sample_relds_wds()]
        return WorkflowChain(name='Hosted Features', desc='', steps=steps, service_providers=dict(), additional_client_step=client)

    @classmethod
    def sample_pro_chain(cls) -> WorkflowChain:
        steps: list[WorkflowDefStep] = [TestWorkflowDefStep.sample_pro_wds(),
                                        TestWorkflowDefStep.sample_dbms_wds()]
        return WorkflowChain(name='Pro DC', desc='', steps=steps, service_providers=dict(), additional_client_step=None)

    @classmethod
    def sample_provdi_chain(cls) -> WorkflowChain:
        steps: list[WorkflowDefStep] = [TestWorkflowDefStep.sample_vdi_wds(),
                                        TestWorkflowDefStep.sample_pro_wds(),
                                        TestWorkflowDefStep.sample_dbms_wds()]
        return WorkflowChain(name='Pro VDI DC', desc='', steps=steps, service_providers=dict(), additional_client_step=None)

    # Sample Workflow Definitions
    @classmethod
    def sample_web_wfdef(cls) -> WorkflowDef:
        chains: list[WorkflowChain] = [TestWorkflowDef.sample_dynmap_chain(TestWorkflowDefStep.sample_browser_wds()),
                                       TestWorkflowDef.sample_basemap_chain(TestWorkflowDefStep.sample_browser_wds())]
        return WorkflowDef(name='Web Map Definition', desc='Sample Web Map', think_time_s=6, chains=chains)

    @classmethod
    def sample_mobile_wfdef(cls) -> WorkflowDef:
        chains: list[WorkflowChain] = [TestWorkflowDef.sample_hosted_chain(TestWorkflowDefStep.sample_mobile_wds()),
                                       TestWorkflowDef.sample_basemap_chain(TestWorkflowDefStep.sample_mobile_wds())]
        return WorkflowDef(name='Mobile Map Definition', desc='Sample Mobile Map', think_time_s=10, chains=chains)

    @classmethod
    def sample_workstation_wfdef(cls) -> WorkflowDef:
        chains: list[WorkflowChain] = [TestWorkflowDef.sample_pro_chain(),
                                       TestWorkflowDef.sample_basemap_chain(TestWorkflowDefStep.sample_pro_wds())]
        return WorkflowDef(name='Workstation Map Definition', desc='Sample Workstation Map', think_time_s=3, chains=chains)

    @classmethod
    def sample_vdi_wfdef(cls) -> WorkflowDef:
        chains: list[WorkflowChain] = [TestWorkflowDef.sample_provdi_chain(),
                                       TestWorkflowDef.sample_basemap_chain(TestWorkflowDefStep.sample_vdi_wds())]
        return WorkflowDef(name='VDI Map Definition', desc='Sample VDI Map', think_time_s=3, chains=chains)

class TestWorkflow(unittest.TestCase):
    def test_create(self):
        wf_pro: Workflow = TestWorkflow.sample_workstation_wf()
        self.assertEqual('Pro', wf_pro.name)
        self.assertListEqual([], wf_pro.definition.missing_service_providers())
        wf_vdi: Workflow = TestWorkflow.sample_vdi_wf()
        self.assertEqual('VDI', wf_vdi.name)
        self.assertListEqual([], wf_vdi.definition.missing_service_providers())
        wf_web: Workflow = TestWorkflow.sample_web_wf()
        self.assertEqual('Web', wf_web.name)
        self.assertListEqual([], wf_web.definition.missing_service_providers())

    def test_missing_sps(self):
        wf: Workflow = TestWorkflow.sample_web_wf()
        for chain in wf.definition.chains:
            chain.service_providers.clear()
        self.assertEqual(0, len(wf.definition.chains[0].service_providers.items()))
        sps: list[ServiceProvider] = [TestServiceProvider.sample_browser_sp(),
                                      TestServiceProvider.sample_file_sp(),
                                      TestServiceProvider.sample_web_sp()]
        for sp in sps: wf.definition.assign_service_provider(sp)
        # print(f'all required for wdef {wf.definition.name}: {wf.definition.all_required_service_types()}')
        missing: list[str] = wf.definition.missing_service_providers()
        # print(f'all missing for wdef {wf.definition.name}: {missing}')
        self.assertEqual(3, len(missing))
        self.assertSetEqual(set(['map', 'dbms', 'portal']), set(missing))
    
    @classmethod
    def sample_workstation_wf(cls) -> Workflow:
        w: Workflow = Workflow(name='Pro', desc='Local workstation', 
                        type=WorkflowType.USER, 
                        definition=TestWorkflowDef.sample_workstation_wfdef(), 
                        user_count=5, productivity=10)
        for sp in TestServiceProvider.sample_webgis_sps():
            w.definition.assign_service_provider(sp)
        return w
    
    @classmethod
    def sample_vdi_wf(cls) -> Workflow:
        w: Workflow = Workflow(name='VDI', desc='VDI workstation',
                        type=WorkflowType.USER,
                        definition=TestWorkflowDef.sample_vdi_wfdef(),
                        user_count=5, productivity=10)
        for sp in TestServiceProvider.sample_webgis_sps():
            w.definition.assign_service_provider(sp)
        return w
    
    @classmethod
    def sample_web_wf(cls) -> Workflow:
        w: Workflow = Workflow(name='Web', desc='Web application',
                        type=WorkflowType.TRANSACTIONAL,
                        definition=TestWorkflowDef.sample_web_wfdef(),
                        tph=10000)
        for sp in TestServiceProvider.sample_webgis_sps():
            w.definition.assign_service_provider(sp)
        return w
    
class TestClientRequestSolution(unittest.TestCase):
    
    def test_create(self):
        crs = TestClientRequestSolution.sample_intranet_crs()
        for step in crs.steps:
            print(step.st_calculator.name) # type: ignore
        self.assertEqual(17, len(crs.steps))

    
    @classmethod
    def sample_intranet_crs(cls) -> ClientRequestSolution:
        chain: WorkflowChain = TestWorkflowDef.sample_dynmap_chain(TestWorkflowDefStep.sample_browser_wds())
        for sp in TestServiceProvider.sample_webgis_sps():
            chain.service_providers[sp.service.service_type] = sp
        return create_solution(chain, TestRoute2.sample_intranet())
