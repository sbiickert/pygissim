import unittest
import copy
from typing import Set, Optional

from pygissim.engine import *
from tests.test_network import *

class TestHWDef(unittest.TestCase):
    
    def test_create_hw_def(self):
        phone: HardwareDef = TestHWDef.sample_moble_hw_def()
        self.assertEqual('Apple Silicon M1', phone.processor)

    @classmethod
    def sample_moble_hw_def(cls) -> HardwareDef:
        return HardwareDef(processor='Apple Silicon M1', 
                           cores=8, specint_rate2017=500, 
                           architecture=ComputeArchitecture.ARM64, 
                           threading=ThreadingModel.PHYSICAL)
    
    @classmethod
    def sample_client_hw_def(cls) -> HardwareDef:
        return HardwareDef(processor='Intel Core i7-4770K', 
                           cores=4, specint_rate2017=20, 
                           architecture=ComputeArchitecture.INTEL, 
                           threading=ThreadingModel.PHYSICAL)
    
    @classmethod
    def sample_server_hw_def(cls) -> HardwareDef:
        return HardwareDef(processor='Intel Xeon E5-2643v3', 
                           cores=12, specint_rate2017=67, 
                           architecture=ComputeArchitecture.INTEL, 
                           threading=ThreadingModel.HYPERTHREADED)

class TestServiceDef(unittest.TestCase):

    def test_create_service_def(self):
        types: list[str] = TestServiceDef.sample_service_types()
        defs: list[ServiceDef] = list(map(lambda s: (TestServiceDef.sample_service(s)), types))
        self.assertEqual(len(types), len(defs))
        all_services: dict[str,ServiceDef] = dict(zip(types,defs))
        self.assertIn("pro", all_services)
        self.assertEqual("Pro", all_services['pro'].name)
        self.assertEqual(BalancingModel.ROUND_ROBIN, all_services['image'].balancing_model)
        self.assertEqual(BalancingModel.FAILOVER, all_services['portal'].balancing_model)
        self.assertEqual(BalancingModel.SINGLE, all_services['geoevent'].balancing_model)
        

    @classmethod
    def sample_service_types(cls) -> list[str]:
        return [
		"pro", "browser", "map", "feature", "image",
		"geocode", "geoevent", "geometry", "gp",
		"network", "scene", "sync", "stream",
		"ranalytics", "un", "custom", "vdi",
		"web", "portal", "dbms", "relational",
		"object", "stbds", "file", "mobile"]
    
    @classmethod
    def sample_service(cls, type: str) -> ServiceDef:
        match type:
            case "pro":
                return ServiceDef(name="Pro", description="Sample Pro", service_type=type, balancing_model=BalancingModel.SINGLE)
            case "browser":
                return ServiceDef(name="Browser", description="Sample Browser", service_type=type, balancing_model=BalancingModel.SINGLE)
            case "mobile":
                return ServiceDef(name="Mobile", description="Sample App", service_type=type, balancing_model=BalancingModel.SINGLE)
            case "map":
                return ServiceDef(name="Map", description="Sample Map", service_type=type, balancing_model=BalancingModel.ROUND_ROBIN)
            case "feature":
                return ServiceDef(name="Feature", description="Sample Feature", service_type=type, balancing_model=BalancingModel.ROUND_ROBIN)
            case "image":
                return ServiceDef(name="Image", description="Sample Image", service_type=type, balancing_model=BalancingModel.ROUND_ROBIN)
            case "geocode":
                return ServiceDef(name="Geocode", description="Sample Geocode", service_type=type, balancing_model=BalancingModel.ROUND_ROBIN)
            case "geoevent":
                return ServiceDef(name="Geoevent", description="Sample Geoevent", service_type=type, balancing_model=BalancingModel.SINGLE)
            case "geometry":
                return ServiceDef(name="Geometry", description="Sample Geometry", service_type=type, balancing_model=BalancingModel.ROUND_ROBIN)
            case "gp":
                return ServiceDef(name="GP", description="Sample GP", service_type=type, balancing_model=BalancingModel.ROUND_ROBIN)
            case "network":
                return ServiceDef(name="Network", description="Sample Network", service_type=type, balancing_model=BalancingModel.ROUND_ROBIN)
            case "scene":
                return ServiceDef(name="Scene", description="Sample Scene", service_type=type, balancing_model=BalancingModel.ROUND_ROBIN)
            case "sync":
                return ServiceDef(name="Sync", description="Sample Sync", service_type=type, balancing_model=BalancingModel.ROUND_ROBIN)
            case "stream":
                return ServiceDef(name="Stream", description="Sample Stream", service_type=type, balancing_model=BalancingModel.ROUND_ROBIN)
            case "ranalytics":
                return ServiceDef(name="Ranalytics", description="Sample Ranalytics", service_type=type, balancing_model=BalancingModel.ROUND_ROBIN)
            case "un":
                return ServiceDef(name="UN", description="Sample UN", service_type=type, balancing_model=BalancingModel.ROUND_ROBIN)
            case "custom":
                return ServiceDef(name="Custom", description="Sample Custom", service_type=type, balancing_model=BalancingModel.SINGLE)
            case "vdi":
                return ServiceDef(name="Vdi", description="Sample VDI", service_type=type, balancing_model=BalancingModel.FAILOVER)
            case "web":
                return ServiceDef(name="Web", description="Sample Web", service_type=type, balancing_model=BalancingModel.ROUND_ROBIN)
            case "portal":
                return ServiceDef(name="Portal", description="Sample Portal", service_type=type, balancing_model=BalancingModel.FAILOVER)
            case "dbms":
                return ServiceDef(name="DBMS", description="Sample DBMS", service_type=type, balancing_model=BalancingModel.FAILOVER)
            case "relational":
                return ServiceDef(name="Relational", description="Sample Relational", service_type=type, balancing_model=BalancingModel.FAILOVER)
            case "object":
                return ServiceDef(name="Object", description="Sample Object", service_type=type, balancing_model=BalancingModel.FAILOVER)
            case "stbds":
                return ServiceDef(name="STBDS", description="Sample Spatio-Temporal Data Store", service_type=type, balancing_model=BalancingModel.ROUND_ROBIN)
            case "file":
                return ServiceDef(name="File", description="Sample File", service_type=type, balancing_model=BalancingModel.FAILOVER)
            case _:
                return ServiceDef(name="None", description="Invalid", service_type=type, balancing_model=BalancingModel.SINGLE)

class TestComputeNode(unittest.TestCase):
    def test_create_compute(self):
        client: ComputeNode = TestComputeNode.sample_client()
        self.assertEqual('Client 001', client.name)
        phone: ComputeNode = TestComputeNode.sample_mobile()
        self.assertEqual(8, phone.memory_GB)
        v_host: ComputeNode = TestComputeNode.sample_vhost()
        self.assertEqual(4, v_host.vcore_count())
        p_host: ComputeNode = TestComputeNode.sample_phost()
        self.assertEqual('Host 001', p_host.name)
        self.assertEqual(1, p_host.virtual_host_count())
        self.assertEqual(2, p_host.total_cpu_allocation())
        self.assertEqual(4, p_host.total_vcpu_allocation())
        self.assertEqual(16, p_host.total_memory_allocation())
        v_host_opt: Optional[ComputeNode] = p_host.virtual_host(0)
        self.assertIsNotNone(v_host_opt)
        if v_host_opt is not None:
            self.assertEqual('VH Host 001: 0', v_host_opt.name)

    _sample_client: Optional[ComputeNode] = None
    _sample_mobile: Optional[ComputeNode] = None
    _sample_vhost: Optional[ComputeNode] = None
    _sample_phost: Optional[ComputeNode] = None
    
    @classmethod
    def sample_client(cls) -> ComputeNode:
        if TestComputeNode._sample_client is None:
            TestComputeNode._sample_client = ComputeNode(name='Client 001',
                                                         desc='Sample PC',
                                                         hw_def=TestHWDef.sample_client_hw_def(),
                                                         memory_GB=16,
                                                         zone=TestZone.sample_intranet_zone(),
                                                         type=ComputeNodeType.CLIENT)
        return TestComputeNode._sample_client
    
    @classmethod
    def sample_mobile(cls) -> ComputeNode:
        if TestComputeNode._sample_mobile is None:
            TestComputeNode._sample_mobile = ComputeNode(name='Mobile 001',
                                                         desc='Sample Phone',
                                                         hw_def=TestHWDef.sample_moble_hw_def(),
                                                         memory_GB=8,
                                                         zone=TestZone.sample_internet_zone(),
                                                         type=ComputeNodeType.CLIENT)
        return TestComputeNode._sample_mobile
    
    @classmethod
    def sample_vhost(cls) -> ComputeNode:
        if TestComputeNode._sample_vhost is None:
            TestComputeNode._sample_vhost = ComputeNode(name='VHost 001',
                                                         desc='Sample VHost',
                                                         hw_def=TestHWDef.sample_server_hw_def(),
                                                         memory_GB=16,
                                                         zone=TestZone.sample_intranet_zone(),
                                                         type=ComputeNodeType.V_SERVER)
            TestComputeNode._sample_vhost.set_vcore_count(4)
        return TestComputeNode._sample_vhost
    
    @classmethod
    def sample_phost(cls) -> ComputeNode:
        if TestComputeNode._sample_phost is None:
            TestComputeNode._sample_phost = ComputeNode(name='Host 001',
                                                         desc='Sample Physical Host',
                                                         hw_def=TestHWDef.sample_server_hw_def(),
                                                         memory_GB=64,
                                                         zone=TestZone.sample_intranet_zone(),
                                                         type=ComputeNodeType.P_SERVER)
            TestComputeNode._sample_phost.add_virtual_host('', 4, 16)
        return TestComputeNode._sample_phost

class TestServiceProvider(unittest.TestCase):
    def test_create(self):
        sp: ServiceProvider = TestServiceProvider.sample_web_sp()
        self.assertEqual('IIS', sp.name)
        handler: Optional[ComputeNode] = sp.handler_node()
        self.assertIsNotNone(handler)
        if handler is not None:
            self.assertEqual('Web 001', handler.name)
        self.assertTrue(sp.is_valid())

        sp = TestServiceProvider.sample_portal_sp()
        self.assertEqual('Portal', sp.name)
        self.assertTrue(sp.is_valid())

        sp = TestServiceProvider.sample_map_sp()
        self.assertEqual('GIS Site', sp.name)
        self.assertTrue(sp.is_valid())

        sp = TestServiceProvider.sample_ha_map_sp()
        self.assertEqual('GIS Site', sp.name)
        self.assertTrue(sp.is_valid())
        node1: Optional[ComputeNode] = sp.handler_node()
        node2: Optional[ComputeNode] = sp.handler_node()
        self.assertNotEqual(node1, node2) # Calling handler_node rotates the round robin

        sp = TestServiceProvider.sample_dbms_sp()
        self.assertEqual('SQL Server', sp.name)
        self.assertTrue(sp.is_valid())

        sp = TestServiceProvider.sample_ha_datastore_sp()
        self.assertEqual('Relational DS', sp.name)
        self.assertTrue(sp.is_valid())
        node1 = sp.handler_node()
        node2 = sp.handler_node()
        self.assertEqual(node1, node2) # Calling handler_node does not change the failover

        sp = TestServiceProvider.sample_file_sp()
        self.assertEqual('File Server', sp.name)
        self.assertTrue(sp.is_valid())

        sp = TestServiceProvider.sample_vdi_sp()
        self.assertEqual('VDI', sp.name)
        self.assertTrue(sp.is_valid())

        sp = TestServiceProvider.sample_browser_sp()
        self.assertEqual('Chrome', sp.name)
        self.assertTrue(sp.is_valid())

        sp = TestServiceProvider.sample_pro_sp()
        self.assertEqual('Pro', sp.name)
        self.assertTrue(sp.is_valid())


    _vm: ComputeNode = TestComputeNode.sample_vhost()

    @classmethod
    def vm(cls, name: str) -> ComputeNode:
        vm_copy: ComputeNode = copy.copy(TestServiceProvider._vm)
        vm_copy.name = name
        return vm_copy
    
    @classmethod
    def sample_web_sp(cls) -> ServiceProvider:
        web_vm: ComputeNode = TestServiceProvider.vm('Web 001')
        return ServiceProvider(name='IIS', desc='Web Server', service=TestServiceDef.sample_service('web'), nodes=[web_vm])
    
    @classmethod
    def sample_portal_sp(cls) -> ServiceProvider:
        my_vm: ComputeNode = TestServiceProvider.vm('Portal 001')
        return ServiceProvider(name='Portal', desc='Portal Server', service=TestServiceDef.sample_service('portal'), nodes=[my_vm])
    
    @classmethod
    def sample_map_sp(cls) -> ServiceProvider:
        my_vm: ComputeNode = TestServiceProvider.vm('GIS 001')
        return ServiceProvider(name='GIS Site', desc='Map Server Site', service=TestServiceDef.sample_service('map'), nodes=[my_vm])
    
    @classmethod
    def sample_ha_map_sp(cls) -> ServiceProvider:
        my_vm1: ComputeNode = TestServiceProvider.vm('GIS 001')
        my_vm2: ComputeNode = TestServiceProvider.vm('GIS 002')
        return ServiceProvider(name='GIS Site', desc='HA Map Server Site', service=TestServiceDef.sample_service('map'), nodes=[my_vm1, my_vm2])
    
    @classmethod
    def sample_dbms_sp(cls) -> ServiceProvider:
        my_vm: ComputeNode = TestServiceProvider.vm('SQL 001')
        return ServiceProvider(name='SQL Server', desc='Geodatabase', service=TestServiceDef.sample_service('dbms'), nodes=[my_vm])
    
    @classmethod
    def sample_ha_datastore_sp(cls) -> ServiceProvider:
        my_vm1: ComputeNode = TestServiceProvider.vm('DS 001')
        my_vm2: ComputeNode = TestServiceProvider.vm('DS 002')
        return ServiceProvider(name='Relational DS', desc='HA Datastore', service=TestServiceDef.sample_service('relational'), nodes=[my_vm1, my_vm2])
    
    @classmethod
    def sample_file_sp(cls) -> ServiceProvider:
        my_vm: ComputeNode = TestServiceProvider.vm('File 001')
        return ServiceProvider(name='File Server', desc='File Server', service=TestServiceDef.sample_service('file'), nodes=[my_vm])
    
    @classmethod
    def sample_vdi_sp(cls) -> ServiceProvider:
        my_vm: ComputeNode = TestServiceProvider.vm('Citrix 001')
        return ServiceProvider(name='VDI', desc='Citrix Server', service=TestServiceDef.sample_service('vdi'), nodes=[my_vm])
    
    @classmethod
    def sample_browser_sp(cls) -> ServiceProvider:
        client: ComputeNode = TestComputeNode.sample_client()
        return ServiceProvider(name='Chrome', desc='PC Workstation', service=TestServiceDef.sample_service('browser'), nodes=[client])
    
    @classmethod
    def sample_pro_sp(cls) -> ServiceProvider:
        client: ComputeNode = TestComputeNode.sample_client()
        return ServiceProvider(name='Pro', desc='Pro Workstation', service=TestServiceDef.sample_service('pro'), nodes=[client])

    @classmethod
    def sample_webgis_sps(cls) -> Set[ServiceProvider]:
        return set([TestServiceProvider.sample_browser_sp(), TestServiceProvider.sample_pro_sp(),
                    TestServiceProvider.sample_vdi_sp(), TestServiceProvider.sample_file_sp(),
                    TestServiceProvider.sample_ha_datastore_sp(), TestServiceProvider.sample_dbms_sp(),
                    TestServiceProvider.sample_map_sp(), TestServiceProvider.sample_portal_sp(),
                    TestServiceProvider.sample_web_sp()])