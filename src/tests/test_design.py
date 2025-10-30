import unittest
from typing import Optional, Tuple

from pygissim.util import *
from pygissim.pygissim import *

from tests.test_network import *
from tests.test_compute import *
from tests.test_work import *

class TestDesign(unittest.TestCase):
    def test_create(self):
        d: Design = TestDesign.sample_design()
        self.assertEqual('Design 1', d.name)
        self.assertEqual(4, len(d.zones))
        dmz: Optional[Zone] = d.get_zone('DMZ')
        self.assertIsNotNone(dmz)
        if dmz is None: return
        self.assertTrue(dmz.is_fully_connected(in_network=d.network))

        self.assertEqual(7, len(d.compute_nodes())) # physical hosts, v hosts and clients
        local_host: Optional[ComputeNode] = d.get_compute_node('SRV01')
        self.assertIsNotNone(local_host)
        if local_host is None: return
        self.assertEqual(16, local_host.total_vcpu_allocation())
        self.assertEqual(8, local_host.total_cpu_allocation())
        gis_host: Optional[ComputeNode] = d.get_compute_node('VGIS01')
        self.assertIsNotNone(gis_host)
        if gis_host is None: return
        self.assertEqual(8, gis_host.vcore_count())
        self.assertEqual(ComputeNodeType.V_SERVER, gis_host.type)

        self.assertEqual(2, len(d.workflow_definitions))
        self.assertEqual(2, len(d._workflows))

        d.print_validation_messages()
        self.assertTrue(d.is_valid())

    def test_update_nodes(self):
        d: Design = TestDesign.sample_design()
        for node in d.compute_nodes():
            node.description = "updated"

        gis: Optional[ComputeNode] = d.get_compute_node('VGIS01')
        self.assertIsNotNone(gis)
        if gis is not None:
            self.assertEqual('updated', gis.description)
        self.assertEqual('updated', d.service_providers[0].nodes[0].description)
        sp: Optional[ServiceProvider] = d._workflows[0].definition.chains[0].service_provider_for_step_at_index(0)
        self.assertIsNotNone(sp)
        if sp is not None:
            self.assertEqual('updated', sp.nodes[0].description)

    def test_remove_zone(self):
        d: Design = TestDesign.sample_design()
        self.assertTrue(d.is_valid())
        self.assertEqual(4, len(d._compute_nodes)) # 2 clients and 2 physical servers
        self.assertEqual(10, len(d.network))
        mobile_wdef: Optional[WorkflowDef] = d.get_workflowdef('Mobile Map Definition')
        self.assertIsNotNone(mobile_wdef)
        if mobile_wdef is None: return
        self.assertEqual(7, len(mobile_wdef.chains[0].service_providers))
        self.assertLess(0, len(mobile_wdef.chains[0].service_providers['feature'].nodes))

        agol_z: Optional[Zone] = d.get_zone("AGOL")
        self.assertIsNotNone(agol_z)
        if agol_z is None: return
        d.remove_zone(agol_z)
        
        self.assertFalse(d.is_valid())
        self.assertEqual(3, len(d._compute_nodes)) # 2 clients and 1 physical server
        self.assertEqual(7, len(d.network))
        self.assertEqual(7, len(mobile_wdef.chains[0].service_providers))
        for st in mobile_wdef.chains[0].configured_service_types():
            if st != 'mobile': self.assertEqual(0, len(mobile_wdef.chains[0].service_providers[st].nodes))

        # Re-point mobile workflow at local service providers
        remaining: list[ServiceProvider] = []
        for sp in d.service_providers:
            if "AGOL" not in sp.name:
                remaining.append(sp)
        d.service_providers = remaining
        d.update_workflow_definitions()

        for sp in d.service_providers:
            mobile_wdef.assign_service_provider(sp)


        self.assertTrue(d.is_valid())
        #d.print_validation_messages()


    @classmethod
    def sample_design(cls) -> Design:
        d: Design = Design(name=Design.next_name(), desc='Sample design')

        # Zones and Connections
        d.add_zone(TestZone.sample_intranet_zone(), 1000, 0)
        d.add_zone(TestZone.sample_edge_zone(), 1000, 0)
        d.add_zone(TestZone.sample_internet_zone(), 10000, 10)
        d.add_zone(TestZone.sample_agol_zone(), 10000, 0)

        d.add_connection(TestConnection.sample_conn_to_dmz(), add_reciprocal=True)
        d.add_connection(TestConnection.sample_conn_to_internet(), add_reciprocal=True)
        d.add_connection(TestConnection.sample_conn_to_agol(), add_reciprocal=True)

        # Physical Servers
        lz: Optional[Zone] = d.get_zone('Local')
        az: Optional[Zone] = d.get_zone('AGOL')
        if lz is None or az is None:
            raise ValueError('Could not get Intranet and AGOL from the network')
        local_host: ComputeNode = ComputeNode(name='SRV01', desc='Local server', 
                                              hw_def=TestHWDef.sample_server_hw_def(),
                                              memory_GB=48, zone=lz,
                                              type=ComputeNodeType.P_SERVER)
        agol_host: ComputeNode = ComputeNode(name='AGOL01', desc='AWS server',
                                             hw_def=TestHWDef.sample_server_hw_def(),
                                             memory_GB=64, zone=az,
                                             type=ComputeNodeType.P_SERVER)
        d.add_compute(local_host)
        d.add_compute(agol_host)

        # Virtual Servers
        local_host.add_virtual_host('VWEB01', 4, memory_GB=16)
        local_host.add_virtual_host('VGIS01', 8, memory_GB=32)
        local_host.add_virtual_host('VDB01', 4, memory_GB=16)

        # Clients
        local_client: ComputeNode = TestComputeNode.sample_client()
        d.add_compute(local_client)
        mobile_client: ComputeNode = TestComputeNode.sample_mobile()
        d.add_compute(mobile_client)

        # Services
        for s_type in TestServiceDef.sample_service_types():
            d.add_servicedef(TestServiceDef.sample_service(s_type))
        
        # Service Providers (Local)
        sp_local: list[ServiceProvider] = list()
        sp_local.append(ServiceProvider(name='Web browser', desc='', service=d.services['browser'], nodes=[local_client]))
        sp_local.append(ServiceProvider(name='Pro workstation', desc='', service=d.services['pro'], nodes=[local_client]))
        vm_web: Optional[ComputeNode] = d.get_compute_node('VWEB01')
        vm_gis: Optional[ComputeNode] = d.get_compute_node('VGIS01')
        vm_db:  Optional[ComputeNode] = d.get_compute_node('VDB01')
        if vm_web is None or vm_gis is None or vm_db is None:
            raise ValueError('Could not get web, gis and db servers from design.')
        sp_local.append(ServiceProvider(name='IIS', desc='', service=d.services['web'], nodes=[vm_web]))
        sp_local.append(ServiceProvider(name='Portal', desc='', service=d.services['portal'], nodes=[vm_gis]))
        sp_local.append(ServiceProvider(name='Map server', desc='', service=d.services['map'], nodes=[vm_gis]))
        sp_local.append(ServiceProvider(name='Hosting server', desc='', service=d.services['feature'], nodes=[vm_gis]))
        sp_local.append(ServiceProvider(name='Datastore', desc='', service=d.services['relational'], nodes=[vm_gis]))
        sp_local.append(ServiceProvider(name='SQL', desc='', service=d.services['dbms'], nodes=[vm_db]))
        sp_local.append(ServiceProvider(name='Local File', desc='', service=d.services['file'], nodes=[vm_gis]))

        for sp in sp_local:
            d.add_service_provider(sp)
        
        # Service Providers (AGOL)
        sp_agol: list[ServiceProvider] = list()
        sp_agol.append(ServiceProvider(name='Field Maps', desc='', service=d.services['mobile'], nodes=[mobile_client]))
        srv_agol: Optional[ComputeNode] = d.get_compute_node('AGOL01')
        if srv_agol is None:
            raise ValueError('Could not get AGOL server from design.')
        sp_agol.append(ServiceProvider(name='AGOL Edge', desc='', service=d.services['web'], nodes=[srv_agol]))
        sp_agol.append(ServiceProvider(name='AGOL Portal', desc='', service=d.services['portal'], nodes=[srv_agol]))
        sp_agol.append(ServiceProvider(name='AGOL GIS', desc='', service=d.services['feature'], nodes=[srv_agol]))
        sp_agol.append(ServiceProvider(name='AGOL Basemap', desc='', service=d.services['map'], nodes=[srv_agol]))
        sp_agol.append(ServiceProvider(name='AGOL DB', desc='', service=d.services['relational'], nodes=[srv_agol]))
        sp_agol.append(ServiceProvider(name='AGOL File', desc='', service=d.services['file'], nodes=[srv_agol]))
    
        for sp in sp_agol:
            d.add_service_provider(sp)

        # Workflow Definitions
        d.add_workflowdef(TestWorkflowDef.sample_web_wfdef())
        for sp in sp_local:
            d.workflow_definitions[0].assign_service_provider(sp)
        d.add_workflowdef(TestWorkflowDef.sample_mobile_wfdef())
        for sp in sp_agol:
            d.workflow_definitions[1].assign_service_provider(sp)

        # Workflows
        d.add_transactional_workflow('Web', desc='', wdef_name='Web Map Definition', tph=1000)
        d.add_client_workflow('Mobile', desc='', wdef_name='Mobile Map Definition', users=15, productivity=6)

        return d
