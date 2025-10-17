import unittest

from pygissim.engine import *

class TestZone(unittest.TestCase):

    def test_create_zone(self):
        z1: Zone = TestZone.sample_intranet_zone()
        z2: Zone = TestZone.sample_edge_zone()
        self.assertFalse(z1 == z2)
        self.assertEqual("Local", z1.name)
        self.assertEqual(ZoneType.EDGE, z2.type)

    _sample_internet_zone: Optional[Zone] = None
    @classmethod
    def sample_internet_zone(cls) -> Zone:
        if cls._sample_internet_zone is None:
            cls._sample_internet_zone = Zone(name='Internet', desc='Internet Zone', z_type=ZoneType.INTERNET)
        return cls._sample_internet_zone

    _sample_edge_zone: Optional[Zone] = None
    @classmethod
    def sample_edge_zone(cls) -> Zone:
        if cls._sample_edge_zone is None:
            cls._sample_edge_zone = Zone(name='DMZ', desc='Edge Zone', z_type=ZoneType.EDGE)
        return cls._sample_edge_zone

    _sample_intranet_zone: Optional[Zone] = None
    @classmethod
    def sample_intranet_zone(cls) -> Zone:
        if cls._sample_intranet_zone is None:
            cls._sample_intranet_zone = Zone(name='Local', desc='Intranet Zone', z_type=ZoneType.LOCAL)
        return cls._sample_intranet_zone

    _sample_agol_zone: Optional[Zone] = None
    @classmethod
    def sample_agol_zone(cls) -> Zone:
        if cls._sample_agol_zone is None:
            cls._sample_agol_zone = Zone(name='AGOL', desc='ArcGIS Online Zone', z_type=ZoneType.EDGE)
        return cls._sample_agol_zone
    
    _sample_wan_zone: Optional[Zone] = None
    @classmethod
    def sample_wan_zone(cls) -> Zone:
        if cls._sample_wan_zone is None:
            cls._sample_wan_zone = Zone(name='WAN Site', desc='Second Office', z_type=ZoneType.LOCAL)
        return cls._sample_wan_zone

class TestConnection(unittest.TestCase):

    def test_create_conn(self):
        z1: Zone = TestZone.sample_intranet_zone()
        z2: Zone = TestZone.sample_edge_zone()
        c1: Connection = z1.connect(z2, 100, 1)
        self.assertEqual(z1, c1.source)
        self.assertEqual(z2, c1.destination)
        c_inv: Connection = c1.inverted()
        self.assertEqual(z1, c_inv.destination)
        self.assertEqual(z2, c_inv.source)
        self.assertEqual(c1.bandwidth, c_inv.bandwidth)
        c_local: Connection = z1.self_connect(500, 5)
        self.assertEqual(z1, c_local.source)
        self.assertEqual(z1, c_local.destination)
        self.assertEqual(500, c_local.bandwidth)

    @classmethod
    def sample_conn_intranet(cls) -> Connection:
        return TestZone.sample_intranet_zone().self_connect(bw=1000, lat=1)

    @classmethod
    def sample_conn_to_dmz(cls) -> Connection:
        return Connection(source=TestZone.sample_intranet_zone(), 
                          dest=TestZone.sample_edge_zone(), 
                          bw=1000, lat=1)

    @classmethod
    def sample_conn_to_internet(cls) -> Connection:
        return Connection(source=TestZone.sample_edge_zone(), 
                          dest=TestZone.sample_internet_zone(), 
                          bw=100, lat=10)

    @classmethod
    def sample_conn_from_internet(cls) -> Connection:
        return Connection(source=TestZone.sample_internet_zone(), 
                          dest=TestZone.sample_edge_zone(), 
                          bw=500, lat=10)

    @classmethod
    def sample_conn_to_agol(cls) -> Connection:
        return Connection(source=TestZone.sample_internet_zone(), 
                          dest=TestZone.sample_agol_zone(), 
                          bw=1000, lat=10)

class TestRoute1(unittest.TestCase):
    def test_network_find(self):
        z1: Zone = TestZone.sample_intranet_zone()
        z2: Zone = TestZone.sample_edge_zone()
        z3: Zone = TestZone.sample_internet_zone()
        c1:Connection = z1.self_connect(1000, 0)
        c2:Connection = z2.self_connect(1000, 0)
        c3:Connection = z3.self_connect(1000, 0)
        c1_2:Connection = TestConnection.sample_conn_to_dmz()
        c2_1:Connection = c1_2.inverted()
        c2_3:Connection = TestConnection.sample_conn_to_internet()
        c3_2:Connection = TestConnection.sample_conn_from_internet()
        self.assertEqual("DMZ to Local", c2_1.name)
        net: list[Connection] = [c1,c2,c3,c1_2,c2_1,c2_3,c3_2]
        self.assertEqual(c1, z1.local_connection(net))
        self.assertEqual(c2, z2.local_connection(net))
        self.assertEqual(c3, z3.local_connection(net))
        self.assertEqual(1, len(z1.entry_connections(net)))
        z1_exit: Connection = z1.exit_connections(net)[0]
        self.assertEqual(c1_2, z1_exit)
        z4: Zone = Zone("Zone 4", "The fourth zone", ZoneType.INTERNET)
        c3_4:Connection = z3.connect(z4, 13, 1)
        net.append(c3_4)
        self.assertTrue(z4.is_a_destination(net))
        self.assertFalse(z4.is_a_source(net))

    def test_find_route(self):
        z1: Zone = TestZone.sample_intranet_zone()
        z2: Zone = TestZone.sample_edge_zone()
        z3: Zone = TestZone.sample_internet_zone()
        z4: Zone = TestZone.sample_agol_zone()
        c1:Connection = z1.self_connect(100, 0)
        c2:Connection = z2.self_connect(100, 0)
        c3:Connection = z3.self_connect(100, 0)
        c4:Connection = z4.self_connect(100, 0)
        c1_2:Connection = TestConnection.sample_conn_to_dmz()
        c2_1:Connection = c1_2.inverted()
        c2_3:Connection = TestConnection.sample_conn_to_internet()
        c3_2:Connection = TestConnection.sample_conn_from_internet()
        c3_4:Connection = TestConnection.sample_conn_to_agol()
        # c4_3:Connection = c3_4.inverted()
        net: list[Connection] = [c1,c2,c3,c4,c1_2,c2_1,c2_3,c3_2,c3_4]

        r1_4:Optional[Route] = find_route(z1,z4,net)
        self.assertIsNotNone(r1_4)
        if r1_4 is not None:
            self.assertListEqual([c1,c1_2,c2_3,c3_4], r1_4.connections)
        r4_1:Optional[Route] = find_route(z4,z1,net)
        self.assertIsNone(r4_1)
        r3_1:Optional[Route] = find_route(z3,z1,net)
        if r3_1 is not None:
            self.assertListEqual([c3,c3_2,c2_1], r3_1.connections)

class TestRoute2(unittest.TestCase):
    def test_intranet(self):
        net: list[Connection] = TestRoute2.sample_intranet()
        source_z: Zone = TestZone.sample_intranet_zone()
        route: Optional[Route] = find_route(start=source_z, end=source_z, in_network=net)
        self.assertIsNotNone(route)
        if route is not None:
            self.assertEqual(1, len(route.connections))

    def test_network(self):
        net: list[Connection] = TestRoute2.sample_network()
        source_z: Zone = TestZone.sample_intranet_zone()
        dest_z: Zone = TestZone.sample_internet_zone()
        route: Optional[Route] = find_route(start=source_z, end=dest_z, in_network=net)
        self.assertIsNotNone(route)
        if route is not None:
            self.assertEqual(3, len(route.connections))
            self.assertEqual(source_z, route.connections[0].source)
            self.assertEqual(dest_z, route.connections[-1].destination)

    def test_complex_network(self):
        net: list[Connection] = TestRoute2.sample_complex_network()
        source_z: Zone = TestZone.sample_intranet_zone()
        dest_z: Zone = TestZone.sample_agol_zone()
        route: Optional[Route] = find_route(start=source_z, end=dest_z, in_network=net)
        self.assertIsNotNone(route)
        if route is not None:
            self.assertEqual(4, len(route.connections))
            self.assertEqual(source_z, route.connections[0].source)
            self.assertEqual(dest_z, route.connections[-1].destination)
        dest_z = TestZone.sample_wan_zone()
        route: Optional[Route] = find_route(start=source_z, end=dest_z, in_network=net)
        self.assertIsNotNone(route)
        if route is not None:
            self.assertEqual(2, len(route.connections))
            self.assertEqual(source_z, route.connections[0].source)
            self.assertEqual(dest_z, route.connections[-1].destination)

    def test_looping_network(self):
        net: list[Connection] = TestRoute2.sample_looping_network()
       
        route_ac: Optional[Route] = find_route(TestRoute2.zone_a, TestRoute2.zone_c, net)
        self.assertIsNotNone(route_ac)
        if route_ac is not None: self.assertEqual(2, len(route_ac.connections))

        route_ab: Optional[Route] = find_route(TestRoute2.zone_a, TestRoute2.zone_b, net)
        self.assertIsNotNone(route_ab)
        if route_ab is not None: self.assertEqual(2, len(route_ab.connections))

        route_bc: Optional[Route] = find_route(TestRoute2.zone_b, TestRoute2.zone_c, net)
        self.assertIsNotNone(route_bc)
        if route_bc is not None: self.assertEqual(2, len(route_bc.connections))

    @classmethod
    def sample_intranet(cls) -> list[Connection]:
        local: Connection = TestZone.sample_intranet_zone().self_connect(bw=1000, lat=0)
        return [local]
    
    @classmethod
    def sample_network(cls) -> list[Connection]:
        network: list[Connection] = []
        intranet: Zone = TestZone.sample_intranet_zone()
        dmz: Zone = TestZone.sample_edge_zone()
        internet: Zone = TestZone.sample_internet_zone()

        network.append(intranet.self_connect(1000,0))

        network.append(dmz.self_connect(1000,0))
        network.append(TestConnection.sample_conn_to_dmz())
        network.append(TestConnection.sample_conn_to_dmz().inverted())

        network.append(internet.self_connect(1000,10))
        network.append(TestConnection.sample_conn_to_internet())
        network.append(TestConnection.sample_conn_from_internet())

        return network
    
    @classmethod
    def sample_complex_network(cls) -> list[Connection]:
        network: list[Connection] = cls.sample_network()

        intranet: Zone = TestZone.sample_intranet_zone()
        internet: Zone = TestZone.sample_internet_zone()
        wan: Zone = TestZone.sample_wan_zone()
        agol: Zone = TestZone.sample_agol_zone()

        # Add connections to WAN and AGOL
        network.append(wan.self_connect(1000,0))
        network.append(wan.connect(intranet, 300, 7))
        network.append(intranet.connect(wan, 300, 7))

        network.append(agol.self_connect(1000,0))
        network.append(agol.connect(internet, 1000, 10))
        network.append(internet.connect(agol, 1000, 10))

        return network

    zone_a: Zone = Zone('Zone A', desc='', z_type=ZoneType.LOCAL)
    zone_b: Zone = Zone('Zone B', desc='', z_type=ZoneType.LOCAL)
    zone_c: Zone = Zone('Zone C', desc='', z_type=ZoneType.LOCAL)
    
    @classmethod
    def sample_looping_network(cls) -> list[Connection]:
        network: list[Connection] = []

        network.append(cls.zone_a.self_connect(1000,0))
        network.append(cls.zone_b.self_connect(1000,0))
        network.append(cls.zone_c.self_connect(1000,0))

        network.append(cls.zone_a.connect(cls.zone_b,100,1))
        network.append(cls.zone_a.connect(cls.zone_c,100,1))

        network.append(cls.zone_b.connect(cls.zone_a,100,1))
        network.append(cls.zone_b.connect(cls.zone_c,100,1))

        network.append(cls.zone_c.connect(cls.zone_a,100,1))
        network.append(cls.zone_c.connect(cls.zone_b,100,1))

        return network