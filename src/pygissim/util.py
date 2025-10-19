"""
util.py

LibManager loads CSV data from the /data directory.
Bootstraps useful things like hardware definitions.

"""

from typing import Optional, Dict
import csv
import re
from pathlib import Path
from pygissim.engine import *

class LibManager:
    """
    Contains all of the code to load the libraries of hardware, services and workflows.
    """

    # The relative path to the CSV files.
    DATA_PATH: str = 'data'

    @classmethod
    def get_full_path_to_data_file(cls, data_file:str) -> str:
        """ Utility method to resolve the relative path to data files. """
        current_file_path: Path = Path(__file__).resolve()
        current_dir: Path = current_file_path.parent
        return str(current_dir / LibManager.DATA_PATH / data_file)

    def __init__(self) -> None:
        """ Initializes the internal dictionaries. """
        self.hardware: dict[str, HardwareDef] = dict()
        self.service_definitions: dict[str, ServiceDef] = dict()
        self.workflow_steps: dict[str, WorkflowDefStep] = dict()
        self.workflow_chains: dict[str, WorkflowChain] = dict()
        self.workflow_definitions: dict[str, WorkflowDef] = dict()

    def load_local(self):
        """
        Loads all of the dictionaries from CSV.
        The order is important, as the workflow definitions reference the workflow chains, etc.
        """
        self.load_hardware_local()
        self.load_services_local()
        self.load_wf_steps_local()
        self.load_wf_chains_local()
        self.load_wf_defs_local()

    def load_hardware_local(self):
        """ Loads the preconfigured hardware types and SPECint_rate2017 scores. """
        self.hardware.clear()
        file_path: str = LibManager.get_full_path_to_data_file('hardware.csv')
        with open(file_path, mode='r') as file:
            csv_reader = csv.DictReader(file, skipinitialspace=True)
            for row in csv_reader:
                hw: HardwareDef = HardwareDef(row['processor'], 
                                              int(row['cores']), 
                                              float(row['spec']), 
                                              architecture=ComputeArchitecture.from_str(row['architecture']),
                                              threading=ThreadingModel.HYPERTHREADED if row['architecture'] == 'INTEL' else ThreadingModel.PHYSICAL)
                self.hardware[hw.processor] = hw

    def load_services_local(self):
        """ Loads the service definitions. """
        file_path: str = LibManager.get_full_path_to_data_file('services.csv')
        with open(file_path, mode='r') as file:
            csv_reader = csv.DictReader(file, skipinitialspace=True)
            for row in csv_reader:
                sd: ServiceDef = ServiceDef(row['name'], row['description'], row['service_type'], 
                                            balancing_model=BalancingModel.from_str(row['balancing_model']))
                self.service_definitions[sd.service_type] = sd

    def load_wf_steps_local(self):
        """ Loads the performance metrics for individual workflow steps. """
        self.workflow_steps.clear()
        file_path: str = LibManager.get_full_path_to_data_file('workflow_steps.csv')
        with open(file_path, mode='r') as file:
            csv_reader = csv.DictReader(file, skipinitialspace=True)
            for row in csv_reader:
                step: WorkflowDefStep = WorkflowDefStep(row['name'], row['description'], 
                                                        service_type=row['type'], 
                                                        service_time=int(row['st']), 
                                                        chatter=int(row['chatter']),
                                                        request_size_kb=int(row['req_kbytes']),
                                                        response_size_kb=int(row['resp_kbytes']),
                                                        data_source_type=DataSourceType.from_str(row['data_store']),
                                                        cache_pct=int(row['cache_pct']))
                self.workflow_steps[step.name] = step

    def load_wf_chains_local(self):
        """ Loads the chains of steps that make up a request and response. """
        self.workflow_chains.clear()
        file_path: str = LibManager.get_full_path_to_data_file('workflow_chains.csv')
        with open(file_path, mode='r') as file:
            csv_reader = csv.DictReader(file, skipinitialspace=True)
            for row in csv_reader:
                names: list[str] = re.split(r';\s*', row['steps'])
                steps: list[WorkflowDefStep] = list(map(lambda name: (self.workflow_steps[name]), names))
                chain: WorkflowChain = WorkflowChain(row['name'], row['description'], steps, service_providers=dict())
                self.workflow_chains[chain.name] = chain

    def load_wf_defs_local(self):
        """ Loads the groups of chains that make a composite workflow definition. """
        self.workflow_definitions.clear()
        file_path: str = LibManager.get_full_path_to_data_file('workflows.csv')
        with open(file_path, mode='r') as file:
            csv_reader = csv.DictReader(file, skipinitialspace=True)
            for row in csv_reader:
                names: list[str] = re.split(r';\s*', row['chains'])
                chains: list[WorkflowChain] = list(map(lambda name: (self.workflow_chains[name]), names))
                wdef: WorkflowDef = WorkflowDef(row['name'], row['description'], int(row['think']), chains)
                self.workflow_definitions[wdef.name] = wdef

network_list: list[str] = ["Local Only", "Local and ArcGIS Online", "Branch Offices", "Cloudy", "Backhaul Cloudy"]

def load_network(name: str) -> Tuple[list[Zone], list[Connection]]:
    zones: list[Zone] = []
    conns: list[Connection] = []

    lan: Zone = Zone("Local", "Local network", ZoneType.LOCAL)
    lan_local: Connection = lan.self_connect(1000,0)
    
    dmz: Zone = Zone("DMZ", "Edge network", ZoneType.EDGE)
    dmz_local: Connection = dmz.self_connect(1000,0)

    internet: Zone = Zone("Internet", "Internet", ZoneType.INTERNET)
    internet_local: Connection = internet.self_connect(10000,0)

    agol: Zone = Zone("ArcGIS Online", "AWS US West", ZoneType.EDGE)
    agol_local: Connection = agol.self_connect(10000,0)

    cloud: Zone = Zone("Cloud", "Public cloud", ZoneType.LOCAL)
    cloud_local: Connection = cloud.self_connect(1000,0)

    cloud_edge: Zone = Zone("Gateway", "Cloud gateway", ZoneType.EDGE)
    cloud_edge_local: Connection = cloud_edge.self_connect(1000, 0)

    wan1: Zone = Zone("WAN 1", "Branch office 1", ZoneType.LOCAL)
    wan1_local: Connection = wan1.self_connect(100, 0)

    wan2: Zone = Zone("WAN 2", "Branch office 2", ZoneType.LOCAL)
    wan2_local: Connection = wan2.self_connect(100, 0)

    match name.upper():
        case "LOCAL ONLY":
            zones = [lan]
            conns = [lan_local]
        case"LOCAL AND ARCGIS ONLINE":
            zones = [lan, dmz, internet, agol]
            conns = [lan_local, dmz_local, internet_local, agol_local]
            conns.extend(lan.connect_both_ways(dmz, 500, 1))
            conns.append(dmz.connect(internet, 250, 10))
            conns.append(internet.connect(dmz, 500, 10))
            conns.extend(internet.connect_both_ways(agol, 10000, 1))
        case"BRANCH OFFICES":
            zones = [lan, wan1, wan2]
            conns = [lan_local, wan1_local, wan2_local]
            conns.extend(lan.connect_both_ways(wan1, 300, 1))
            conns.extend(lan.connect_both_ways(wan2, 100, 10))
        case"CLOUDY":
            zones = [cloud, cloud_edge, internet, agol]
            conns = [cloud_local, cloud_edge_local, internet_local, agol_local]
            conns.extend(cloud.connect_both_ways(cloud_edge, 1000, 0))
            conns.extend(cloud_edge.connect_both_ways(internet, 1000, 10))
            conns.extend(internet.connect_both_ways(agol, 10000, 1))
        case"BACKHAUL CLOUDY":
            zones = [lan, dmz, internet, agol, cloud, cloud_edge]
            conns = [lan_local, dmz_local, internet_local, agol_local, cloud_local, cloud_edge_local]
            conns.extend(lan.connect_both_ways(dmz, 500, 1))
            conns.append(dmz.connect(internet, 250, 10))
            conns.append(internet.connect(dmz, 500, 10))
            conns.extend(internet.connect_both_ways(agol, 10000, 1))
            conns.extend(cloud.connect_both_ways(cloud_edge, 1000, 0))
            conns.extend(cloud_edge.connect_both_ways(internet, 1000, 10))
            conns.extend(internet.connect_both_ways(agol, 10000, 1))
            conns.extend(lan.connect_both_ways(cloud, 1000, 5))

    return (zones,conns)
