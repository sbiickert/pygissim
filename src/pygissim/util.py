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
                self.service_definitions[sd.name] = sd

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

