from typing import Optional, Dict
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from yfiles_jupyter_graphs import GraphWidget


from pygissim.engine import *
from pygissim.pygissim import *

# .88b  d88. d88888b d888888b d8888b. d888888b  .o88b. .d8888.
# 88'YbdP`88 88'     `~~88~~' 88  `8D   `88'   d8P  Y8 88'  YP
# 88  88  88 88ooooo    88    88oobY'    88    8P      `8bo.  
# 88  88  88 88~~~~~    88    88`8b      88    8b        `Y8b.
# 88  88  88 88.        88    88 `88.   .88.   Y8b  d8 db   8D
# YP  YP  YP Y88888P    YP    88   YD Y888888P  `Y88P' `8888Y'

def _qm_list_to_df(data: list[QueueMetric], queue_type: Optional[str] = None) -> pd.DataFrame:
    df = pd.DataFrame(list(map(lambda qm: ([qm.source, qm.stc_type, qm.clock, qm.channel_count, qm.request_count, qm.utilization]), data)),
                      columns=['Source', 'Type', 'Clock', 'Channels', 'Requests', 'Utilization'])
    if queue_type is not None:
        df = df[df['Type'] == queue_type]
    return df

def utilization_for_queues(data: list[QueueMetric], queue_type: Optional[str] = None) -> pd.DataFrame:
    """ Creates a dataframe with a column for each queue and rows are time series. """
    all_metrics = _qm_list_to_df(data, queue_type)

    clocks = all_metrics['Clock'].unique()
    clocks.sort()
    queues = all_metrics['Source'].unique()
    queues.sort()
    columns = np.insert(queues, 0, 'Clock')
    util_df = pd.DataFrame(columns=columns)

    for clock in clocks:
        metrics_for_clock:pd.DataFrame = all_metrics[all_metrics['Clock'] == clock]
        metrics_for_clock.sort_values(by='Source')
        util_values = [clock]
        for index, row in metrics_for_clock.iterrows():
            util_values.append(row.loc['Utilization'])
        util_df.loc[len(util_df)] = util_values

    util_df.set_index('Clock', inplace=True)

    return util_df

def util_stats_for_queues(metrics: list[QueueMetric], queue_type: Optional[str] = None) -> pd.DataFrame:
    all_metrics = _qm_list_to_df(metrics, queue_type)
    queues = all_metrics['Source'].unique()
    queues.sort()
    data = []
    columns = ['Queue', 'Avg']
    percentiles = [5, 50, 75, 95, 99]
    for p in percentiles:
        columns.append(f'p{p}')

    for q in queues:
        q_metrics = all_metrics[all_metrics['Source'] == q]
        q_data = [q]
        mean = np.nanmean(q_metrics['Utilization'])
        q_data.append(mean)
        for p in percentiles:
            p_val = np.percentile(q_metrics['Utilization'], p)
            q_data.append(p_val)
        data.append(q_data)

    return pd.DataFrame(data, columns=columns)


def _rm_list_to_df(data: list[RequestMetric]) -> pd.DataFrame:
    df = pd.DataFrame(list(map(lambda rm: ([rm.source, rm.request_name, rm.workflow_name, rm.clock, \
                                            rm.service_time, rm.queue_time, rm.latency_time, \
                                            rm.service_time + rm.queue_time + rm.latency_time]), data)),
                      columns=['Source', 'Request', 'Workflow', 'Clock', "Service_Time", 'Queue_Time', 'Latency_Time', 'Response_Time'])
    return df
    
def perf_stats_for_requests(metrics: list[RequestMetric]) -> pd.DataFrame:
    """ p stats per Workflow
        Response times: p5 p50 p75 p90 min max mean
        Latency: mean
        Queue times: min max mean

        Assumes that the request metrics are summarized per request, not DEBUG.
    """
    all_metrics = _rm_list_to_df(metrics)
    workflows = all_metrics['Workflow'].unique()
    workflows.sort()
    data = []
    percentiles = [5, 50, 75, 95, 99]
    columns = ["Workflow", "Count", "QT_Min", "QT_Max", "QT_Avg", "Lat_Avg", "ST_Avg", "RT_Avg"]
    for p in percentiles:
        columns.append(f'RT_p{p}')
    
    for w in workflows:
        w_metrics = all_metrics[all_metrics['Workflow'] == w]
        w_data = [w, len(w_metrics)]
        w_data.append(np.nanmin(w_metrics['Queue_Time']))
        w_data.append(np.nanmax(w_metrics['Queue_Time']))
        w_data.append(np.nanmean(w_metrics['Queue_Time']))
        w_data.append(np.nanmean(w_metrics['Latency_Time']))
        w_data.append(np.nanmean(w_metrics['Service_Time']))
        w_data.append(np.nanmean(w_metrics['Response_Time']))
        for p in percentiles:
            w_data.append(np.percentile(w_metrics['Response_Time'], p))
        data.append(w_data)

    return pd.DataFrame(data, columns=columns)

def draw_queue_utilization(df: pd.DataFrame, rolling: bool = False):
    min_val = 0.0
    max_val = 1.0
    
    for col in df.columns:
        if col == 'Clock': continue;
        df[col] = pd.to_numeric(df[col])
        if rolling:
            df[col] = df[col].rolling(3).mean()
        max_val = max(max_val, df[col].max())

    fig, ax = plt.subplots(figsize=(15,7))
    ax.grid(True)
    plt.xlim(df.index.min(), df.index.max())
    plt.ylim(min_val, max_val)

    for col in df.columns:
        if col == 'Clock': continue;
        plt.plot(df.index, df[col], label=col, linewidth=1.75)#, color=SERIES_INFO[vm]['color'])

    plt.legend(loc='upper left')
    plt.title('Utilization')
    plt.show()

# d8888b. d88888b .d8888.  .o88b. d8888b. d888888b d8888b. d88888b
# 88  `8D 88'     88'  YP d8P  Y8 88  `8D   `88'   88  `8D 88'    
# 88   88 88ooooo `8bo.   8P      88oobY'    88    88oooY' 88ooooo
# 88   88 88~~~~~   `Y8b. 8b      88`8b      88    88~~~b. 88~~~~~
# 88  .8D 88.     db   8D Y8b  d8 88 `88.   .88.   88   8D 88.    
# Y8888D' Y88888P `8888Y'  `Y88P' 88   YD Y888888P Y8888P' Y88888P

def describe_workflow(w:Workflow, indent: int = 0) -> str:
    result = [f'{indent * "\t"}{w.name} ({w.description}) type: {w.type} ']
    if w.type == WorkflowType.USER:
        result.append(f'{(indent+1) * "\t"}user count: {w.user_count}; productivity: {w.productivity}; tph: {w.transaction_rate()}')
    else:
        result.append(f'{(indent+1) * "\t"}tph: {w.transaction_rate()}')
    result.append(describe_workflowdef(w.definition, indent + 1))
    return "\n".join(result)

def describe_workflowdef(wdef: WorkflowDef, indent: int = 0) -> str:
    result = [f'{indent * "\t"}{wdef.name} ({wdef.description}):']
    result.append(f'{indent * "\t"}Layers:')
    for chain in wdef.chains:
        result.append(describe_workflowchain(chain, indent + 1))
    return "\n".join(result)

def describe_workflowchain(chain: WorkflowChain, indent: int = 0) -> str:
    result = [f'{indent * "\t"}{chain.name} ({chain.description}):']
    result.append(f'{indent * "\t"}Steps:')
    for step in chain.steps:
        result.append(describe_worflowdefstep(step, indent + 1))
    result.append(f'{indent * "\t"}Service Providers:')
    for service_type, sp in chain.service_providers.items():
        result.append(describe_service_provider(sp, indent + 1))
    return "\n".join(result)

def describe_worflowdefstep(step: WorkflowDefStep, indent: int = 0) -> str:
    return f'{indent * "\t"}{step.name} ({step.description}) type: {step.service_type}' + "\n" + \
        f'{(indent+1) * "\t"}st: {step.service_time} ms; chatter: {step.chatter}; size: {step.request_size_kb}/{step.response_size_kb} kb; data: {step.data_source_type}; cache: {step.cache_pct}%'

def describe_service_provider(sp: ServiceProvider, indent: int = 0) -> str:
    return f'{indent * "\t"}{sp.name} ({sp.description}) service: {sp.service.name}; nodes: [{",".join(map(lambda n: (n.name), sp.nodes))}]'

def zones_to_graph_nodes(zones: list[Zone]) -> list:
    nodes = []
    for zone in zones:
        nodes.append({"id": zone.id, 
                      "properties":{'label': zone.name, 'type': 'Network Zone'}})
    return nodes

def connections_to_graph_edges(net: list[Connection], metrics: Optional[list[QueueMetric]] = None):
    edges = []
    util_stats: Optional[pd.DataFrame] = None
    if metrics is not None:
        util_stats = util_stats_for_queues(metrics, queue_type='CONNECTION')
    for conn in net:
        if util_stats is not None:
            conn_stats = util_stats[util_stats['Queue'] == conn.name].reset_index(drop=True)
            avg = conn_stats.at[0, 'Avg']
        else:
            avg = 0
        edges.append({"id": conn.name, 'start': conn.source.id, 'end': conn.destination.id, 
                      "properties":{'name': conn.name, 'label': f'{conn.bandwidth}/{conn.latency_ms}', 'bw': conn.bandwidth, 'lat': conn.latency_ms, 'util': avg}})
    return edges

def network_edge_styles_mapping(index, edge):
    m = dict()
    m['thickness'] = 4
    return m

def network_node_color_mapping(node: Dict) -> str:
    match node['properties']['type']:
        case 'ZoneType.LOCAL': return '#9999FF'
        case 'ZoneType.EDGE': return '#FF9911'
        case 'ZoneType.INTERNET': return '#DDDDDD'
        case _: return '#000000'

def network_edge_color_mapping(edge: Dict) -> str:
    util: float = edge['properties']['util']
    if util < .01: return '#CCCCCC'
    elif util < .10: return '#0000FF'
    elif util < .25: return '#00FF00'
    elif util < 0.5: return '#DDDD00'
    elif util < 0.9: return '#FFA500'
    else: return '#FF0000'

def draw_network(zones: list[Zone], connections: list[Connection]) -> GraphWidget:
    w = GraphWidget()
    w.nodes = zones_to_graph_nodes(zones)
    w.edges = connections_to_graph_edges(connections)
    w.directed = True
    w.graph_layout = 'orthogonal'
    w.set_node_color_mapping(network_node_color_mapping)
    w.set_edge_color_mapping(network_edge_color_mapping)
    w.default_edge_styles_mapping = network_edge_styles_mapping
    return w

def compute_to_graph(c_nodes: list[ComputeNode]) -> Tuple[list, list]:
    nodes = []
    edges = []
    for c in c_nodes:
        if c.type == ComputeNodeType.V_SERVER: continue
        # print(f'adding {c.name}')
        nodes.append({"id": c.name, 
                      "properties":{'label': c.name, 'type': str(c.type)}})
        edges.append({"id": f'{c.name}-{c.zone.id}', 'start': c.name, 'end': c.zone.id})
        if c.type == ComputeNodeType.P_SERVER:
            for v in c._v_hosts:
                # print(f'adding {v.name}')
                nodes.append({"id": v.name, 
                              "properties":{'label': v.name, 'type': str(v.type)}})
                edges.append({"id": f'{c.name}-{v.name}', 'start': c.name, 'end': v.name})
    return (nodes, edges)

def sp_to_graph(sps: list[ServiceProvider]) -> Tuple[list, list]:
    nodes = []
    edges = []

    for sp in sps:
        nodes.append({"id": sp.name, 
                      "properties":{'label': sp.name, 'type': 'Service Provider', 'service_type': sp.service.name}})
        for node in sp.nodes:
            edges.append({"id": f'{sp.name}-{node.name}', 'start':sp.name, 'end':node.name})

    return (nodes, edges)

def workflows_to_graph(workflows:list[Workflow]) -> Tuple[list, list]:
    nodes:Dict = dict()
    edges:Dict = dict()

    for w in workflows:
        nodes[w.name] = {"id": w.name, "properties":{'label': w.name, 'type': 'Workflow'}}
        wdef = w.definition
        nodes[wdef.name] = {"id": wdef.name, "properties":{'label': wdef.name, 'type': 'Definition'}}
        e_id = f'{w.name}-{wdef.name}'
        if e_id not in edges.keys():
            edges[e_id] = {"id": e_id, "start": w.name, "end": wdef.name, 'properties': {'wf': w.name}}
        for chain in wdef.chains:
            if chain.name not in nodes.keys():
                nodes[chain.name] = {"id": chain.name, "properties":{'label': chain.name, 'type': 'Chain'}}
            e_id = f'{wdef.name}-{chain.name}'
            if e_id not in edges.keys():
                edges[e_id] = {"id": e_id, "start": wdef.name, "end": chain.name, 'properties': {'wf': w.name}}
            if len(chain.steps) == 0: continue
            step = chain.steps[0]
            if step.name not in nodes.keys():
                nodes[step.name] = {"id": f'{step.name}', 
                                    "properties":{'label': step.name,
                                                  'type': f'{step.service_type} step',
                                                  'st': step.service_time,
                                                  'response size': f'{step.response_size_kb} kB'}}
            e_id = f'{chain.name}-{nodes[step.name]}'
            if e_id not in edges.keys():
                edges[e_id] =  {"id": e_id, "start": chain.name, "end": step.name, 'properties': {'wf': w.name}}
            for i in range(1, len(chain.steps)):
                prev_step = chain.steps[i-1]
                step = chain.steps[i]
                if step.name not in nodes.keys():
                    nodes[step.name] = {"id": step.name, 
                                        "properties":{'label': step.name, 
                                                      'type': f'{step.service_type} step',
                                                      'st': step.service_time,
                                                      'response size': f'{step.response_size_kb} kB' }}
                e_id = f'{prev_step.name}-{step.name}'
                if e_id not in edges.keys():
                    edges[e_id] = {"id": e_id, "start": prev_step.name, "end": step.name, 'properties': {'wf': w.name}}

    return (list(nodes.values()), list(edges.values()))

def zone_compute_sp_node_color_mapping(node: Dict) -> str:
    if 'ZoneType' in node['properties']['type']:
        return '#FF9911'
    if node['properties']['type'] == str(ComputeNodeType.CLIENT):
        return '#9900CC'
    elif node['properties']['type'] == str(ComputeNodeType.P_SERVER):
        return '#000099'
    elif node['properties']['type'] == str(ComputeNodeType.V_SERVER):
        return '#9999CC'
    else:
        return '#333333'
    
def wf_node_color_mapping(node: Dict) -> str:
    match node['properties']['type']:
        case 'Workflow': return '#9999FF'
        case 'Definition': return '#FF9911'
        case 'Chain': return '#DDDDDD'
        case 'Step': return '#999999'
        case _: return '#FFFFFF'

def wf_edge_color_mapping(edge: Dict) -> str:
    match edge['properties']['wf']:
        case 'Web': return '#0000FF'
        case 'Mobile': return '#00FF00'
        case _: return '#333333'

def draw_zone_compute(d: Design) -> GraphWidget:
    w = GraphWidget()

    zone_nodes = zones_to_graph_nodes(d.zones)
    compute_nodes, compute_edges = compute_to_graph(d.compute_nodes())
    w.nodes = zone_nodes + compute_nodes
    w.edges = compute_edges
    w.set_node_color_mapping(zone_compute_sp_node_color_mapping)
    w.graph_layout = 'organic'
    return w


def draw_compute_sp(d: Design) -> GraphWidget:
    w = GraphWidget()

    compute_nodes, compute_edges = compute_to_graph(d.compute_nodes())
    sp_nodes, sp_edges = sp_to_graph(d.service_providers)
    w.nodes = compute_nodes + sp_nodes
    w.edges = compute_edges + sp_edges
    w.set_node_color_mapping(zone_compute_sp_node_color_mapping)
    w.graph_layout = 'radial'
    return w

def draw_workflows(workflows: list[Workflow]) -> GraphWidget:
    w = GraphWidget()

    w_nodes, w_edges = workflows_to_graph(workflows)
    w.nodes = w_nodes
    w.edges = w_edges
    w.set_node_color_mapping(wf_node_color_mapping)
    w.set_edge_color_mapping(wf_edge_color_mapping)
    w.graph_layout = 'hierarchic'

    return w

def create_service_provider(d: Design, name: str, service: str, node_names: list[str], tags: Optional[Set[str]]):
    nodes: list[ComputeNode] = []
    for node_name in node_names:
        node = d.get_compute_node(node_name)
        if node is not None:
            nodes.append(node)
    d.add_service_provider(ServiceProvider(name, "", service=d.services[service], nodes=nodes, tags=tags))


def create_agol_service_providers(d: Design, agol_zone_name: str):
    sp_agol: list[ServiceProvider] = list()
    agol: Optional[Zone] = d.get_zone(agol_zone_name)
    if agol is None: raise ValueError(f'No zone named "{agol_zone_name} found in design.')
    servers: list[ComputeNode] = list(filter(lambda node: (node.zone == agol), d.compute_nodes()))
    sp_agol.append(ServiceProvider(name='AGOL Edge', desc='', service=d.services['web'], nodes=servers, tags={'agol'}))
    sp_agol.append(ServiceProvider(name='AGOL Portal', desc='', service=d.services['portal'], nodes=servers, tags={'agol'}))
    sp_agol.append(ServiceProvider(name='AGOL GIS', desc='', service=d.services['feature'], nodes=servers, tags={'agol'}))
    sp_agol.append(ServiceProvider(name='AGOL Basemap', desc='', service=d.services['map'], nodes=servers, tags={'agol'}))
    sp_agol.append(ServiceProvider(name='AGOL DB', desc='', service=d.services['relational'], nodes=servers, tags={'agol'}))
    sp_agol.append(ServiceProvider(name='AGOL File', desc='', service=d.services['file'], nodes=servers, tags={'agol'}))

    for sp in sp_agol:
        d.add_service_provider(sp)