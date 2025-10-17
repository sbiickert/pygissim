from typing import Optional, Dict
import numpy as np
import pandas as pd

from pygissim.engine import *

# .88b  d88. d88888b d888888b d8888b. d888888b  .o88b. .d8888.
# 88'YbdP`88 88'     `~~88~~' 88  `8D   `88'   d8P  Y8 88'  YP
# 88  88  88 88ooooo    88    88oobY'    88    8P      `8bo.  
# 88  88  88 88~~~~~    88    88`8b      88    8b        `Y8b.
# 88  88  88 88.        88    88 `88.   .88.   Y8b  d8 db   8D
# YP  YP  YP Y88888P    YP    88   YD Y888888P  `Y88P' `8888Y'

def _qm_list_to_df(data: list[QueueMetric], queue_type: Optional[str] = None) -> pd.DataFrame:
    df = pd.DataFrame(list(map(lambda qm: ([qm.source, qm.stc_type, qm.clock, qm.channel_count, qm.request_count, qm.utilization()]), data)),
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