import ipywidgets as widgets
from IPython.display import display

from typing import Optional, Dict
from pygissim.engine import *
from pygissim.pygissim import *

def edit_design(d:Optional[Design]) -> Design:
    _d:Design = Design("", "") if d is None else d

    name = widgets.Text(value=_d.name, placeholder="Enter Name", description="Name:")
    desc = widgets.Text(value=_d.description, placeholder="Enter Description", description="Description:")
    close = widgets.Button(description='Close')
    dialog:widgets.VBox = widgets.VBox([name,desc,close])

    name.observe(lambda evt: _d.set_name(evt['new']), names='value')
    desc.observe(lambda evt: _d.set_desc(evt['new']), names='value')
    close.on_click(lambda b: dialog.close())

    display(dialog)
    
    return _d


def edit_zone(z:Optional[Zone]) -> Zone:
    _z:Zone = Zone("", "") if z is None else z

    name = widgets.Text(value=_z.name, placeholder="Enter Name", description="Name:")
    desc = widgets.Text(value=_z.description, placeholder="Enter Description", description="Description:")
    close = widgets.Button(description='Close')
    dialog:widgets.VBox = widgets.VBox([name,desc,close])

    name.observe(lambda evt: _z.set_name(evt['new']), names='value')
    desc.observe(lambda evt: _z.set_description(evt['new']), names='value')
    close.on_click(lambda b: dialog.close())

    display(dialog)
    return _z

def design_add_zone(d:Design, z:Zone) -> Zone:
    bw = widgets.BoundedIntText(min=0, max=10000, step=50, description="Local Bandwidth (Mbps):")
    lat = widgets.BoundedIntText(min=0, max=1000, step=1, description="Latency (ms):")
    close = widgets.Button(description='Close')
    dialog:widgets.VBox = widgets.VBox([bw,lat,close])

    c_opt = d.add_zone(zone=z, local_bw_mbps=1000, local_latency_ms=0)
    if c_opt is None: return z
    conn: Connection = c_opt
    bw.observe(lambda evt: conn.set_bandwidth(evt['new']), names='value')
    lat.observe(lambda evt: conn.set_latency(evt['new']), names='value')
    close.on_click(lambda b: dialog.close())

    display(dialog)
    return z

def design_add_connection(d:Design) -> Optional[Connection]:
    if len(d.zones) < 1: return None

    options = list(map(lambda z: (z.name, z) , d.zones))
    src = widgets.Dropdown(options=options, value=d.zones[0], description="Source Zone:")
    dst = widgets.Dropdown(options=options, value=d.zones[-1], description="Destination Zone:")
    bw = widgets.BoundedIntText(min=0, max=10000, step=50, description="Local Bandwidth (Mbps):")
    lat = widgets.BoundedIntText(min=0, max=1000, step=1, description="Latency (ms):")
    close = widgets.Button(description='Close')
    dialog:widgets.VBox = widgets.VBox([src, dst, bw,lat,close])

    conn: Connection = Connection(d.zones[0], d.zones[-1])
    d.add_connection(conn)
    src.observe(lambda evt: conn.set_source(evt['new']), names='value')
    dst.observe(lambda evt: conn.set_destination(evt['new']), names='value')
    bw.observe(lambda evt: conn.set_bandwidth(evt['new']), names='value')
    lat.observe(lambda evt: conn.set_latency(evt['new']), names='value')
    close.on_click(lambda b: dialog.close())
   
    display(dialog)
    return conn

def _update_dialog_value(conn:Connection, bw:widgets.BoundedIntText, lat:widgets.BoundedIntText):
    bw.value = conn.bandwidth
    lat.value = conn.latency_ms

def design_edit_connections(d:Design):
    if len(d.network) < 1: return
    options = list(map(lambda conn: (conn.name, conn) , d.network))
    choose = widgets.Dropdown(options=options, value=d.network[0], description="Connection:")
    bw:widgets.BoundedIntText = widgets.BoundedIntText(min=0, max=10000, step=50, description="Local Bandwidth (Mbps):")
    lat = widgets.BoundedIntText(min=0, max=1000, step=1, description="Latency (ms):")
    close = widgets.Button(description='Close')
    dialog:widgets.VBox = widgets.VBox([choose, bw,lat,close])
    _update_dialog_value(d.network[0], bw, lat)

    choose.observe(lambda evt: _update_dialog_value(evt['new'], bw, lat), names='value')
    bw.observe(lambda evt: choose.value.set_bandwidth(evt['new']), names='value')
    lat.observe(lambda evt: choose.value.set_latency(evt['new']), names='value')
    close.on_click(lambda b: dialog.close())

    display(dialog)
