import type {ObsessStatus} from "./obsess-status";
import {obsessLabel} from "./obsess-status";
import "./overview.css";

type Recent={scheduled:number,attempted:number,sent:number,settled:number,pending:number,on_time:number,late:number,unanswered:number,send_errors:number,scheduler_missed:number,rtt_count:number,p50_ms?:number,p95_ms?:number,deadline_miss_pct:number|null,no_reply_pct:number|null,partial:boolean};
type Trend={from_ms:number,to_ms:number,p50_ms?:number,p95_ms?:number,sent:number,settled:number,late:number,unanswered:number,send_errors:number,scheduler_missed:number,pending:number};
export type OverviewTarget={id:string,name:string,address:string,endpoint?:string,state:string,interval_ms:number,timeout_ms:number,last_scheduled_at_ms?:number,last_rtt_ms?:number,obsess?:ObsessStatus,recent:Recent,trend:Trend[],route:{traces:number,reached:number,unreached:number,errors:number,changes:number,last_trace_at_ms?:number,last_change_at_ms?:number}};
type HostInterface={name:string,display_name:string,present:boolean,last_at_ms:number,peak_rx_mbps?:number,peak_tx_mbps?:number,has_deltas:boolean,partial:boolean,has_errors:boolean,has_drops:boolean,resets:number,rx_errors:number,tx_errors:number,rx_dropped:number,tx_dropped:number,rx_missed:number,rx_fifo:number,tx_fifo:number,rx_crc:number,rx_frame:number,tx_carrier:number,collisions:number};
export type Overview={as_of_ms:number,from_ms:number,to_ms:number,window_ms:number,targets:OverviewTarget[],interfaces:HostInterface[]};
export type MonitorStatus={ready:boolean,pressure:string,live_bytes:number,dirty_buckets:number,available_bytes:number,writer_error?:string,trace_capabilities?:Record<string,string>,interface_capability?:string};
type Filter="all"|"signals"|"deadline"|"obsess"|"local"|"missing";
const esc=(value:string|number)=>String(value).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]!));
const n=(value:number)=>value.toLocaleString();
const ms=(value:number|undefined)=>value==null?"—":`${Number(value.toFixed(2))} ms`;
const pct=(value:number|null)=>value==null?"—":`${Number(value.toFixed(2))}%`;
const when=(value:number)=>new Date(value).toLocaleString();
const age=(value:number|undefined,now:number)=>{if(value==null||value<=0)return "No observation";const seconds=Math.max(0,Math.floor((now-value)/1000));return seconds<60?`${seconds}s ago`:seconds<3600?`${Math.floor(seconds/60)}m ago`:`${Math.floor(seconds/3600)}h ago`};
const deadline=(t:OverviewTarget)=>t.recent.late+t.recent.unanswered>0;
const local=(t:OverviewTarget)=>t.recent.send_errors+t.recent.scheduler_missed>0;
const obsessing=(t:OverviewTarget)=>!!t.obsess?.monitoring&&t.obsess.state==="obsessing";
const missing=(t:OverviewTarget)=>t.state==="unknown"||t.state==="stale";
const signal=(t:OverviewTarget)=>["down","late","send_error","scheduler_gap"].includes(t.state)||t.route.errors>0||deadline(t)||local(t)||obsessing(t)||missing(t)||t.route.changes>0||!!t.obsess?.tracking_limited;
const outcome:Record<string,string>={up:"Replied on time",down:"No reply by deadline",late:"Late reply",send_error:"Local send error",scheduler_gap:"Scheduler gap",stale:"Stale observation",unknown:"No observation",pending:"Awaiting reply"};
const stateTone=(state:string)=>state==="down"?"bad":state==="late"?"warm":state==="up"?"good":"muted";
export function detailLink(id:string|undefined,data:Pick<Overview,"from_ms"|"to_ms">,focus?:string){const params=new URLSearchParams();if(id!=null)params.set("target",id);else params.set("view","details");params.set("from",String(data.from_ms));params.set("to",String(data.to_ms));if(focus)params.set("focus",focus);return `#${params}`}

function trend(t:OverviewTarget,data:Overview){
  const points=t.trend,maximum=Math.max(1,...points.map(p=>p.p95_ms??p.p50_ms??0));
  let median="",tail="",previous=false;
  const bars:string[]=[];
  points.forEach((p,i)=>{
    const left=4+(p.from_ms-data.from_ms)/(data.to_ms-data.from_ms)*172,right=4+(p.to_ms-data.from_ms)/(data.to_ms-data.from_ms)*172,x=(left+right)/2,has=p.p50_ms!=null;
    if(has){median+=`${previous?"L":"M"}${x.toFixed(1)},${(26-(p.p50_ms!/maximum)*23).toFixed(1)} `;tail+=`${previous?"L":"M"}${x.toFixed(1)},${(26-((p.p95_ms??p.p50_ms!)/maximum)*23).toFixed(1)} `;bars.push(`<circle cx="${x}" cy="${26-p.p50_ms!/maximum*23}" r="1.3" class="trend-dot"/>`)}
    previous=has;
    const color=p.unanswered>0?"trend-loss":p.late>0?"trend-late":p.send_errors+p.scheduler_missed>0?"trend-local":has?"trend-observed":"trend-empty";
    bars.push(`<rect x="${left}" y="31" width="${Math.max(.1,right-left-.5)}" height="3" class="${color}"/>`);
  });
  return `<svg class="overview-trend" viewBox="0 0 180 37" role="img" aria-label="Median and p95 latency trend, independently scaled to ${esc(ms(maximum))}. Gaps have no measured RTT; lower marks show unanswered, late, or local events."><path d="${tail}" class="trend-tail"/><path d="${median}" class="trend-median"/>${bars.join("")}</svg>`;
}

export class OverviewView {
  private data:Overview|null=null;
  private controller:AbortController|null=null;
  private visible=false;
  private window="15m";
  private filter:Filter="all";
  private query="";
  private sort="name";
  private failed=false;
  private receivedAt=0;
  private monitor:MonitorStatus|null=null;
  private monitorFailed=false;
  constructor(private root:HTMLElement,private onTargets:(targets:OverviewTarget[])=>void){
    root.innerHTML=`<div class="overview-heading"><div><p class="eyebrow">NETWORK OBSERVATIONS</p><h2>Overview</h2><p class="overview-description">Spot changes. Follow the evidence.</p></div><div class="overview-window"><label for="overview-window">Recent window</label><select id="overview-window"><option value="5m">Last 5 minutes</option><option value="15m" selected>Last 15 minutes</option><option value="1h">Last hour</option></select></div></div><p id="overview-freshness" role="status">Loading recent observations…</p><div id="overview-counts" class="overview-counts"></div><p class="overview-note">Counts refer to targets; categories can overlap. Latest observations and window totals are shown separately.</p><section class="panel overview-target-panel" aria-labelledby="overview-target-title"><div class="overview-table-tools"><div><h3 id="overview-target-title">Targets</h3><p id="overview-result-count" class="overview-note"></p></div><div class="overview-controls"><label class="search-label" for="overview-search">Find a target<input id="overview-search" type="search" placeholder="Name, ID or address" autocomplete="off"></label><label for="overview-filter">Show<select id="overview-filter"><option value="all">All targets</option><option value="signals">With signals</option><option value="deadline">Deadline misses</option><option value="obsess">Obsessing</option><option value="local">Local issues</option><option value="missing">Stale / no data</option></select></label><label for="overview-sort">Sort by<select id="overview-sort"><option value="name">Name</option><option value="deadline">Deadline miss %</option><option value="latency">p95 RTT</option></select></label></div></div><div id="overview-table"></div><p class="overview-note overview-key">RTT trends use a separate scale per target. <span class="key-amber">Amber: latency / late replies.</span> <span class="key-red">Red: no reply.</span> Gray: local events or no RTT.</p><details class="overview-semantics"><summary>How to read these observations</summary><p>Deadline misses include late replies and probes still unanswered after their timeout. Percentages divide by successfully sent probes; pending replies make a window provisional. Local send errors and skipped schedules are separate from network loss. RTT percentiles include measured late replies.</p><p>A latest missed deadline is one probe observation. An empty interval or missing trace is not evidence of a healthy connection. Obsessing describes faster probing against the target’s configured trigger. Target links open this fixed time window in the detailed charts.</p></details></section><div class="overview-context"><section class="panel" aria-labelledby="overview-interface-title"><div class="panel-title"><h3 id="overview-interface-title">Host interfaces</h3><span>Same recent window</span></div><p class="overview-note">Local counter observations; these do not establish which interface a target uses.</p><div id="overview-interfaces"></div></section><section class="panel" aria-labelledby="overview-monitor-title"><div class="panel-title"><h3 id="overview-monitor-title">Monitor</h3><span>Collection &amp; storage</span></div><div id="overview-monitor">Loading monitor status…</div></section></div>`;
    this.el<HTMLSelectElement>("#overview-window").onchange=e=>{this.window=(e.target as HTMLSelectElement).value;this.controller?.abort();this.controller=null;this.data=null;this.failed=false;this.render();void this.load()};
    this.el<HTMLInputElement>("#overview-search").oninput=e=>{this.query=(e.target as HTMLInputElement).value.trim().toLowerCase();this.renderTable()};
    this.el<HTMLSelectElement>("#overview-filter").onchange=e=>{this.filter=(e.target as HTMLSelectElement).value as Filter;this.renderTable();this.renderCounts()};
    this.el<HTMLSelectElement>("#overview-sort").onchange=e=>{this.sort=(e.target as HTMLSelectElement).value;this.renderTable()};
  }
  private el<T extends HTMLElement=HTMLElement>(selector:string){return this.root.querySelector<T>(selector)!}
  setVisible(value:boolean){this.visible=value;if(!value){this.controller?.abort();this.controller=null}}
  setMonitor(status:MonitorStatus|null,failed=false){this.monitor=status;this.monitorFailed=failed;this.renderMonitor()}
  async load(){
    if(!this.visible||this.controller)return;
    const controller=new AbortController();this.controller=controller;
    const timeout=setTimeout(()=>controller.abort(),10000);
    this.root.setAttribute("aria-busy","true");
    try{
      const response=await fetch(`/api/v1/overview?window=${this.window}`,{headers:{accept:"application/json"},signal:controller.signal});
      if(!response.ok)throw new Error("Overview unavailable");
      const data:Overview=await response.json();
      if(this.controller!==controller||!this.visible)return;
      this.data=data;this.receivedAt=Date.now();this.failed=false;this.onTargets(data.targets);this.render();
    }catch{
      if(this.controller!==controller||!this.visible)return;
      this.failed=true;this.render();
    }finally{clearTimeout(timeout);if(this.controller===controller){this.controller=null;this.root.removeAttribute("aria-busy")}}
  }
  private render(){
    const data=this.data,freshness=this.el("#overview-freshness");
    this.root.dataset.asOf=data?String(data.as_of_ms):"";this.root.dataset.loaded=String(!!data);this.root.dataset.failed=String(this.failed);
    freshness.className=this.failed?"overview-refresh-error":"";
    freshness.textContent=this.failed?(data?`Refresh failed — previous observations from ${when(data.as_of_ms)} remain shown. Retrying…`:"Could not load the overview. Retrying…"):data?`${when(data.from_ms)} – ${when(data.to_ms)} · Updated ${new Date(data.as_of_ms).toLocaleTimeString()} · Refreshes every 5 seconds`:"Loading recent observations…";
    this.renderCounts();this.renderTable();this.renderInterfaces();
  }
  private renderCounts(){
    const data=this.data,targets=data?.targets??[];
    const cards:[Filter,string,number,string][]=[["all","Configured targets",targets.length,"in this monitor"],["deadline","Deadline misses",targets.filter(deadline).length,"targets in this window"],["obsess","Obsessing",targets.filter(obsessing).length,"targets probing faster now"],["local","Local issues",targets.filter(local).length,"targets with errors / gaps"],["missing","Stale / no data",targets.filter(missing).length,"targets without fresh observations"]];
    const active=document.activeElement as HTMLElement|null,focus=active?.dataset.overviewCount;
    this.el("#overview-counts").innerHTML=cards.map(([key,label,count,note])=>`<button class="overview-count ${key===this.filter?"selected":""} ${count&&key!=="all"?key:""}" data-overview-count="${key}" aria-pressed="${key===this.filter}"><span>${label}</span><strong>${data?n(count):"—"}</strong><small>${note}</small></button>`).join("");
    this.el("#overview-counts").querySelectorAll<HTMLButtonElement>("button").forEach(button=>{button.onclick=()=>{this.filter=button.dataset.overviewCount as Filter;this.el<HTMLSelectElement>("#overview-filter").value=this.filter;this.renderCounts();this.renderTable()};if(button.dataset.overviewCount===focus)button.focus({preventScroll:true})});
  }
  private renderTable(){
    const data=this.data,table=this.el("#overview-table");
    if(!data){table.innerHTML=`<p class="empty">${this.failed?"Observations unavailable.":"Loading target observations…"}</p>`;this.el("#overview-result-count").textContent="";return}
    const matches=(t:OverviewTarget)=>this.filter==="all"||this.filter==="signals"&&signal(t)||this.filter==="deadline"&&deadline(t)||this.filter==="obsess"&&obsessing(t)||this.filter==="local"&&local(t)||this.filter==="missing"&&missing(t);
    const rows=data.targets.filter(t=>matches(t)&&[t.name,t.id,t.address,t.endpoint??""].some(v=>v.toLowerCase().includes(this.query)));
    rows.sort((a,b)=>{const difference=this.sort==="deadline"?(b.recent.deadline_miss_pct??-1)-(a.recent.deadline_miss_pct??-1):this.sort==="latency"?(b.recent.p95_ms??-1)-(a.recent.p95_ms??-1):0;return difference||a.name.localeCompare(b.name)||a.id.localeCompare(b.id)});
    this.el("#overview-result-count").textContent=`${n(rows.length)} of ${n(data.targets.length)} targets · select a target to inspect this window`;
    if(!rows.length){table.innerHTML=`<p class="empty">${data.targets.length?"No targets match this view.":"No targets configured. Add a target to your Flameping configuration to start collecting observations."}</p>`;return}
    const active=document.activeElement as HTMLElement|null,key=table.contains(active)?active?.dataset.overviewFocus:undefined;
    table.innerHTML=`<table class="overview-table"><thead><tr><th scope="col">Target</th><th scope="col">Latest observation</th><th scope="col">Window RTT · p50 / p95</th><th scope="col">Deadline misses</th><th scope="col">Local issues</th><th scope="col">Route activity</th></tr></thead><tbody>${rows.map(t=>{
      const r=t.recent,link=detailLink(t.id,data),obsess=t.obsess;
      const obsessText=obsess?.enabled?obsessLabel(obsess):"";
      return `<tr data-target-row="${esc(t.id)}"><th scope="row" data-label="Target"><a class="overview-target-link" data-overview-focus="target:${esc(t.id)}" data-overview-target="${esc(t.id)}" href="${esc(link)}">${esc(t.name)}<span aria-hidden="true">↗</span></a><small class="target-identity">${esc(t.id)} · ${esc(t.endpoint||t.address)}</small>${t.endpoint&&t.endpoint!==t.address?`<small>${esc(t.address)}</small>`:""}${obsessText?`<small class="overview-obsess ${obsessing(t)?"is-obsessing":""}">${esc(obsessText)}${obsessing(t)?` · ${esc(obsess!.reason==="loss"?"missed deadline":"latency spike")}`:""}</small>`:""}${obsess?.tracking_limited?'<small class="warm">Live observation limit reached</small>':""}</th><td data-label="Latest observation"><span class="overview-outcome ${stateTone(t.state)}">${esc(outcome[t.state]??t.state.replaceAll("_"," "))}</span><small title="${t.last_scheduled_at_ms?esc(when(t.last_scheduled_at_ms)):""}">${esc(age(t.last_scheduled_at_ms,data.as_of_ms+Math.max(0,Date.now()-this.receivedAt)))}${t.last_rtt_ms!=null?` · ${ms(t.last_rtt_ms)}`:""}</small></td><td class="overview-latency" data-label="Window RTT · p50 / p95"><span class="metric">${ms(r.p50_ms)} <span class="muted">/</span> ${ms(r.p95_ms)}</span>${trend(t,data)}<small>${n(r.rtt_count)} replies measured</small></td><td data-label="Deadline misses"><span class="metric ${deadline(t)?"warm":""}">${pct(r.deadline_miss_pct)}</span><small>${n(r.unanswered)} no reply · ${n(r.late)} late</small><small>${n(r.sent)} sent${r.pending?` · ${n(r.pending)} pending`:""}${r.partial?" · provisional":""}</small></td><td data-label="Local issues"><span class="${local(t)?"warm":"muted"}">${n(r.send_errors)} send errors</span><small>${n(r.scheduler_missed)} skipped schedules</small></td><td data-label="Route activity"><a href="${esc(detailLink(t.id,data,"route-history"))}" data-overview-focus="route:${esc(t.id)}" class="overview-route ${t.route.changes?"warm":""}">${n(t.route.changes)} changes</a><small>${n(t.route.traces)} retained traces</small>${t.route.errors?`<small class="warm">${n(t.route.errors)} trace errors</small>`:""}</td></tr>`;
    }).join("")}</tbody></table>`;
    if(key!=null)Array.from(table.querySelectorAll<HTMLAnchorElement>("[data-overview-focus]")).find(el=>el.dataset.overviewFocus===key)?.focus({preventScroll:true});
  }
  private renderInterfaces(){
    const root=this.el("#overview-interfaces"),data=this.data,focused=root.contains(document.activeElement);
    if(!data){root.innerHTML="<p class=\"empty\">Waiting for interface observations…</p>";return}
    if(!data.interfaces.length){root.innerHTML="<p class=\"empty\">No interfaces configured.</p>";return}
    const fields:[keyof HostInterface,string][]=[["rx_errors","RX errors"],["tx_errors","TX errors"],["rx_dropped","RX dropped"],["tx_dropped","TX dropped"],["rx_missed","RX missed"],["rx_fifo","RX FIFO"],["tx_fifo","TX FIFO"],["rx_crc","RX CRC"],["rx_frame","RX frame"],["tx_carrier","TX carrier"],["collisions","collisions"]];
    root.innerHTML=data.interfaces.map(info=>{
      const counters=fields.filter(([key])=>Number(info[key])>0).map(([key,label])=>`${n(Number(info[key]))} ${label}`);
      if(info.resets)counters.push(`${n(info.resets)} resets`);
      return `<div class="overview-interface"><div><strong>${esc(info.display_name||info.name)}</strong><span class="muted">${esc(info.name)}</span><span class="${info.has_errors||info.has_drops||info.resets?"warm":"muted"}">${counters.length?esc(counters.join(" · ")):info.has_deltas?"No counter signals recorded":"No counter deltas in this window"}</span></div><small>${info.last_at_ms>0?`${info.present?"Last seen":"Missing when last sampled"} · ${esc(age(info.last_at_ms,data.as_of_ms+Math.max(0,Date.now()-this.receivedAt)))}`:"Not yet observed"}</small>${info.has_deltas?`<small>Peak RX ${Number((info.peak_rx_mbps??0).toFixed(2))} / TX ${Number((info.peak_tx_mbps??0).toFixed(2))} Mbps${info.partial?" · boundary interval included":""}</small>`:""}</div>`;
    }).join("")+`<a class="overview-inspect" href="${esc(detailLink(data.targets[0]?.id,data,"interface-history"))}">Inspect host counter history →</a>`;
    if(focused)root.querySelector<HTMLAnchorElement>("a")?.focus({preventScroll:true});
  }
  private renderMonitor(){
    const status=this.monitor,root=this.el("#overview-monitor");
    if(!status){root.textContent=this.monitorFailed?"Monitor status unavailable. Retrying…":"Loading monitor status…";return}
    const bytes=(value:number)=>value>=2**30?`${(value/2**30).toFixed(1)} GiB`:value>=2**20?`${(value/2**20).toFixed(1)} MiB`:`${n(value)} B`;
    root.innerHTML=`${this.monitorFailed?'<p class="overview-refresh-error">Status refresh failed. Last known monitor state shown.</p>':""}<dl class="overview-monitor"><div><dt>Writer</dt><dd class="${status.ready?"good":"warm"}">${status.ready?"Ready":"Paused"}</dd></div><div><dt>Storage pressure</dt><dd>${esc(status.pressure)}</dd></div><div><dt>Database / free space</dt><dd>${bytes(status.live_bytes)} / ${bytes(status.available_bytes)}</dd></div><div><dt>Dirty rollup buckets</dt><dd>${n(status.dirty_buckets)}</dd></div><div><dt>Interface collection</dt><dd>${esc(status.interface_capability||"Unknown")}</dd></div>${Object.entries(status.trace_capabilities??{}).map(([family,value])=>`<div><dt>Traceroute ${esc(family)}</dt><dd>${esc(value)}</dd></div>`).join("")}</dl>${status.writer_error?`<p class="warm">${esc(status.writer_error)}</p>`:""}<p class="overview-note">Readiness describes the monitor, not connection health.</p>`;
  }
}
