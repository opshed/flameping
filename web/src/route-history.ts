import "./route-history.css";

export type Bounds = {from:number,to:number};
type Counts = {traces:number,reached:number,unreached:number,errors:number,changes:number};
type Bucket = Counts & {from_ms:number,to_ms:number};
type Hop = {ttl:number,responders:string[]|null};
type Trace = {id:number,started_ms:number,ended_ms:number,endpoint:string,method:string,flow_id:string,status:string,reached:boolean,reached_hop:number,error_detail?:string};
type Change = {id:number,confirmed_ms:number,first_seen_ms:number,endpoint:string,method?:string,flow_id?:string,old_route:Hop[],new_route:Hop[],old_reached_hop:number,new_reached_hop:number,old_trace_id?:number,candidate_trace_id?:number,confirming_trace_id?:number};
type History = {from_ms:number,to_ms:number,bucket_ms:number,totals:Counts,buckets:Bucket[],traces:Trace[],changes:Change[],traces_truncated:boolean,changes_truncated:boolean};
type Probe = {ttl:number,index:number,responder?:string,rtt_ms?:number,icmp_type?:number,icmp_code?:number};
type TraceDetail = Trace & {target_id:string,probes:Probe[]};

const esc = (value:string|number) => String(value).replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]!));
const time = (ms:number) => new Date(ms).toLocaleTimeString([], {hour:"2-digit",minute:"2-digit",second:"2-digit"});
const date = (ms:number) => new Date(ms).toLocaleString();
const count = (n:number,word:string) => `${n.toLocaleString()} ${word}${n===1?"":"s"}`;
const period = (bounds:Bounds) => `${date(bounds.from)} – ${date(bounds.to)}`;
const outcome = (trace:Trace) => trace.status!=="completed" ? `Trace ${trace.status.replaceAll("_"," ")}` : trace.reached ? `Destination reached · hop ${trace.reached_hop}` : "Destination not reached";

async function api<T>(path:string,signal:AbortSignal):Promise<T> {
  const response = await fetch(path,{headers:{accept:"application/json"},signal});
  if(!response.ok)throw new Error(response.status===404?(path.startsWith("/api/v1/traces/")?"This trace is no longer retained.":"This target is no longer available."):"Could not load route history. Try again.");
  return response.json();
}

function hopDiff(change:Change) {
  const before = new Map((change.old_route??[]).map(hop=>[hop.ttl,[...(hop.responders??[])].sort()]));
  const after = new Map((change.new_route??[]).map(hop=>[hop.ttl,[...(hop.responders??[])].sort()]));
  return [...new Set([...before.keys(),...after.keys()])].sort((a,b)=>a-b).map(ttl=>{
    const old = before.get(ttl)??[],next = after.get(ttl)??[];
    const different = old.join(",")!==next.join(",");
    const evidence = different&&old.length>0&&next.length>0&&!old.some(ip=>next.includes(ip));
    return {ttl,old,next,different,evidence};
  });
}

function changeLabel(change:Change) {
  const hops = hopDiff(change).filter(hop=>hop.evidence).map(hop=>hop.ttl);
  const reached = change.old_reached_hop!==change.new_reached_hop;
  return [hops.length?`Hop${hops.length===1?"":"s"} ${hops.join(", ")} changed`:"",reached?"Destination observation changed":""].filter(Boolean).join(" · ")||"Confirmed route change";
}

// The overview describes recorded events and retained observations only. It does
// not reconstruct persistent routes from signatures or fill collection gaps.
export class RouteHistoryView {
  private target:string|null = null;
  private bounds:Bounds|null = null;
  private windowKey:string|null = null;
  private history:History|null = null;
  private overviewRequest?:AbortController;
  private detailRequest?:AbortController;
  private traceRequest?:AbortController;
  private selected:Bounds|null = null;
  private detailData:History|null = null;
  private changeID:number|null = null;
  private open = false;
  private focusIndex = 0;

  constructor(private root:HTMLElement,private onZoom:(bounds:Bounds)=>void,private onSelection:(bounds:Bounds|null)=>void) {
    root.innerHTML = `<div class="panel-title"><div><h3>Route activity <span class="route-experiment">experiment</span></h3><p class="route-subtitle">See when the path changed. Inspect what changed.</p></div><button class="route-action" id="route-inspect" disabled aria-expanded="false" aria-controls="route-inspector">Inspect history</button></div>
      <div id="route-summary" class="route-summary" role="status">Select a target to view route history.</div>
      <div id="route-overview"><div class="route-chart"><div class="route-lane-labels" aria-hidden="true"><span>CHANGES</span><span>TRACES</span></div><div id="route-buckets" class="route-buckets" role="group" aria-label="Route activity intervals. Use arrow keys to navigate, Enter to inspect."></div></div><div id="route-axis" class="route-axis"></div></div>
      <div class="route-key"><span><i class="route-key-change"></i>confirmed changes · height = relative count</span><span><i class="route-key-reached"></i>reached</span><span><i class="route-key-unreached"></i>not reached</span><span><i class="route-key-error">×</i>trace error / timeout</span><span><i class="route-key-empty"></i>no retained traces</span></div>
      <p id="route-hint" class="route-note">Select an interval to inspect it. Traces are periodic observations; a missing reply does not establish packet loss.</p>
      <section id="route-inspector" class="route-inspector" aria-label="Route history details" hidden><div class="route-detail-toolbar"><strong id="route-detail-title"></strong><div><button id="route-zoom" class="route-action">Zoom charts here</button><button id="route-all" class="route-action">Whole window</button><button id="route-close" class="route-action">Close</button></div></div><div id="route-records"></div><div id="route-comparison"></div><div id="trace-detail" class="route-probes"></div></section>`;
    this.el<HTMLButtonElement>("#route-inspect").onclick = ()=>this.open?this.close():this.inspect(null);
    this.el<HTMLButtonElement>("#route-close").onclick = ()=>{this.close();this.el("#route-inspect").focus()};
    this.el<HTMLButtonElement>("#route-all").onclick = ()=>this.inspect(null);
    this.el<HTMLButtonElement>("#route-zoom").onclick = ()=>{if(this.selected)this.onZoom(this.selected)};
  }

  private el<T extends HTMLElement=HTMLElement>(selector:string) {return this.root.querySelector<T>(selector)!}

  align(left:number,right:number) {
    this.root.style.setProperty("--route-left",`${left}px`);
    this.root.style.setProperty("--route-right",`${right}px`);
  }

  async load(target:string,bounds:Bounds,windowKey:string) {
    const changedTarget = target!==this.target;
    const changedWindow = windowKey!==this.windowKey;
    this.target = target;
    this.bounds = bounds;
    this.windowKey = windowKey;
    this.overviewRequest?.abort();
    const request = this.overviewRequest = new AbortController();
    if(changedTarget||changedWindow) {
      this.close();
      this.history = null;
      this.el("#route-buckets").replaceChildren();
      this.el("#route-axis").replaceChildren();
      this.el("#route-summary").textContent = "Loading route activity…";
      this.el<HTMLButtonElement>("#route-inspect").disabled = true;
    }
    this.root.setAttribute("aria-busy","true");
    try {
      const history = await this.fetchHistory(bounds,request.signal);
      if(request.signal.aborted)return;
      this.history = history;
      this.renderOverview(history);
      // An open inspector is a snapshot. Polling must not replace a selected
      // event, an expanded disclosure, keyboard focus, or a raw trace request.
    } catch(error) {
      if(request.signal.aborted)return;
      this.el("#route-summary").textContent = this.history?"Route refresh failed · showing the previous window. Retrying…":(error as Error).message;
    } finally {if(!request.signal.aborted)this.root.removeAttribute("aria-busy")}
  }

  private fetchHistory(bounds:Bounds,signal:AbortSignal) {
    const width = this.el("#route-buckets").clientWidth||600;
    const bins = Math.max(12,Math.min(120,Math.floor(width/12)));
    return api<History>(`/api/v1/targets/${encodeURIComponent(this.target!)}/route-history?from=${Math.round(bounds.from)}&to=${Math.round(bounds.to)}&max_points=${bins}&limit=100`,signal);
  }

  private renderOverview(history:History) {
    const totals = history.totals;
    this.el("#route-summary").innerHTML = `<strong class="${totals.changes?"route-warm":""}">${totals.changes?count(totals.changes,"confirmed change"):"No confirmed changes recorded"}</strong><span>${count(totals.traces,"retained trace")} · ${totals.reached.toLocaleString()} reached${totals.unreached?` · ${totals.unreached} not reached`:""}${totals.errors?` · ${count(totals.errors,"error / timeout")}`:""}</span>`;
    this.el<HTMLButtonElement>("#route-inspect").disabled = false;
    this.root.dataset.routeChanges = String(totals.changes);
    this.root.dataset.routeTraces = String(totals.traces);
    this.root.dataset.from = String(history.from_ms);
    this.root.dataset.to = String(history.to_ms);
    const track = this.el("#route-buckets");
    const hadFocus = track.contains(document.activeElement);
    this.focusIndex = Math.max(0,Math.min(this.focusIndex,history.buckets.length-1));
    const maxChanges = Math.max(1,...history.buckets.map(bucket=>bucket.changes));
    track.replaceChildren(...history.buckets.map((bucket,index)=>{
      const button = document.createElement("button");
      button.type = "button";
      button.className = `route-bin${bucket.changes?" has-changes":""}${!bucket.traces?" no-traces":""}`;
      button.dataset.index = String(index);
      button.dataset.changes = String(bucket.changes);
      button.style.flexGrow = String(bucket.to_ms-bucket.from_ms);
      button.tabIndex = index===this.focusIndex?0:-1;
      const label = `${period({from:bucket.from_ms,to:bucket.to_ms})}: ${count(bucket.changes,"confirmed change")}, ${count(bucket.traces,"retained trace")}, ${bucket.reached} reached, ${bucket.unreached} not reached, ${bucket.errors} errors or timeouts`;
      button.setAttribute("aria-label",label);
      button.title = label;
      button.innerHTML = `<span class="route-event-lane" aria-hidden="true"><i class="route-ember"></i></span><span class="route-observation-lane" aria-hidden="true"><i class="route-observation reached"></i><i class="route-observation unreached"></i><i class="route-observation error">${bucket.errors?"×":""}</i></span>`;
      const ember = button.querySelector<HTMLElement>(".route-ember")!;
      ember.style.height = `${bucket.changes?20+80*Math.log2(bucket.changes+1)/Math.log2(maxChanges+1):0}%`;
      for(const field of ["reached","unreached","error"] as const)button.querySelector<HTMLElement>(`.route-observation.${field}`)!.style.opacity = (field==="error"?bucket.errors:bucket[field])?"1":"0";
      button.onclick = ()=>{this.focusIndex=index;this.inspect({from:bucket.from_ms,to:bucket.to_ms})};
      button.onfocus = ()=>{this.focusIndex=index;this.el("#route-hint").textContent=label};
      button.onmouseenter = ()=>{this.el("#route-hint").textContent=label};
      button.onkeydown = event=>{
        const next = event.key==="ArrowLeft"?index-1:event.key==="ArrowRight"?index+1:event.key==="Home"?0:event.key==="End"?history.buckets.length-1:null;
        if(next===null)return;
        event.preventDefault();
        const clamped = Math.max(0,Math.min(history.buckets.length-1,next));
        button.tabIndex=-1;
        const sibling = track.children[clamped] as HTMLButtonElement;
        sibling.tabIndex=0;sibling.focus();
      };
      return button;
    }));
    if(hadFocus)(track.children[this.focusIndex] as HTMLElement)?.focus();
    this.markSelection();
    const span = history.to_ms-history.from_ms;
    this.el("#route-axis").innerHTML = [0,.5,1].map(fraction=>{
      const ms=history.from_ms+span*fraction;
      const label = span>=86400_000?new Date(ms).toLocaleString([], {month:"short",day:"numeric",hour:"2-digit",minute:"2-digit"}):time(ms);
      return `<span>${esc(label)}</span>`;
    }).join("");
    if(!hadFocus)this.el("#route-hint").textContent = totals.traces?"Select an interval to inspect it. Traces are periodic observations; a missing reply does not establish packet loss.":"No retained traces in this window. Collection may be disabled, unavailable, or outside retention; change records can outlive ordinary traces.";
  }

  private markSelection() {
    this.el("#route-buckets").querySelectorAll<HTMLButtonElement>("button").forEach((button,index)=>{
      const bucket = this.history?.buckets[index];
      const selected = !!(bucket&&this.selected&&bucket.from_ms<this.selected.to&&bucket.to_ms>this.selected.from);
      button.classList.toggle("selected",selected);
      button.setAttribute("aria-pressed",String(selected));
    });
  }

  private close() {
    this.open=false;this.selected=null;this.detailData=null;this.changeID=null;
    this.detailRequest?.abort();this.traceRequest?.abort();
    this.el("#route-inspector").hidden=true;
    this.el("#route-records").replaceChildren();this.el("#route-comparison").replaceChildren();this.el("#trace-detail").replaceChildren();
    this.el("#route-inspect").setAttribute("aria-expanded","false");
    this.el("#route-inspect").textContent="Inspect history";
    this.markSelection();this.onSelection(null);
  }

  private async inspect(bounds:Bounds|null) {
    if(!this.history||!this.bounds)return;
    this.detailRequest?.abort();this.traceRequest?.abort();
    const request = this.detailRequest = new AbortController();
    this.open=true;this.selected=bounds;this.changeID=null;this.detailData=null;
    this.el("#route-inspector").hidden=false;
    this.el("#route-inspect").setAttribute("aria-expanded","true");
    this.el("#route-inspect").textContent="Hide history";
    this.el("#route-zoom").hidden=!bounds;this.el("#route-all").hidden=!bounds;
    this.el("#route-detail-title").textContent = bounds?period(bounds):`Window snapshot · ${period({from:this.history.from_ms,to:this.history.to_ms})}`;
    this.el("#route-records").textContent="Loading observations…";
    this.el("#route-comparison").replaceChildren();this.el("#trace-detail").replaceChildren();
    this.markSelection();this.onSelection(bounds);
    try {
      const data = bounds?await this.fetchHistory(bounds,request.signal):this.history;
      if(request.signal.aborted)return;
      this.detailData=data;
      this.renderRecords(data);
      if(data.changes.length===1)this.showChange(data.changes[0].id);
    } catch(error) {
      if(request.signal.aborted)return;
      this.el("#route-records").innerHTML=`<p class="empty" role="alert">${esc((error as Error).message)}</p><button class="route-action route-retry">Retry</button>`;
      this.el<HTMLButtonElement>(".route-retry")?.addEventListener("click",()=>this.inspect(bounds));
    }
  }

  private renderRecords(data:History) {
    const changeRows = data.changes.map(change=>`<button class="route-record route-change-record" data-change="${change.id}"><span class="route-record-icon">◆</span><span><strong>${esc(time(change.confirmed_ms))}</strong><small>confirmed</small></span><span>${esc(changeLabel(change))}<small>First observed ${esc(date(change.first_seen_ms))}</small></span><span aria-hidden="true">›</span></button>`).join("");
    const traceRows = data.traces.map(trace=>`<button class="route-record" data-trace="${trace.id}"><span class="route-record-icon ${trace.status!=="completed"?"trace-error":trace.reached?"trace-reached":""}">${trace.status!=="completed"?"×":trace.reached?"●":"○"}</span><span><strong>${esc(time(trace.started_ms))}</strong><small>${esc(new Date(trace.started_ms).toLocaleDateString())}</small></span><span>${esc(outcome(trace))}<small>${esc(trace.endpoint)} · ${esc(trace.method)}</small></span><span aria-hidden="true">›</span></button>`).join("");
    this.el("#route-records").innerHTML = `<h4>${count(data.totals.changes,"confirmed change")}</h4>${data.changes_truncated?`<p class="route-limit">Showing the latest ${data.changes.length} of ${data.totals.changes} changes. Zoom charts into a smaller interval to see earlier records. The overview counts every retained event.</p>`:""}<div class="route-record-list">${changeRows||'<p class="empty">No confirmed changes recorded in this interval.</p>'}</div><details class="route-trace-list" ${data.changes.length?"":"open"}><summary>${count(data.totals.traces,"retained trace")} · inspect individual runs</summary>${data.traces_truncated?`<p class="route-limit">Showing the latest ${data.traces.length} of ${data.totals.traces} traces. Select a smaller interval for earlier runs.</p>`:""}<div class="route-record-list">${traceRows||'<p class="empty">No retained trace runs in this interval.</p>'}</div></details>`;
    this.el("#route-records").querySelectorAll<HTMLButtonElement>("[data-change]").forEach(button=>button.onclick=()=>this.showChange(Number(button.dataset.change)));
    this.el("#route-records").querySelectorAll<HTMLButtonElement>("[data-trace]").forEach(button=>button.onclick=()=>this.showTrace(Number(button.dataset.trace)));
  }

  private showChange(id:number) {
    const changes=this.detailData?.changes??[],index=changes.findIndex(change=>change.id===id),change=changes[index];
    if(!change)return;
    this.traceRequest?.abort();this.el("#trace-detail").replaceChildren();this.changeID=id;
    this.el("#route-records").querySelectorAll<HTMLButtonElement>("[data-change]").forEach(button=>button.setAttribute("aria-pressed",String(Number(button.dataset.change)===id)));
    const hops=hopDiff(change);
    const row = (hop:ReturnType<typeof hopDiff>[number])=>`<tr class="${hop.evidence?"route-hop-changed":hop.different?"route-hop-observation":""}"><th scope="row">${hop.ttl}</th><td>${hop.old.length?hop.old.map(esc).join("<br>"):"No reply observed"}</td><td>${hop.next.length?hop.next.map(esc).join("<br>"):"No reply observed"}</td><td>${hop.evidence?"Changed":hop.different?"Observation differs":"Same responders"}</td></tr>`;
    const table=(rows:typeof hops)=>`<div class="route-table-scroll"><table class="route-hop-table"><thead><tr><th>TTL</th><th>Before</th><th>After</th><th>Observation</th></tr></thead><tbody>${rows.map(row).join("")}</tbody></table></div>`;
    const changed=hops.filter(hop=>hop.different),same=hops.filter(hop=>!hop.different);
    const reach=(hop:number)=>hop?`reached at hop ${hop}`:"not reached";
    this.el("#route-comparison").innerHTML=`<div class="route-comparison-heading"><h4>${esc(changeLabel(change))}</h4><div><button class="route-action" data-step="1" ${index===changes.length-1?"disabled":""}>← Older change</button><button class="route-action" data-step="-1" ${index===0?"disabled":""}>Newer change →</button></div></div><p class="route-note">First observed ${esc(date(change.first_seen_ms))} · confirmed ${esc(date(change.confirmed_ms))}. The exact transition time is unknown.</p><p class="route-context">${esc(change.endpoint)}${change.method?` · ${esc(change.method)}`:""}${change.flow_id?`<small>Flow ${esc(change.flow_id)}</small>`:""}</p><p class="route-reach">Destination: ${reach(change.old_reached_hop)} → ${reach(change.new_reached_hop)}</p>${changed.length?table(changed):'<p class="route-note">No disjoint responder changes at comparable TTLs.</p>'}${same.length?`<details class="route-unchanged"><summary>${count(same.length,"unchanged TTL")}</summary>${table(same)}</details>`:""}<p class="route-note">Responder sets at each TTL; missing replies and overlapping sets are observation differences. They do not independently prove a route change.</p><div class="route-evidence">${[[change.old_trace_id,"Baseline trace"],[change.candidate_trace_id,"First observation"],[change.confirming_trace_id,"Confirming trace"]].map(([traceID,label])=>traceID?`<button class="route-action" data-evidence="${traceID}">${label}</button>`:`<span class="route-note">${label} unavailable</span>`).join("")}</div><p class="route-note">Baseline is the stored comparison route, which can predate the last trace before this event.</p>`;
    this.el("#route-comparison").querySelectorAll<HTMLButtonElement>("[data-step]").forEach(button=>button.onclick=()=>{
      const step=button.dataset.step!;
      this.showChange(changes[index+Number(step)].id);
      const next=this.el<HTMLButtonElement>(`[data-step="${step}"]`);
      (next.disabled?this.el<HTMLButtonElement>(`[data-step="${-Number(step)}"]`):next).focus();
    });
    this.el("#route-comparison").querySelectorAll<HTMLButtonElement>("[data-evidence]").forEach(button=>button.onclick=()=>this.showTrace(Number(button.dataset.evidence)));
  }

  private async showTrace(id:number) {
    this.traceRequest?.abort();
    const request=this.traceRequest=new AbortController(),target=this.target;
    this.el("#trace-detail").innerHTML='<p class="empty" role="status">Loading probe details…</p>';
    try {
      const trace=await api<TraceDetail>(`/api/v1/traces/${id}`,request.signal);
      if(request.signal.aborted||target!==this.target)return;
      if(trace.target_id!==target)throw new Error("This trace belongs to another target.");
      this.el("#trace-detail").innerHTML=`<h4>Trace ${trace.id} · ${esc(date(trace.started_ms))}</h4><p class="route-context">${esc(trace.endpoint)} · ${esc(trace.method)}<small>${esc(outcome(trace))} · ${esc(trace.status.replaceAll("_"," "))}</small><small>Flow ${esc(trace.flow_id||"not recorded")}</small></p>${trace.error_detail?`<p class="route-note">${esc(trace.error_detail)}</p>`:""}<p class="route-note">RTT is round trip to the responder, not delay on a single link. A silent hop can still forward traffic.</p><div class="route-table-scroll"><table class="route-hop-table"><thead><tr><th>TTL / probe</th><th>Responder</th><th>RTT</th><th>ICMP type / code</th></tr></thead><tbody>${(trace.probes??[]).map(probe=>`<tr><th scope="row">${probe.ttl} / ${probe.index+1}</th><td>${esc(probe.responder||"No reply observed")}</td><td>${probe.rtt_ms==null?"—":probe.rtt_ms.toFixed(2)+" ms"}</td><td>${probe.icmp_type==null?"—":`${probe.icmp_type} / ${probe.icmp_code??"—"}`}</td></tr>`).join("")}</tbody></table>${trace.probes?.length?"":'<p class="empty">No probes recorded.</p>'}</div>`;
    } catch(error) {
      if(request.signal.aborted)return;
      this.el("#trace-detail").innerHTML=`<p class="empty" role="alert">${esc((error as Error).message)}</p><button class="route-action route-trace-retry">Retry trace</button>`;
      this.el<HTMLButtonElement>(".route-trace-retry")?.addEventListener("click",()=>this.showTrace(id));
    }
  }
}
