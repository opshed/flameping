import uPlot from "uplot";
import type {Bounds} from "./route-history";
import "./interface-history.css";

type Interface = {name:string,display_name:string,mac:string,ifindex:number,present:boolean,last_at_ms:number};
const counters = [
  ["rx_errors","RX errors"], ["tx_errors","TX errors"],
  ["rx_dropped","RX dropped"], ["tx_dropped","TX dropped"], ["rx_missed","RX missed"],
  ["rx_crc","RX CRC"], ["rx_frame","RX frame"], ["rx_fifo","RX FIFO"],
  ["tx_fifo","TX FIFO"], ["tx_carrier","TX carrier"], ["collisions","Collisions"],
] as const;
type Counter = typeof counters[number][0];
type Point = Record<Counter,number> & {time_ms:number,rx_mbps:number,tx_mbps:number,has_deltas:boolean,partial?:boolean,reset_count:number,reset?:boolean};
type Series = {name:string,as_of_ms:number,bucket_ms:number,source_resolution_ms:number,points:Point[],resets:{at_ms:number,reason:string}[],resets_truncated:boolean};
type Row = {info:Interface,series?:Series,error?:boolean};
type Slot = {from:number,to:number,point?:Point};
type Snapshot = {row:Row,slot:Slot};

const esc = (value:string|number) => String(value).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]!));
const n = (value:number) => value.toLocaleString();
const date = (ms:number) => new Date(ms).toLocaleString();
const time = (ms:number) => new Date(ms).toLocaleTimeString([],{hour:"2-digit",minute:"2-digit"});
const plural = (value:number,label:string) => `${n(value)} ${label}${value===1?"":"s"}`;
const period = (bounds:Bounds) => `${date(bounds.from)} – ${date(bounds.to)}`;
const errors = (p:Point) => [p.rx_errors,p.tx_errors,p.rx_crc,p.rx_frame,p.rx_fifo,p.tx_fifo,p.tx_carrier,p.collisions].some(v=>v>0);
const drops = (p:Point) => [p.rx_dropped,p.tx_dropped,p.rx_missed].some(v=>v>0);
const signal = (p:Point) => errors(p)||drops(p)||p.reset_count>0;
const signals = (row:Row) => row.series?.points.filter(signal).length??0;
const recorded = (row:Row) => row.series?.points.some(p=>p.has_deltas)??false;
const resetLabel = (reason:string) => ({interface_missing:"Interface missing when sampled",counter_decreased:"Counter decreased",identity_changed:"Interface identity changed",counter_decreased_while_stopped:"Counter decreased while collector was stopped",collector_restart_or_identity_change:"Collector restarted or interface identity changed"}[reason]??reason.replaceAll("_"," "));

async function api<T>(path:string,abort:AbortSignal):Promise<T> {
  const response=await fetch(path,{headers:{accept:"application/json"},signal:abort});
  if(!response.ok)throw new Error("Interface observations unavailable");
  return response.json();
}

// These are observations of host counters, never inferred target loss or link state.
export class InterfaceHistoryView {
  private bounds:Bounds|null=null;
  private windowKey:string|null=null;
  private request?:AbortController;
  private rows:Row[]=[];
  private selectedName:string|null=null;
  private snapshot:Snapshot|null=null;
  private plot:uPlot|null=null;
  private capability="";
  private listState:"loading"|"ready"|"error"="loading";
  private focusIndexes=new Map<string,number>();

  constructor(private root:HTMLElement,private alert:HTMLElement,private detailRoot:HTMLElement,private onZoom:(bounds:Bounds)=>void,private onSelection:(bounds:Bounds|null)=>void) {
    root.innerHTML=`<div class="panel-title"><div><h3 id="interface-overview-title" tabindex="-1">Interface activity <span class="route-experiment">host</span></h3><p class="route-subtitle">Find local errors, drops, and counter resets.</p></div><button class="route-action" id="interface-next" disabled>Next signal</button></div>
      <p id="interface-status" class="route-note" role="status">Loading interface observations…</p>
      <div id="interfaces"></div><div class="interface-axis"></div>
      <div class="interface-key"><span><i class="if-error"></i>errors / diagnostics</span><span><i class="if-drop"></i>drops / missed</span><span><i class="if-reset">◆</i>reset</span><span><i class="if-empty"></i>no counter interval</span></div>
      <p class="route-note interface-scope">Host-wide counters · same time window as the graphs · select an interval for specifics.</p>
      <section id="interface-detail" aria-label="Interface details" hidden><div class="interface-detail-heading"><div><h4 id="interface-title"></h4><p id="interface-identity" class="route-note"></p></div><div><button class="route-action" id="interface-back">Back to activity</button><button class="route-action" id="interface-detail-next" disabled>Next signal</button><button class="route-action" id="interface-whole" hidden>Whole window</button><button class="route-action" id="interface-zoom" hidden>Zoom charts here</button></div></div>
      <p id="interface-detail-status" class="route-note" role="status"></p><div id="interface-counts"></div><p id="interface-diagnostics" class="route-note interface-attention" hidden></p>
      <div class="interface-traffic-heading"><strong>Traffic context</strong><span>Peak observed Mbps per bucket · RX / TX</span></div><div id="interface-chart" class="chart small" role="group" aria-label="Interface peak observed receive and transmit traffic in megabits per second"></div>
      <details id="interface-specifics"><summary>All diagnostic counters &amp; reset records</summary><div id="interface-counter-table"></div><div id="interface-resets"></div><p class="route-note">Counters can overlap; diagnostic subcounters are not added to errors. Counter increases are assigned to observation buckets; exact event times are unknown. Drops can have several causes. These observations do not establish target loss, link utilization, or link state.</p></details></section>`;
    detailRoot.append(this.el("#interface-detail"));
    this.el<HTMLButtonElement>("#interface-next").onclick=()=>this.nextSignal();
    this.el<HTMLButtonElement>("#interface-detail-next").onclick=()=>this.nextSignal();
    this.el<HTMLButtonElement>("#interface-back").onclick=()=>alert.click();
    this.el<HTMLButtonElement>("#interface-whole").onclick=()=>{this.snapshot=null;this.onSelection(null);this.renderRows();this.renderDetail();this.el("#interface-title").focus()};
    this.el<HTMLButtonElement>("#interface-zoom").onclick=()=>{if(this.snapshot)this.onZoom(this.snapshot.slot)};
    this.el("#interface-title").tabIndex=-1;
    alert.onclick=()=>{root.scrollIntoView({behavior:"smooth",block:"start"});this.el("#interface-overview-title").focus({preventScroll:true})};
  }

  private el<T extends HTMLElement=HTMLElement>(selector:string) {return (this.root.querySelector<T>(selector)??this.detailRoot.querySelector<T>(selector))!}
  align(left:number,right:number) {this.root.style.setProperty("--interface-left",`${left}px`);this.root.style.setProperty("--interface-right",`${right}px`)}
  setCapability(value:string) {this.capability=value;this.renderStatus()}

  async load(bounds:Bounds,windowKey:string,maxPoints:number) {
    // Let a slow sweep finish. Restarting it every poll can starve later interfaces.
    if(this.windowKey===windowKey&&this.root.hasAttribute("aria-busy"))return;
    this.request?.abort();
    const request=this.request=new AbortController();
    const changed=this.windowKey!==windowKey;
    this.windowKey=windowKey;this.bounds=bounds;
    this.root.dataset.from=String(bounds.from);this.root.dataset.to=String(bounds.to);
    this.root.setAttribute("aria-busy","true");
    this.listState="loading";
    maxPoints=Math.max(16,Math.min(maxPoints,180,Math.floor(Math.max(120,this.root.clientWidth-90)/7)));
    if(changed){
      this.snapshot=null;this.onSelection(null);this.rows=[];
      this.el("#interfaces").innerHTML="";this.el("#interface-detail").hidden=true;this.detailRoot.hidden=true;this.clearPlot();
      this.el("#interface-status").textContent="Loading interface observations…";
      this.alert.textContent="Interfaces · loading observations…";this.alert.className="interface-alert";
      this.el<HTMLButtonElement>("#interface-next").disabled=true;this.el<HTMLButtonElement>("#interface-detail-next").disabled=true;
    }
    try {
      const interfaces=(await api<Interface[]>("/api/v1/interfaces",request.signal))??[];
      const rows:Row[]=interfaces.map(info=>({info}));
      // Bound concurrent reads when hosts have many configured interfaces.
      let next=0;
      await Promise.all(Array.from({length:Math.min(4,rows.length)},async()=>{
        while(next<rows.length&&!request.signal.aborted){
          const row=rows[next++];
          try {row.series=await api<Series>(`/api/v1/interfaces/${encodeURIComponent(row.info.name)}/series?from=${Math.round(bounds.from)}&to=${Math.round(bounds.to)}&max_points=${maxPoints}`,request.signal)}
          catch {row.error=true}
        }
      }));
      if(request.signal.aborted)return;
      this.listState="ready";
      this.rows=rows;
      if(!rows.some(row=>row.info.name===this.selectedName)){
        this.selectedName=(rows.find(row=>signals(row)>0)||rows.find(row=>!row.info.present)||rows[0])?.info.name??null;
        this.snapshot=null;this.onSelection(null);
      }
      this.root.removeAttribute("aria-busy");this.renderStatus();this.renderRows();this.renderDetail();
    }catch {
      if(request.signal.aborted)return;
      this.listState="error";
      // Preserve an intentional snapshot through a transient polling failure.
      // Current traffic becomes unknown until the interface list recovers.
      this.rows=this.rows.map(row=>({info:row.info,error:true}));
      this.root.removeAttribute("aria-busy");this.renderRows();this.renderDetail();
      this.el("#interface-status").textContent="Could not load interfaces. Retrying…";
      this.alert.className="interface-alert attention";this.alert.textContent="Interfaces · observations unavailable";
      this.el<HTMLButtonElement>("#interface-next").disabled=true;this.el<HTMLButtonElement>("#interface-detail-next").disabled=true;
    }
  }

  private renderStatus() {
    if(this.root.hasAttribute("aria-busy")||this.listState!=="ready")return;
    const affected=this.rows.filter(row=>signals(row)>0).length,failed=this.rows.filter(row=>row.error).length;
    const missing=this.rows.filter(row=>!row.info.present).length,unmeasured=this.rows.filter(row=>!row.error&&!recorded(row)).length;
    const unavailable=this.capability.startsWith("unavailable");
    this.alert.className=`interface-alert ${affected||failed||missing||unavailable?"attention":""}`;
    this.alert.innerHTML=`<strong>Interfaces</strong><span>${!this.rows.length?"No interfaces configured":[
      affected?`${plural(affected,"interface")} with recorded signals`:failed||unmeasured?"Counter history incomplete":"No counter signals recorded",
      failed?`${failed} unavailable`:"",missing?`${missing} missing / unobserved`:"",
    ].filter(Boolean).join(" · ")}${unavailable?" · collection unavailable":""}</span><span aria-hidden="true">↓</span>`;
    this.el("#interface-status").textContent=this.rows.length?
      `${plural(this.rows.length,"interface")} · ${plural(affected,"interface")} with signals in the displayed buckets${unavailable?` · ${this.capability}`:""}`:
      `No interfaces configured.${unavailable?` Collection ${this.capability}.`:""}`;
    this.el<HTMLButtonElement>("#interface-next").disabled=!affected;this.el<HTMLButtonElement>("#interface-detail-next").disabled=!affected;
    this.root.dataset.signalInterfaces=String(affected);
  }

  private slots(series:Series):Slot[] {
    if(!this.bounds||series.bucket_ms<=0)return[];
    const width=series.bucket_ms,byTime=new Map(series.points.map(p=>[p.time_ms,p]));
    const result:Slot[]=[];
    for(let at=Math.floor(this.bounds.from/width)*width;at<this.bounds.to;at+=width){
      result.push({from:Math.max(at,this.bounds.from),to:Math.min(at+width,this.bounds.to),point:byTime.get(at)});
    }
    return result;
  }

  private slotLabel(slot:Slot) {
    const p=slot.point;
    const increased=p?counters.filter(([field])=>p[field]>0).map(([field,label])=>`${label} ${n(p[field])}`):[];
    return `${period(slot)} · ${increased.join(", ")||(!p?.has_deltas?"No counter interval recorded":"No counter increases recorded")}${p?.reset_count?` · ${plural(p.reset_count,"reset")}`:""}${p?.partial?" · clipped edge bucket":""}`;
  }

  private renderRows() {
    const focused=this.root.contains(document.activeElement)?document.activeElement as HTMLElement:null;
    const focusName=focused?.closest<HTMLElement>("[data-interface]")?.dataset.interface;
    const focusIndex=focused?.dataset.index;
    const bounds=this.bounds!;
    this.el("#interfaces").innerHTML=this.rows.map(row=>{
      const {info,series}=row,count=signals(row),slots=series?this.slots(series):[];
      const label=row.error?"Could not load counter history · retrying…":count?`${plural(count,"bucket")} with signals`:recorded(row)?"No counter increases recorded":"No counter intervals recorded";
      const title=`<strong>${esc(info.display_name||info.name)}</strong>${info.display_name&&info.display_name!==info.name?` <span>${esc(info.name)}</span>`:""}`;
      return `<div class="interface-row ${this.selectedName===info.name?"active":""}" data-interface="${esc(info.name)}"><div class="interface-row-heading">${this.rows.length===1?`<div>${title}</div>`:`<button class="interface-name" data-name="${esc(info.name)}" aria-pressed="${this.selectedName===info.name}">${title}</button>`}<span class="${count||row.error?"interface-attention":""}">${esc(label)}${!info.present?" · missing / unobserved":""}</span></div>
        <div class="interface-track-wrap"><div class="interface-lane-labels" aria-hidden="true"><span>ERROR</span><span>DROP</span><span>RESET</span></div><div class="interface-track" role="group" aria-label="${esc(info.name)} counter intervals. Arrow keys navigate, Enter inspects.">${slots.map((slot,index)=>{
          const p=slot.point,selected=this.snapshot?.row.info.name===info.name&&this.snapshot.slot.from<slot.to&&this.snapshot.slot.to>slot.from;
          return `<button class="interface-bucket ${!p?.has_deltas?"no-deltas":""} ${selected?"selected":""} ${p?.partial?"partial":""}" data-index="${index}" data-signal="${!!p&&signal(p)}" data-from="${slot.from}" data-to="${slot.to}" tabindex="${index===Math.min(this.focusIndexes.get(info.name)??0,slots.length-1)?0:-1}" aria-pressed="${!!selected}" aria-label="${esc(this.slotLabel(slot))}" title="${esc(this.slotLabel(slot))}"><i class="${p&&errors(p)?"if-error":""}"></i><i class="${p&&drops(p)?"if-drop":""}"></i><i class="${p?.reset_count?"if-reset":""}">${p?.reset_count?"◆":""}</i></button>`;
        }).join("")}</div></div></div>`;
    }).join("");
    this.el(".interface-axis").innerHTML=`<span>${time(bounds.from)}</span><span>${time((bounds.from+bounds.to)/2)}</span><span>${time(bounds.to)}</span>`;
    this.root.querySelectorAll<HTMLElement>("[data-interface]").forEach(element=>{
      const row=this.rows.find(row=>row.info.name===element.dataset.interface)!,slots=row.series?this.slots(row.series):[];
      const name=element.querySelector<HTMLButtonElement>(".interface-name");
      if(name)name.onclick=()=>{this.selectedName=row.info.name;this.snapshot=null;this.onSelection(null);this.renderRows();this.renderDetail()};
      const buttons=Array.from(element.querySelectorAll<HTMLButtonElement>(".interface-bucket"));
      buttons.forEach((button,index)=>{
        const slot=slots[index];
        button.style.left=`${100*(slot.from-bounds.from)/(bounds.to-bounds.from)}%`;
        button.style.width=`${100*(slot.to-slot.from)/(bounds.to-bounds.from)}%`;
        button.onclick=()=>this.inspect(row,slots[index]);
        button.onkeydown=event=>{
          const next=event.key==="Home"?0:event.key==="End"?buttons.length-1:event.key==="ArrowLeft"?Math.max(0,index-1):event.key==="ArrowRight"?Math.min(buttons.length-1,index+1):-1;
          if(next<0)return;event.preventDefault();buttons.forEach(b=>b.tabIndex=-1);buttons[next].tabIndex=0;buttons[next].focus();this.focusIndexes.set(row.info.name,next);
        };
      });
    });
    if(focusName){
      const element=Array.from(this.root.querySelectorAll<HTMLElement>("[data-interface]")).find(el=>el.dataset.interface===focusName);
      (focusIndex!=null?element?.querySelector<HTMLElement>(`[data-index="${focusIndex}"]`):element?.querySelector<HTMLElement>(".interface-name"))?.focus({preventScroll:true});
    }
  }

  private inspect(row:Row,slot:Slot) {
    this.selectedName=row.info.name;this.snapshot={row,slot};this.onSelection(slot);this.renderRows();this.renderDetail();
    this.detailRoot.scrollIntoView({behavior:"smooth",block:"start"});this.el("#interface-title").focus({preventScroll:true});
  }

  private nextSignal() {
    const items=this.rows.flatMap(row=>row.series?this.slots(row.series).filter(slot=>slot.point&&signal(slot.point)).map(slot=>({row,slot})):[]).sort((a,b)=>a.slot.from-b.slot.from||a.row.info.name.localeCompare(b.row.info.name));
    if(!items.length)return;
    const current=items.findIndex(item=>item.row.info.name===this.snapshot?.row.info.name&&item.slot.from===this.snapshot.slot.from);
    const item=items[(current+1)%items.length];this.inspect(item.row,item.slot);
  }

  private renderDetail() {
    const current=this.rows.find(row=>row.info.name===this.selectedName);
    this.el("#interface-detail").hidden=!current;this.detailRoot.hidden=!current;
    if(!current){this.clearPlot();return}
    const row=this.snapshot?.row??current,series=row.series,slot=this.snapshot?.slot;
    this.el("#interface-title").textContent=row.info.name;
    this.el("#interface-identity").textContent=[row.info.display_name,row.info.mac,row.info.ifindex?`index ${row.info.ifindex}`:"",row.info.last_at_ms?`Last sample ${date(row.info.last_at_ms)}`:"No sample recorded",!row.info.present?"Missing / unobserved":""].filter(Boolean).join(" · ");
    this.el("#interface-whole").hidden=!slot;this.el("#interface-zoom").hidden=!slot;
    const points=slot?(slot.point?[slot.point]:[]):series?.points??[];
    const hasDeltas=points.some(p=>p.has_deltas),hasPartial=points.some(p=>p.partial);
    this.el("#interface-detail-status").textContent=[
      current.error?"Current interface history unavailable · retrying…":"",
      slot?`Selected interval · ${period(slot)} · snapshot`:`Displayed window · ${period(this.bounds!)}`,
      !hasDeltas?"No counter interval recorded; values are unknown.":"Counter increases recorded in these buckets.",
      hasPartial?"Clipped edge bucket: historical rollups may include counts outside the window.":"",
      slot?.point?.partial&&series?`Full bucket: ${period({from:slot.point.time_ms,to:slot.point.time_ms+series.bucket_ms})}.`:"",
    ].filter(Boolean).join(" ");
    const total=(field:Counter)=>points.reduce((sum,p)=>sum+(p[field]??0),0);
    const value=(field:Counter)=>hasDeltas?n(total(field)):"—";
    const resetCount=points.reduce((sum,p)=>sum+p.reset_count,0);
    this.el("#interface-counts").innerHTML=[
      ["Errors",`RX ${value("rx_errors")} <span>·</span> TX ${value("tx_errors")}`,total("rx_errors")+total("tx_errors")],
      ["Dropped",`RX ${value("rx_dropped")} <span>·</span> TX ${value("tx_dropped")}`,total("rx_dropped")+total("tx_dropped")],
      ["Missed by host",`RX ${value("rx_missed")}`,total("rx_missed")],
      ["Resets",series?n(resetCount):"—",resetCount],
    ].map(([label,value,active])=>`<div class="interface-count ${active?"has-signal":""}"><label>${label}</label><strong>${value}</strong></div>`).join("");
    const diagnostics=counters.slice(5).filter(([field])=>total(field)>0).map(([field,label])=>`${label} ${n(total(field))}`);
    this.el("#interface-diagnostics").hidden=!diagnostics.length;
    this.el("#interface-diagnostics").textContent=`Diagnostic counters: ${diagnostics.join(" · ")}`;
    this.el("#interface-counter-table").innerHTML=`<table><caption>Counter increases · ${slot?"selected interval":"displayed buckets"}</caption><thead><tr><th>Counter</th><th>Increase</th></tr></thead><tbody>${counters.map(([field,label])=>`<tr class="${total(field)>0?"has-signal":""}"><th>${label}</th><td>${value(field)}</td></tr>`).join("")}</tbody></table>`;
    const resets=(series?.resets??[]).filter(reset=>!slot||reset.at_ms>=slot.from&&reset.at_ms<slot.to);
    this.el("#interface-resets").innerHTML=`<h4>Reset observations</h4>${resets.length?`<ul>${resets.map(reset=>`<li><time>${date(reset.at_ms)}</time> · ${esc(resetLabel(reset.reason))}</li>`).join("")}</ul>`:"<p class=route-note>No reset details in this selection.</p>"}${series?.resets_truncated?"<p class=route-note>Only the latest 100 reset details in the window are included. Marker counts include every recorded reset. Zoom in for older details.</p>":""}`;
    // Keep traffic synchronized with the main window; selected counters above are a snapshot.
    this.drawTraffic(current);
  }

  private clearPlot() {this.plot?.destroy();this.plot=null;this.el("#interface-chart").replaceChildren();delete this.el("#interface-chart").dataset.from;delete this.el("#interface-chart").dataset.to}

  private drawTraffic(row:Row) {
    const bounds=this.bounds!,root=this.el("#interface-chart"),slots=row.series?this.slots(row.series):[];
    const data:uPlot.AlignedData=[slots.map(slot=>(slot.from+slot.to)/2000),...(["rx_mbps","tx_mbps"] as const).map(field=>slots.map(slot=>slot.point?.has_deltas?slot.point[field]:null))] as uPlot.AlignedData;
    const size={width:Math.max(200,root.clientWidth),height:180};
    root.dataset.observed=String(slots.filter(slot=>slot.point?.has_deltas).length);
    const draw=(u:uPlot)=>{
      root.dataset.from=String((u.scales.x.min??0)*1000);root.dataset.to=String((u.scales.x.max??0)*1000);
      const selected=this.snapshot?.slot;if(!selected)return;
      const {ctx,bbox}=u,left=Math.max(bbox.left,u.valToPos(selected.from/1000,"x",true)),right=Math.min(bbox.left+bbox.width,u.valToPos(selected.to/1000,"x",true));
      if(right<=left)return;ctx.save();ctx.fillStyle="rgba(177,140,255,.13)";ctx.fillRect(left,bbox.top,right-left,bbox.height);ctx.restore();
    };
    if(this.plot){
      this.plot.scales.x.range=()=>[bounds.from/1000,bounds.to/1000];
      this.plot.batch(()=>{this.plot!.setSize(size);this.plot!.setData(data);this.plot!.setScale("x",{min:bounds.from/1000,max:bounds.to/1000})});
      return;
    }
    this.plot=new uPlot({...size,legend:{show:true},cursor:{drag:{x:true,y:false}},scales:{x:{auto:false,range:()=>[bounds.from/1000,bounds.to/1000]},y:{range:(_u,_min,max)=>[0,Math.max(1,max*1.1)]}},axes:[{stroke:"#82949f",grid:{stroke:"#20313a"}},{stroke:"#82949f",grid:{stroke:"#20313a"}}],series:[{},...["RX","TX"].map((label,index)=>({label:`${label} peak Mbps`,stroke:index?"#839ce7":"#5bd6d0",width:1.4,spanGaps:false,points:{show:true,size:4},value:(_u:uPlot,value:number)=>Number.isFinite(value)?`${value.toFixed(2)} Mbps`:"—"}))],hooks:{draw:[draw],setSelect:[u=>{if(u.select.width<8)return;const from=u.posToVal(u.select.left,"x")*1000,to=u.posToVal(u.select.left+u.select.width,"x")*1000;queueMicrotask(()=>this.onZoom({from,to}))}]}},data,root);
  }
}
