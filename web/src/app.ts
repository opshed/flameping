import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import "./app.css";
import "./extra.css";

type Target={id:string,name:string,address:string,endpoint?:string,state:string,last_rtt_ms?:number};
type Point={time_ms:number,scheduled:number,attempted:number,sent:number,on_time?:number,late:number,unanswered:number,send_errors:number,scheduler_missed:number,rtt_count:number,avg_ms?:number,min_ms?:number,p50_ms?:number,p95_ms?:number,p99_ms?:number,max_ms?:number,distribution_ms?:number[],deadline_miss_pct:number,no_reply_pct:number,partial?:boolean,late_pct?:number,send_error_pct?:number,gap_pct?:number,partial_marker?:number};
type RangePreset={duration:number,maxPoints:number};
type FlameState={points:Point[]};
const $=(s:string)=>document.querySelector(s) as HTMLElement;
let target:string|null=null,selectedInterface:string|null=null,range="6h",targets:Target[]=[];
let flame:uPlot|null=null,interfacePlot:uPlot|null=null;
const flameState:FlameState={points:[]};
let viewport:{from:number,to:number}|null=null,seriesRequest=0,interfaceRequest=0,traceRequest=0;
const ranges:Record<string,RangePreset>={"1h":{duration:3600e3,maxPoints:61},"6h":{duration:6*3600e3,maxPoints:73},"24h":{duration:86400e3,maxPoints:97},"7d":{duration:7*86400e3,maxPoints:169},"30d":{duration:30*86400e3,maxPoints:181}};
async function api(path:string){const r=await fetch(path,{headers:{accept:"application/json"}});if(!r.ok)throw new Error(await r.text());return r.json()}
function esc(s:string){const e=document.createElement("span");e.textContent=s;return e.innerHTML}
function rangeButtons(){const el=$("#ranges");el.innerHTML="";for(const n of Object.keys(ranges)){const b=document.createElement("button");b.textContent=n;b.className=n===range&&!viewport?"active":"";b.onclick=()=>{range=n;viewport=null;rangeButtons();loadSeries();if(selectedInterface)loadInterface(selectedInterface)};el.append(b)}}
function renderTargets(){const el=$("#targets");el.innerHTML=targets.map(t=>`<button class="target ${t.id===target?"active":""}" data-id="${esc(t.id)}"><i class="dot ${t.state}"></i><span><strong>${esc(t.name)}</strong><small>${esc(t.endpoint||t.address)}</small></span><span class="rtt">${t.last_rtt_ms==null?"—":t.last_rtt_ms.toFixed(1)+" ms"}</span></button>`).join("");el.querySelectorAll<HTMLButtonElement>("button").forEach(b=>b.onclick=()=>select(b.dataset.id!))}
function select(id:string){target=id;viewport=null;renderTargets();rangeButtons();const t=targets.find(x=>x.id===id)!;$("#title").textContent=t.name;$("#subtitle").textContent=t.endpoint||t.address;loadSeries();if(selectedInterface)loadInterface(selectedInterface);loadTraces()}

const flameSeriesIndex={mean:1,p50:2,p95:3,min:4,p99:5,max:6,deadline:7,noReply:8,late:9,sendError:10,schedulerGap:11,partial:12} as const;
const noPath=()=>null;
const msValue=(_u:uPlot,value:number)=>Number.isFinite(value)?`${value.toFixed(2)} ms`:"—";
const pctValue=(_u:uPlot,value:number)=>Number.isFinite(value)?`${value.toFixed(2)}%`:"—";

function flameData(points:Point[]):uPlot.AlignedData {
  const fields:(keyof Point)[]=["avg_ms","p50_ms","p95_ms","min_ms","p99_ms","max_ms","deadline_miss_pct","no_reply_pct","late_pct","send_error_pct","gap_pct","partial_marker"];
  return [points.map(point=>point.time_ms/1000),...fields.map(field=>points.map(point=>typeof point[field]==="number"?point[field] as number:null))] as uPlot.AlignedData;
}

function flameLatencyRange(_u:uPlot,min:number,max:number):uPlot.Range.MinMax {
  let renderedMax=Number.isFinite(max)?max:0;
  for(const point of flameState.points)for(const grain of point.distribution_ms??[])if(Number.isFinite(grain)&&grain>renderedMax)renderedMax=grain;
  if(!Number.isFinite(min)&&renderedMax===0)return[-0.38,1];
  const upper=Math.max(1,renderedMax*1.08);
  return[-upper*.38,upper];
}

function flameSmokePlugin(state:FlameState):uPlot.Plugin {
  const grainColors=["rgba(205,48,31,.28)","rgba(241,82,32,.34)","rgba(255,145,37,.40)","rgba(255,202,69,.48)","rgba(255,232,144,.58)"];
  const visible=(u:uPlot,index:number)=>u.series[index]?.show!==false;
  const clampPct=(value:number|undefined)=>Math.max(0,Math.min(100,value??0));
  return{hooks:{drawAxes:[u=>{
    const points=state.points;
    if(points.length===0)return;
    const {ctx,bbox}=u,ratio=uPlot.pxRatio;
    const right=bbox.left+bbox.width,bottom=bbox.top+bbox.height;
    const railHeight=Math.min(bbox.height*.27,68*ratio);
    const railTop=bottom-railHeight;
    const localHeight=11*ratio,gap=3*ratio;
    const laneHeight=(railHeight-localHeight-gap*3)/2;
    const deadlineTop=railTop+localHeight+gap;
    const noReplyTop=deadlineTop+laneHeight+gap;
    const xs=points.map(point=>u.valToPos(point.time_ms/1000,"x",true));
    const slots=xs.map((x,index)=>{
      const previous=index===0?x-(xs[1]==null?6*ratio:xs[1]-x):xs[index-1];
      const next=index===xs.length-1?x+(xs[index-1]==null?6*ratio:x-xs[index-1]):xs[index+1];
      return[Math.max(bbox.left,(previous+x)/2),Math.min(right,(x+next)/2)] as const;
    });

    ctx.save();
    ctx.beginPath();ctx.rect(bbox.left,bbox.top,bbox.width,bbox.height);ctx.clip();
    ctx.fillStyle="rgba(6,11,14,.88)";ctx.fillRect(bbox.left,railTop,bbox.width,railHeight);
    ctx.fillStyle="rgba(244,163,64,.045)";ctx.fillRect(bbox.left,deadlineTop,bbox.width,laneHeight);
    ctx.fillStyle="rgba(238,73,55,.055)";ctx.fillRect(bbox.left,noReplyTop,bbox.width,laneHeight);
    ctx.strokeStyle="rgba(150,167,176,.24)";ctx.lineWidth=ratio;
    for(const y of[railTop,deadlineTop,noReplyTop,bottom]){ctx.beginPath();ctx.moveTo(bbox.left,y);ctx.lineTo(right,y);ctx.stroke()}

    ctx.globalCompositeOperation="lighter";
    for(let pointIndex=0;pointIndex<points.length;pointIndex++){
      const distribution=points[pointIndex].distribution_ms;
      if(!distribution?.length)continue;
      const [left,slotRight]=slots[pointIndex];
      if(slotRight<=left)continue;
      const x=xs[pointIndex],grainWidth=Math.max(ratio,Math.min(7*ratio,(slotRight-left)*.78));
      const ys=distribution.map(value=>u.valToPos(value,"y",true));
      for(let grainIndex=0;grainIndex<ys.length;grainIndex++){
        const y=ys[grainIndex];
        if(y<bbox.top||y>=railTop)continue;
        let neighbors=0;
        for(let nearby=Math.max(0,grainIndex-3);nearby<=Math.min(ys.length-1,grainIndex+3);nearby++)if(Math.abs(ys[nearby]-y)<=4*ratio)neighbors++;
        ctx.fillStyle=grainColors[Math.min(grainColors.length-1,neighbors-1)];
        ctx.fillRect(x-grainWidth/2,y-1.35*ratio,grainWidth,2.7*ratio);
      }
    }
    ctx.globalCompositeOperation="source-over";

    const fillLane=(left:number,slotRight:number,top:number,value:number,color:string)=>{
      if(value<=0||slotRight<=left)return;
      const innerHeight=laneHeight-2*ratio;
      const height=Math.max(ratio,innerHeight*value/100);
      ctx.fillStyle=color;ctx.fillRect(left+.35*ratio,top+laneHeight-ratio-height,Math.max(ratio,slotRight-left-.7*ratio),height);
    };
    for(let index=0;index<points.length;index++){
      const point=points[index],[left,slotRight]=slots[index];
      const deadline=clampPct(point.deadline_miss_pct),noReply=clampPct(point.no_reply_pct);
      if(visible(u,flameSeriesIndex.deadline))fillLane(left,slotRight,deadlineTop,deadline,"rgba(244,163,64,.82)");
      if(visible(u,flameSeriesIndex.noReply))fillLane(left,slotRight,noReplyTop,noReply,noReply>=99.5?"#ee4937":"rgba(238,73,55,.86)");
      if(noReply>=99.5&&visible(u,flameSeriesIndex.noReply)&&slotRight>left){
        ctx.save();ctx.beginPath();ctx.rect(left,noReplyTop,slotRight-left,laneHeight);ctx.clip();ctx.strokeStyle="rgba(70,12,10,.72)";ctx.lineWidth=ratio;
        for(let x=left-laneHeight;x<slotRight+laneHeight;x+=5*ratio){ctx.beginPath();ctx.moveTo(x,noReplyTop+laneHeight);ctx.lineTo(x+laneHeight,noReplyTop);ctx.stroke()}
        ctx.restore();
      }
      const markerY=railTop+localHeight*.52;
      if(point.send_errors>0&&visible(u,flameSeriesIndex.sendError)){
        const size=5*ratio;ctx.strokeStyle="#aab5ba";ctx.lineWidth=1.2*ratio;ctx.strokeRect(xs[index]-size/2,markerY-size/2,size,size);
      }
      if(point.scheduler_missed>0&&visible(u,flameSeriesIndex.schedulerGap)){
        const size=3.2*ratio;ctx.strokeStyle="#82949f";ctx.lineWidth=1.2*ratio;ctx.beginPath();ctx.moveTo(xs[index]-size,markerY-size);ctx.lineTo(xs[index]+size,markerY+size);ctx.moveTo(xs[index]+size,markerY-size);ctx.lineTo(xs[index]-size,markerY+size);ctx.stroke();
      }
      if(point.partial&&visible(u,flameSeriesIndex.partial)){
        ctx.save();ctx.setLineDash([2*ratio,2*ratio]);ctx.strokeStyle="rgba(150,167,176,.45)";ctx.lineWidth=ratio;ctx.strokeRect(left+.5*ratio,railTop+.5*ratio,Math.max(0,slotRight-left-ratio),railHeight-ratio);ctx.restore();
      }
    }

    ctx.font=`${9*ratio}px ui-sans-serif,system-ui,sans-serif`;ctx.textAlign="left";ctx.textBaseline="middle";
    for(const[label,top,color]of[["DEADLINE",deadlineTop,"#dca052"],["NO REPLY",noReplyTop,"#db6a5d"]] as [string,number,string][]){
      const width=54*ratio;ctx.fillStyle="rgba(6,11,14,.82)";ctx.fillRect(bbox.left+2*ratio,top+2*ratio,width,laneHeight-4*ratio);ctx.fillStyle=color;ctx.fillText(label,bbox.left+6*ratio,top+laneHeight/2);
    }
    ctx.restore();
    const host=u.root.parentElement??u.root;
    host.dataset.flameRendered=points.some(point=>point.distribution_ms?.length)?"distribution":"loss-only";
    host.dataset.lossRail=points.some(point=>point.no_reply_pct>=99.5&&point.rtt_count===0)?"full-no-rtt":"rendered";
    host.dataset.localGaps=points.some(point=>point.send_errors>0||point.scheduler_missed>0)?"rendered":"none";
  }]}};
}

function flameGraph(old:uPlot|null,points:Point[],method:string|undefined,cap:number|undefined,zoom:(from:number,to:number)=>void){
  flameState.points=points;
  const root=$("#flame"),size={width:Math.max(280,root.clientWidth),height:360},data=flameData(points);
  root.dataset.distributionMethod=method??"";root.dataset.distributionCap=cap==null?"":String(cap);root.dataset.maxGrains=String(Math.max(0,...points.map(point=>point.distribution_ms?.length??0)));
  if(old){old.batch(()=>{if(old.width!==size.width||old.height!==size.height)old.setSize(size);old.setData(data)});return old}
  return new uPlot({...size,legend:{show:true},cursor:{drag:{x:true,y:false}},scales:{y:{range:flameLatencyRange},loss:{auto:false,range:()=>[0,100]}},axes:[{stroke:"#82949f",grid:{stroke:"#20313a"}},{scale:"y",stroke:"#82949f",grid:{stroke:"#20313a"},values:(_u,splits)=>splits.map(value=>value<0?"":uPlot.fmtNum(value))}],series:[{},
    {label:"mean RTT",stroke:"#fff0c7",width:2.6,value:msValue},
    {label:"p50",stroke:"#ffd369",width:1,value:msValue},
    {label:"p95",stroke:"#ff9d42",width:1.7,dash:[8,5],value:msValue},
    {label:"best",stroke:"#ffbf69",width:0,paths:noPath,points:{show:true,size:4,width:1,stroke:"#ffbf69",fill:"#101a20"},value:msValue},
    {label:"p99",class:"hover-only",stroke:"#f26a3d",auto:false,paths:noPath,points:{show:false},value:msValue},
    {label:"max",class:"hover-only",stroke:"#d54b35",auto:false,paths:noPath,points:{show:false},value:msValue},
    {label:"deadline miss",scale:"loss",stroke:"#f4a340",auto:false,paths:noPath,points:{show:false},value:pctValue},
    {label:"no reply",scale:"loss",stroke:"#ee4937",auto:false,paths:noPath,points:{show:false},value:pctValue},
    {label:"late reply",class:"tooltip-only",scale:"loss",stroke:"#ffc35c",auto:false,paths:noPath,points:{show:false},value:pctValue},
    {label:"local send error",scale:"loss",stroke:"#aab5ba",auto:false,paths:noPath,points:{show:false},value:pctValue},
    {label:"scheduler gap",scale:"loss",stroke:"#82949f",auto:false,paths:noPath,points:{show:false},value:pctValue},
    {label:"partial bucket",scale:"loss",stroke:"#7f8f98",auto:false,paths:noPath,points:{show:false},value:(_u,value)=>value?"partial":"—"},
  ],plugins:[flameSmokePlugin(flameState)],hooks:{setSelect:[u=>{if(u.select.width<8)return;const from=u.posToVal(u.select.left,"x")*1000,to=u.posToVal(u.select.left+u.select.width,"x")*1000;queueMicrotask(()=>zoom(from,to))}]}},data,root);
}

function graph(el:string,old:uPlot|null,points:any[],fields:string[],labels:string[],colors:string[],max?:number,zoom?:(from:number,to:number)=>void){const data:any[]=[points.map(p=>p.time_ms/1000),...fields.map(f=>points.map(p=>p[f]??null))];const root=$(el),size={width:Math.max(280,root.clientWidth),height:215};if(old){old.batch(()=>{if(old.width!==size.width||old.height!==size.height)old.setSize(size);old.setData(data as uPlot.AlignedData)});return old}return new uPlot({...size,legend:{show:true},cursor:{drag:{x:true,y:false}},scales:{y:{range:max?()=>[0,max]:undefined}},axes:[{stroke:"#82949f",grid:{stroke:"#20313a"}},{stroke:"#82949f",grid:{stroke:"#20313a"}}],series:[{},...labels.map((label,i)=>({label,stroke:colors[i],width:1.6}))],hooks:{setSelect:[u=>{if(!zoom||u.select.width<8)return;const from=u.posToVal(u.select.left,"x")*1000,to=u.posToVal(u.select.left+u.select.width,"x")*1000;queueMicrotask(()=>zoom(from,to))}]}},data,root)}
function seriesBounds(){if(viewport)return viewport;const to=Date.now();return{to,from:to-ranges[range].duration}}
function drillDown(from:number,to:number){viewport={from,to};rangeButtons();loadSeries();if(selectedInterface)loadInterface(selectedInterface)}
async function loadSeries(){if(!target)return;const bounds=seriesBounds(),to=bounds.to,from=bounds.from,maxPoints=viewport?900:ranges[range].maxPoints,id=++seriesRequest,selected=target;try{const s=await api(`/api/v1/targets/${encodeURIComponent(selected)}/ping?from=${Math.round(from)}&to=${Math.round(to)}&max_points=${maxPoints}`);if(id!==seriesRequest||selected!==target)return;const p:Point[]=s.points??[];p.forEach(x=>{x.rtt_count??=0;x.late_pct=x.sent?100*x.late/x.sent:0;x.send_error_pct=x.attempted?100*x.send_errors/x.attempted:0;x.gap_pct=x.scheduled?100*x.scheduler_missed/x.scheduled:0;x.partial_marker=x.partial?1:undefined});flame=flameGraph(flame,p,s.distribution_method,s.distribution_cap,drillDown);const last=p.at(-1)||{} as Point,sent=p.reduce((a,x)=>a+x.sent,0),miss=p.reduce((a,x)=>a+x.late+x.unanswered,0),no=p.reduce((a,x)=>a+x.unanswered,0);$("#stats").innerHTML=stat("Latest mean",last.avg_ms==null?"—":last.avg_ms.toFixed(2)+" ms")+stat("Deadline miss",sent?(100*miss/sent).toFixed(2)+"%":"—")+stat("No reply",sent?(100*no/sent).toFixed(2)+"%":"—")+stat("Packets sent",sent.toLocaleString())}catch(e){if(id===seriesRequest)$("#subtitle").textContent=(e as Error).message}}
function stat(k:string,v:string){return`<div class="stat"><label>${k}</label><strong>${v}</strong></div>`}
async function loadInterface(name:string){selectedInterface=name;const bounds=seriesBounds(),maxPoints=viewport?900:ranges[range].maxPoints,id=++interfaceRequest;try{const series=await api(`/api/v1/interfaces/${encodeURIComponent(name)}/series?from=${Math.round(bounds.from)}&to=${Math.round(bounds.to)}&max_points=${maxPoints}`);if(id!==interfaceRequest||name!==selectedInterface)return;const points:any[]=series.points??[];points.forEach((p:any)=>p.reset_marker=p.reset?1:null);$("#interface-title").textContent=name;interfacePlot=graph("#interface-chart",interfacePlot,points,["rx_mbps","tx_mbps","rx_errors","tx_errors","rx_dropped","tx_dropped","rx_missed","reset_marker"],["RX Mbps","TX Mbps","RX errors","TX errors","RX dropped","TX dropped","RX missed","RESET"],["#5bd6d0","#f0a657","#ef6b66","#b18cff","#6e9cc9","#c77d65","#82949f","#ff3158"],undefined,drillDown)}catch(e){if(id===interfaceRequest&&name===selectedInterface)$("#interface-title").textContent=`${name} · ${(e as Error).message}`}}
function routeDiff(r:any){const old=new Map<number,string>((r.old_route??[]).map((h:any)=>[h.ttl,(h.responders??[]).join(", ")]));const next=new Map<number,string>((r.new_route??[]).map((h:any)=>[h.ttl,(h.responders??[]).join(", ")]));const changed=[...new Set([...old.keys(),...next.keys()])].sort((a,b)=>a-b).filter(ttl=>old.get(ttl)!==next.get(ttl));const hops=changed.map(ttl=>`${ttl}: ${old.get(ttl)||"*"} → ${next.get(ttl)||"*"}`).join("; ");const reached=r.old_reached_hop!==r.new_reached_hop?`reached ${r.old_reached_hop||"no"} → ${r.new_reached_hop||"no"}`:"";return [hops,reached].filter(Boolean).join(" · ")||"hop set changed"}
async function loadTraces(){if(!target)return;const id=++traceRequest,selected=target;const[rawTraces,rawChanges]=await Promise.all([api(`/api/v1/targets/${encodeURIComponent(selected)}/traces?limit=12`),api(`/api/v1/route-changes?target=${encodeURIComponent(selected)}&limit=6`)]);if(id!==traceRequest||selected!==target)return;const traces:any[]=rawTraces??[],changes:any[]=rawChanges??[];const el=$("#routes");const changed=changes.map((r:any)=>`<div class="row"><strong>Route changed</strong><span>${new Date(r.confirmed_ms).toLocaleString()} · ${esc(routeDiff(r))}</span></div>`).join("");const runs=traces.map((r:any)=>`<button class="row trace-row" data-id="${r.id}"><strong>${new Date(r.started_ms).toLocaleString()}</strong><span>${esc(r.method)} · ${r.reached?"reached hop "+r.reached_hop:esc(r.error_detail||r.status)}</span></button>`).join("");el.innerHTML=changed+runs||"<div class=empty>No trace runs yet</div>";el.querySelectorAll<HTMLButtonElement>("button").forEach(b=>b.onclick=()=>loadTrace(Number(b.dataset.id)))}
async function loadTrace(id:number){const trace=await api(`/api/v1/traces/${id}`);const hops=new Map<number,string[]>();for(const p of trace.probes??[]){const values=hops.get(p.ttl)||[];values.push(p.responder?`${p.responder}${p.rtt_ms==null?"":` (${p.rtt_ms.toFixed(1)} ms)`}`:"*");hops.set(p.ttl,values)}$("#trace-detail").innerHTML=[...hops].map(([ttl,v])=>`<div class="row"><strong>${ttl}</strong><span>${v.map(esc).join(" · ")}</span></div>`).join("")||`<div class=empty>${esc(trace.error_detail||"No probe replies")}</div>`}
async function refresh(){try{targets=(await api("/api/v1/targets"))??[];renderTargets();if(!target&&targets.length)select(targets[0].id);else if(target){loadSeries();loadTraces()}const ifs:any[]=(await api("/api/v1/interfaces"))??[];const interfaces=$("#interfaces");interfaces.innerHTML=ifs.length?ifs.map((i:any)=>`<button class="row trace-row" data-name="${esc(i.name)}"><strong>${esc(i.display_name)}</strong><span>${i.present?esc(i.mac||"observed"):"missing"}</span></button>`).join(""):"<div class=empty>No interfaces configured</div>";interfaces.querySelectorAll<HTMLButtonElement>("button").forEach(b=>b.onclick=()=>loadInterface(b.dataset.name!));if(selectedInterface)loadInterface(selectedInterface);const status=await api("/api/v1/status");const traceState=Object.entries(status.trace_capabilities??{}).filter(([,v])=>v!=="available"&&v!=="disabled").map(([k,v])=>`${k} ${v}`);if(status.interface_capability?.startsWith("unavailable"))traceState.push(`interfaces ${status.interface_capability}`);$("#storage").textContent=`Storage ${status.pressure} · ${formatBytes(status.live_bytes)} live · ${status.dirty_buckets} dirty buckets${traceState.length?" · "+traceState.join(" · "):""}`;$("#health").textContent=status.ready?"ready":"paused";$("#health").className=status.ready?"pill ready":"pill";$("#health").title=status.writer_error||`${formatBytes(status.available_bytes)} filesystem space available`}catch{$("#health").textContent="unavailable";$("#health").className="pill"}}
function formatBytes(n:number){for(const[u,v]of[["TiB",2**40],["GiB",2**30],["MiB",2**20]] as [string,number][]){if(n>=v)return(n/v).toFixed(1)+" "+u}return n+" B"}
rangeButtons();refresh();setInterval(refresh,5000);addEventListener("resize",()=>loadSeries());
