import type uPlot from "uplot";
import {formatMs,type Point} from "./flame-scale";

const esc=(s:string)=>s.replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]!));
const clock=(ms:number)=>new Date(ms).toLocaleTimeString([],{hour:"2-digit",minute:"2-digit",second:"2-digit"});
const pct=(n:number,of:number)=>of>0?` (${(100*n/of).toFixed(1)}%)`:"";
const row=(label:string,value:string)=>`<div class="ft-row"><span>${esc(label)}</span><b>${esc(value)}</b></div>`;

// Escaped HTML for one bucket: counts and percentiles first, then the exact worst pings (or, without them, the first six tail bins).
export function bucketTooltip(point:Point,bucketMs:number):string{
  const ms=(v:number|undefined)=>v==null?"—":formatMs(v),end=bucketMs>0?` – ${clock(point.time_ms+bucketMs)}`:"";
  let html=`<div class="ft-head">${esc(clock(point.time_ms)+end)} · ${esc(point.sent.toLocaleString())} sent</div>`;
  html+=row("p50 / p95",`${ms(point.p50_ms)} / ${ms(point.p95_ms)}`)+row("p99 / max",`${ms(point.p99_ms)} / ${ms(point.max_ms)}`);
  html+=row("late",`${point.late}${pct(point.late,point.sent)}`)+row("no reply",`${point.unanswered}${pct(point.unanswered,point.sent)}`);
  if(point.send_errors>0)html+=row("local send errors",String(point.send_errors));
  if(point.worst?.length)html+=`<div class="ft-sec">worst pings</div>`+point.worst.map(w=>row(clock(w.time_ms),formatMs(w.rtt_ms)+(w.late?" late":""))).join("");
  else if(point.tail?.length)html+=`<div class="ft-sec">slowest replies</div>`+point.tail.slice(0,6).map(t=>row(`≈${formatMs(t.ms)}`,`×${t.n}`)).join("")+(point.tail.length>6||point.tail_truncated?`<div class="ft-more">…</div>`:"");
  if(point.misses?.length)html+=`<div class="ft-sec">no reply at</div><div class="ft-times">${esc(point.misses.map(clock).join(", "))}${point.unanswered>point.misses.length?" …":""}</div>`;
  return html;
}

// Tooltip element lives inside the chart root; styling is class-based because CSP forbids inline style attributes (element.style.* property writes are allowed).
export function flameTooltipPlugin(points:()=>Point[],bucketMs:()=>number):uPlot.Plugin{
  let tip:HTMLDivElement;
  const hide=()=>{tip.hidden=true};
  return{hooks:{
    init:[u=>{tip=document.createElement("div");tip.id="flame-tooltip";tip.className="flame-tooltip";tip.hidden=true;tip.setAttribute("role","status");u.root.append(tip);u.over.addEventListener("mouseleave",hide)}],
    setCursor:[u=>{
      const idx=u.cursor.idx,point=idx==null?undefined:points()[idx],left=u.cursor.left??-1;
      if(!point||left<0){hide();return}
      tip.innerHTML=bucketTooltip(point,bucketMs());tip.hidden=false;
      const rw=u.root.clientWidth,rh=u.root.clientHeight,tw=tip.offsetWidth,th=tip.offsetHeight,x=u.over.offsetLeft+left,y=u.over.offsetTop+(u.cursor.top??0);
      tip.style.left=`${Math.max(0,Math.min(rw-tw,x+14+tw>rw?x-14-tw:x+14))}px`;tip.style.top=`${Math.max(0,Math.min(rh-th,y+14))}px`;
    }],
  }};
}
