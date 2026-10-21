export type Point={time_ms:number,scheduled:number,attempted:number,sent:number,on_time?:number,late:number,unanswered:number,send_errors:number,scheduler_missed:number,rtt_count:number,avg_ms?:number,min_ms?:number,p50_ms?:number,p95_ms?:number,p99_ms?:number,max_ms?:number,distribution_ms?:number[],timeout_min_ms?:number,timeout_max_ms?:number,tail?:{ms:number,n:number}[],tail_truncated?:boolean,worst?:{time_ms:number,rtt_ms:number,late:boolean}[],misses?:number[],deadline_miss_pct:number,no_reply_pct:number,partial?:boolean,late_pct?:number,send_error_pct?:number,gap_pct?:number,partial_marker?:number};
export type FlameWindow={rtt_count:number,p50_ms?:number,p95_ms?:number,p99_ms?:number,max_ms?:number,seam_ms?:number};
export type SkyLayout={sky:boolean,seam:number,top:number};
export type FlameSplit={value:number,kind:"body"|"seam"|"sky"};

const body=.62; // body share of the latency area; the sky gets the remaining 38%
const nice=[1,1.2,1.5,2,2.5,3,4,5,6,8];
const clean=(v:number)=>+v.toPrecision(12);

// Mirrors the server's niceCeil (query.go) for positive x; non-positive x yields 1 so an empty chart keeps a unit axis.
export function niceCeil(x:number):number{if(!(x>0)||!Number.isFinite(x))return 1;const scale=10**Math.floor(Math.log10(x));for(const step of nice)if(x<=step*scale*(1+1e-9))return clean(step*scale);return clean(10*scale)}

// The server owns the seam (window.seam_ms); the client only decides whether the window's max reaches far enough above it to need a sky.
// The seam is cleaned like the ticks so float noise (0.30000000000000004) cannot sit a hair above a body tick and duplicate it and its grid line.
export function skyLayout(window:FlameWindow|undefined,fallbackMax:number):SkyLayout{const seam=clean(window?.seam_ms??0),max=window?.max_ms??fallbackMax,sky=seam>0&&max>=3*seam;return{sky,seam,top:niceCeil((sky?1.15:1.08)*max)}}

// AIDEV-NOTE: piecewise latency scale. fwd maps ms to "latency units" where the latency area is [0,1]: linear 0→top without a sky; with a sky,
// linear 0→seam fills the lower 62% and log10 seam→top fills the upper 38%. Below zero (the loss rail reservation, [-0.38,0]) stays linear
// in both modes, so bwd(-.38) is the scale minimum. flameBwd must stay the exact inverse of flameFwd or cursor/zoom positions drift.
export function flameFwd(layout:SkyLayout,v:number):number{const{sky,seam,top}=layout;return!sky?v/top:v<=seam?v/seam*body:body+(Math.log10(v)-Math.log10(seam))/(Math.log10(top)-Math.log10(seam))*(1-body)}
export function flameBwd(layout:SkyLayout,u:number):number{const{sky,seam,top}=layout;return!sky?u*top:u<=body?u/body*seam:10**(Math.log10(seam)+(u-body)/(1-body)*(Math.log10(top)-Math.log10(seam)))}

function linearTicks(max:number,count:number){const raw=max/count,scale=10**Math.floor(Math.log10(raw)),m=raw/scale,step=(m<=1?1:m<=2?2:m<=2.5?2.5:m<=5?5:10)*scale,ticks:number[]=[];for(let i=0;i*step<=max*(1+1e-9);i++)ticks.push(clean(i*step));return ticks}
function logTicks(low:number,high:number){const ticks:number[]=[];for(let e=Math.floor(Math.log10(low));e<=Math.ceil(Math.log10(high));e++)for(const m of[1,2,5]){const v=clean(m*10**e);if(v>low*1.02&&v<=high*(1+1e-9))ticks.push(v)}return ticks}

// Candidate y ticks before pixel filtering; the caller drops sky ticks that land within 12 px of the seam.
export function flameSplits(layout:SkyLayout):FlameSplit[]{
  if(!layout.sky)return linearTicks(layout.top,5).map(value=>({value,kind:"body"}));
  return[...linearTicks(layout.seam,4).filter(value=>value<layout.seam).map(value=>({value,kind:"body" as const})),{value:layout.seam,kind:"seam"},...logTicks(layout.seam,layout.top).map(value=>({value,kind:"sky" as const}))];
}

export function formatMs(v:number):string{return v>=1000?(v/1000).toFixed(v>=1e4?0:1)+" s":v>=10?Math.round(v)+" ms":v>=1?v.toFixed(1)+" ms":clean(+v.toFixed(2))+" ms"}
export function formatTick(v:number):string{return v>=1000?clean(+(v/1000).toFixed(1))+"k":String(clean(+v.toFixed(3)))}
