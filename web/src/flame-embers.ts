import uPlot from "uplot";
import {formatMs,type Point,type SkyLayout} from "./flame-scale";

export type Ember={index:number,value:number,count:number,late:boolean,severity:number,mild:boolean};
export type TailGrain={index:number,value:number,late:boolean,alpha:number};
export type Box=[number,number,number,number];
const overlaps=(a:Box,b:Box)=>a[0]<b[0]+b[2]&&b[0]<a[0]+a[2]&&a[1]<b[1]+b[3]&&b[1]<a[1]+a[3];

const keyline="rgba(6,11,14,.85)";
const trailWidth=(count:number)=>count>=4?4:count>=2?3:1.8,coreRadius=(count:number)=>count>=4?2.2:count>=2?1.8:1.3;

// AIDEV-NOTE: ember severity mapping. A tail value above the seam in sky mode is an ember with severity sev = ln(v/S) / ln(max(top/S, 20)), clamped to [0,1]:
// how far up the log sky it sits. Severity drives brightness, size, trail, and glow rather than grain density because an outlier is one or a few replies;
// density would draw a 4 s ping as the same faint speck as a 30 ms one. The fixed 20× floor stops a short sky (top barely past 3× the seam) from making a
// modest excursion look maximal. The mild gate (on time and sev < .35, i.e. under ~2.85× the seam on a 20× sky) keeps near-seam values as plain tail grains
// so only genuinely slow or late replies flame. Mild embers stay in `embers` (flagged) for the ×n label rule and are also drawn from `grains`.
// AIDEV-NOTE: `late` is the server's per-bin flag (query.go), never derived from the value: a ~2%-wide bin's midpoint can sit past the deadline
// while it holds only on-time replies, and a red spark must never imply a late ping that did not happen. A bin mixing late and on-time replies arrives unflagged.
export function classifyTail(points:Point[],layout:SkyLayout):{embers:Ember[],grains:TailGrain[]}{
  const embers:Ember[]=[],grains:TailGrain[]=[],{sky,seam,top}=layout,span=Math.log(Math.max(top/seam,20));
  points.forEach((point,index)=>{for(const bin of point.tail??[]){
    const{ms:value,n:count}=bin,late=bin.late===true;
    if(!sky||value<=seam){grains.push({index,value,late,alpha:late?.85:.7});continue}
    const severity=Math.max(0,Math.min(1,Math.log(value/seam)/span)),mild=!late&&severity<.35;
    embers.push({index,value,count,late,severity,mild});if(mild)grains.push({index,value,late,alpha:.55+.6*severity});
  }});
  return{embers,grains};
}

// Draws trails, then tail grains, then glowing cores, then labels; every mark sits on a dark keyline underlay so a line beneath it cannot merge with it.
// `obstacles` are boxes no label may cover (the seam and deadline captions). Pixel constants are CSS px scaled by uPlot.pxRatio. Returns the number of labels drawn.
export function drawEmbers(u:uPlot,points:Point[],slots:readonly (readonly [number,number])[],result:ReturnType<typeof classifyTail>,layout:SkyLayout,obstacles:readonly Box[]=[]):number{
  const{ctx,bbox}=u,ratio=uPlot.pxRatio,right=bbox.left+bbox.width,floor=u.valToPos(0,"y",true),k=.75*ratio;
  const x=(index:number)=>u.valToPos(points[index].time_ms/1000,"x",true),y=(value:number)=>u.valToPos(value,"y",true),slot=(index:number)=>slots[index][1]-slots[index][0];
  // The worst ember usually sits a few px under the top (top = niceCeil(1.15 × max)), so the clip opens upward into uPlot's empty top padding
  // and its core and glow are never cut off; sideways it stays inside the plot, clear of the axis labels.
  ctx.save();ctx.beginPath();ctx.rect(bbox.left,0,bbox.width,bbox.top+bbox.height);ctx.clip();
  // Every drawn core and tail grain is recorded with its keyline box so a label backdrop can avoid covering it.
  const marks:{index:number,box:Box}[]=[];
  // Buckets whose slot is clamped away (centre outside the plot) draw nothing and cannot take one of the three value labels.
  const onPlot=result.embers.filter(ember=>slot(ember.index)>0),flames=onPlot.filter(ember=>!ember.mild);
  // A trail tapers from the top of its bucket's smoke (p95, or the seam when p95 is above it) to the ember and fades in toward the tip.
  // AIDEV-NOTE: keylines go down in their own pass before any coloured trail, and core keylines before any core. Drawn per ember, a later ember's
  // keyline darkened the trails and glow already drawn in the same bucket, so stacked 4–5 s trails read about half as bright as the approved mockup.
  // Tail grains (keyline and fill) go after every trail and before any core for the same reason: drawn first, a trail or its keyline dimmed the grains in its bucket.
  const trail=(ember:Ember,rgb:string,a:number,pad:number)=>{
    const point=points[ember.index],cx=x(ember.index),tip=y(ember.value),base=y(Math.min(point.p95_ms??layout.seam,layout.seam)),mid=(base+tip)/2;
    const width=Math.min(slot(ember.index)*.55,(trailWidth(ember.count)+1.6*ember.severity)*ratio)+2*pad,half=.6*ratio+pad;
    const gradient=ctx.createLinearGradient(0,base,0,tip);gradient.addColorStop(0,`rgba(${rgb},0)`);gradient.addColorStop(.6,`rgba(${rgb},${(a*.5).toFixed(3)})`);gradient.addColorStop(1,`rgba(${rgb},${a.toFixed(3)})`);
    ctx.fillStyle=gradient;ctx.beginPath();ctx.moveTo(cx-width/2,base);ctx.quadraticCurveTo(cx-width*.15,mid,cx-half,tip);ctx.lineTo(cx+half,tip);ctx.quadraticCurveTo(cx+width*.15,mid,cx+width/2,base);ctx.closePath();ctx.fill();
  };
  for(const ember of flames)trail(ember,"6,11,14",.85,k);
  for(const ember of flames)trail(ember,ember.late?"255,90,46":"255,157,66",.2+.7*ember.severity,0);
  for(const grain of result.grains){
    const cy=y(grain.value),width=slot(grain.index);if(width<=0||cy<bbox.top||cy>floor)continue;
    const w=Math.max(ratio,Math.min(7*ratio,width*.78)),h=4*ratio,left=x(grain.index)-w/2;
    marks.push({index:grain.index,box:[left-k,cy-h/2-k,w+2*k,h+2*k]});
    ctx.fillStyle=keyline;ctx.fillRect(left-k,cy-h/2-k,w+2*k,h+2*k);ctx.fillStyle=`rgba(${grain.late?"255,120,90":"255,190,110"},${grain.alpha.toFixed(3)})`;ctx.fillRect(left,cy-h/2,w,h);
  }
  const cores=flames.map(ember=>{const rx=(coreRadius(ember.count)+1.1*ember.severity)*ratio;return{ember,cx:x(ember.index),cy:y(ember.value),rx,ry:rx*1.7}});
  for(const{ember,cx,cy,rx,ry}of cores){
    marks.push({index:ember.index,box:[cx-rx-k,cy-ry-k,2*(rx+k),2*(ry+k)]});
    ctx.fillStyle=keyline;ctx.beginPath();ctx.ellipse(cx,cy,rx+k,ry+k,0,0,Math.PI*2);ctx.fill();
  }
  for(const{ember,cx,cy,rx,ry}of cores){
    ctx.save();ctx.shadowBlur=(3+9*ember.severity+(ember.late?3:0))*ratio;ctx.shadowColor=ember.late?"#ff3b1e":"#ff8a2e";ctx.globalAlpha=.7+.3*ember.severity;
    ctx.fillStyle=ember.severity>.66?"#fff6ea":ember.severity>.33?"#ffdcae":"#ffb86a";ctx.beginPath();ctx.ellipse(cx,cy,rx,ry,0,0,Math.PI*2);ctx.fill();ctx.restore();
  }
  // The value goes on the highest ember of each of the three worst buckets (exact worst reply when known, else "≈" the bin value); clusters add "×n"
  // when their slot is at least 10 px. Labels sit beside the ember on a dark backdrop, right side first, else left.
  // AIDEV-NOTE: a side is rejected when it leaves the plot, overlaps a placed label or an obstacle (the "LOG SCALE ABOVE" and "DEADLINE" captions), or
  // would cover another bucket's ember core or tail grain: at 6h the "5.1 s" backdrop otherwise hid the lone 4.1 s ember one slot to its right, and in a
  // drag-zoom a "1.0 s" label sat on the deadline caption. A label that fits neither side is skipped; hover still has the value.
  const ranked=onPlot.sort((a,b)=>b.value-a.value),valued=new Set<Ember>(),buckets=new Set<number>(),placed:Box[]=[];
  for(const ember of ranked){if(ember.mild||buckets.has(ember.index))continue;buckets.add(ember.index);valued.add(ember);if(valued.size===3)break}
  ctx.font=`${9.5*ratio}px ui-sans-serif,system-ui,sans-serif`;ctx.textAlign="left";ctx.textBaseline="middle";
  for(const ember of ranked){
    const worst=points[ember.index].worst,parts:string[]=[];
    if(valued.has(ember))parts.push(worst?.length?formatMs(worst[0].rtt_ms):"≈"+formatMs(ember.value));
    if(slot(ember.index)>=10*ratio&&(ember.count>=3||ember.count>=2&&!ember.mild))parts.push("×"+ember.count);
    if(!parts.length)continue;
    const text=parts.join(" "),width=ctx.measureText(text).width,cx=x(ember.index),cy=Math.max(bbox.top+6*ratio,y(ember.value));
    const box=[cx+6*ratio,cx-6*ratio-width].map((left):Box=>[left-2*ratio,cy-6*ratio,width+4*ratio,12*ratio]).find(box=>box[0]>=bbox.left&&box[0]+box[2]<=right
      &&!placed.some(other=>overlaps(box,other))&&!obstacles.some(other=>overlaps(box,other))&&!marks.some(mark=>mark.index!==ember.index&&overlaps(box,mark.box)));
    if(!box)continue;
    placed.push(box);ctx.fillStyle="rgba(6,11,14,.75)";ctx.fillRect(...box);ctx.fillStyle=valued.has(ember)?"#dbe5ed":"#aebfc9";ctx.fillText(text,box[0]+2*ratio,cy);
  }
  ctx.restore();
  return placed.length;
}
