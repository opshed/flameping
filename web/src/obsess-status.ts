export type ObsessStatus={enabled:boolean,monitoring:boolean,state:string,interval_ms:number,reason?:string,baseline_ms?:number,threshold_ms?:number,baseline_samples:number,healthy_for_ms:number,recover_after_ms:number,since_ms?:number,tracking_limited?:boolean};

const duration=(ms:number)=>ms<1000?`${Number(ms.toFixed(2))} ms`:`${Number((ms/1000).toFixed(1))} s`;

export function obsessLabel(status:ObsessStatus):string {
  if(!status.monitoring)return "Obsess paused";
  if(status.state==="obsessing")return `Obsessing · ${duration(status.interval_ms)}`;
  return status.state==="warming"?"Obsess learning":"Obsess ready";
}

export function renderObsessStatus(element:HTMLElement,status?:ObsessStatus,stale=false){
  element.hidden=!status?.enabled;
  element.replaceChildren();
  delete element.dataset.state;
  if(!status?.enabled)return;
  element.dataset.state=stale?"stale":status.monitoring?status.state:"paused";
  const heading=document.createElement("strong"),detail=document.createElement("span");
  heading.textContent=stale?"Obsess status unavailable · last update shown":obsessLabel(status);
  if(status.tracking_limited){
    heading.textContent="Obsess observation limit reached";
    detail.textContent="Some live evidence was unavailable. This notice clears after a full healthy recovery or an endpoint reset.";
  }else if(!status.monitoring){
    detail.textContent="Probing is stopped. The saved live state will reset when the monitor restarts.";
  }else if(status.state==="obsessing"){
    const cause=status.reason==="loss"?"a missed ping deadline":"a latency spike";
    const threshold=status.threshold_ms==null?"on-time replies (no pre-event latency baseline)":`on-time replies at or below ${duration(status.threshold_ms)}`;
    const healthy=Math.min(status.healthy_for_ms,status.recover_after_ms);
    detail.textContent=`Triggered by ${cause}. Healthy observations: ${duration(healthy)} / ${duration(status.recover_after_ms)}. Recovery requires ${threshold}.${status.baseline_ms==null?"":` Pre-event average: ${duration(status.baseline_ms)}.`}`;
  }else if(status.state==="warming"){
    detail.textContent=`Probing every ${duration(status.interval_ms)} · learning the latency baseline (${status.baseline_samples} replies). Packet loss already triggers faster probing.`;
  }else{
    detail.textContent=`Probing every ${duration(status.interval_ms)}. Faster probing starts on packet loss${status.threshold_ms==null?"":` or RTT above ${duration(status.threshold_ms)}`}.`;
  }
  element.append(heading,detail);
}
