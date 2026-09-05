import {createServer} from "node:http";
import {readFile, writeFile, mkdtemp, rm} from "node:fs/promises";
import {spawn} from "node:child_process";
import {join, extname} from "node:path";
import {tmpdir} from "node:os";

const dist = new URL("../internal/webui/dist/", import.meta.url).pathname;
const json = value => JSON.stringify(value);
let status = {ready:true, pressure:"normal", live_bytes:0, dirty_buckets:0, available_bytes:1_000_000, trace_capabilities:{ipv4:"disabled",ipv6:"disabled"}, interface_capability:"disabled"};
let fixtureMode = false;
let interfacePresetFailureUsed = false;
let interfaceZoomFailureUsed = false;
let interfaceZoomFailureArmed = false;
const requestLog = [];
const routeWindows = new Map();
let routeMode = "normal", nextHistoryAction = "", nextTraceAction = "", nextPingAction = "";
let delayedHistory = 0, delayedTrace = 0;
const pendingDelays = new Set();
let lastHistory;

const fixtures = {
  targets: [
    {id:"gateway", name:"Gateway", address:"192.0.2.1", endpoint:"192.0.2.1", state:"up", last_rtt_ms:2.4},
    {id:"backup", name:"Backup", address:"198.51.100.8", endpoint:"198.51.100.8", state:"degraded", last_rtt_ms:18.7},
  ],
  interfaces: [
    {name:"eth0", display_name:"Ethernet 0", mac:"02:00:00:00:00:01", present:true},
    {name:"wan0", display_name:"Vanished uplink", mac:"02:00:00:00:00:02", present:false},
  ],
};

function points(from, to) {
  const step = (to - from) / 20;
  return Array.from({length:21}, (_, i) => {
    const noReplyOnly = i === 3;
    const sendErrors = i === 7 ? 4 : 0;
    const schedulerMissed = i === 12 ? 8 : 0;
    const attempted = 60;
    const sent = attempted - sendErrors;
    const late = noReplyOnly ? 0 : i % 7 === 0 ? 2 : 0;
    const unanswered = noReplyOnly ? sent : i % 11 === 0 ? 1 : 0;
    const distribution = noReplyOnly ? undefined : Array.from({length:17}, (_, grain) => grain < 11 ? 1.55+i*.08+grain*.055 : 8.4+i*.12+(grain-11)*.42);
    const point = {
      time_ms:Math.round(from+step*i), scheduled:attempted+schedulerMissed, attempted, sent,
      late, unanswered, send_errors:sendErrors, scheduler_missed:schedulerMissed,
      rtt_count:noReplyOnly?0:sent-unanswered, timeout_max_ms:5000,
      deadline_miss_pct:sent?100*(late+unanswered)/sent:0,
      no_reply_pct:sent?100*unanswered/sent:0,
      partial:i===20,
    };
    if (distribution) Object.assign(point, {
      avg_ms:4.75+i*.1, min_ms:1.35+i*.08, p50_ms:1.95+i*.08,
      p95_ms:10.25+i*.12, p99_ms:13.5+i*.15, max_ms:19+i*.2,
      distribution_ms:distribution,
    });
    return point;
  });
}

const zeroCounts = () => ({traces:0,reached:0,unreached:0,errors:0,changes:0});
function routeFixture(target, from, to, maxPoints) {
  const key = `${target}:${from}:${to}`;
  if (routeMode === "normal" && routeWindows.has(key)) return routeWindows.get(key);
  const base = target === "gateway" ? 100 : 200;
  const endpoint = target === "gateway" ? "192.0.2.1" : "198.51.100.8";
  const length = Math.min(16, maxPoints || 16);
  const buckets = Array.from({length}, (_, index) => ({
    from_ms:Math.round(from+(to-from)*index/length), to_ms:Math.round(from+(to-from)*(index+1)/length),
    ...zeroCounts(), ...(routeMode === "empty" ? {} : index === 4 ? {traces:330,reached:310,unreached:15,errors:5,changes:121} : index === 8 ? {traces:30,reached:20,unreached:5,errors:5,changes:6} : {}),
  }));
  const records = bucket => {
    const at = fraction => Math.round(bucket.from_ms+(bucket.to_ms-bucket.from_ms)*fraction);
    const trace = (id, fraction, fields={}) => ({id:base+id,started_ms:at(fraction),ended_ms:at(fraction)+1000,endpoint,method:"udp",flow_id:`flow-${target}`,status:"completed",reached:true,reached_hop:5,...fields});
    const traces = bucket.traces ? [trace(3,.85),trace(2,.65,{reached:false,reached_hop:0}),trace(1,.4,{status:"timed_out",reached:false,reached_hop:0,error_detail:"Trace deadline expired"})] : [];
    const change = (id, fraction) => ({
      id:base+id,confirmed_ms:at(fraction),first_seen_ms:at(fraction-.1),endpoint,method:"udp",flow_id:`flow-${target}`,
      old_reached_hop:4,new_reached_hop:5,old_trace_id:base+1,candidate_trace_id:base+2,confirming_trace_id:base+3,
      old_route:[{ttl:1,responders:["192.0.2.2"]},{ttl:2,responders:["192.0.2.3"]},{ttl:3,responders:["203.0.113.1","203.0.113.2"]},{ttl:4,responders:[]}],
      new_route:[{ttl:1,responders:["192.0.2.2"]},{ttl:2,responders:["198.51.100.2"]},{ttl:3,responders:["203.0.113.2","203.0.113.3"]},{ttl:4,responders:[endpoint]}],
    });
    const changes = bucket.changes ? [change(13,.9),change(12,.6),change(11,.3)] : [];
    return {traces,changes};
  };
  for (const bucket of buckets) {
    const {from_ms,to_ms,...totals} = bucket;
    const rows = records(bucket);
    routeWindows.set(`${target}:${from_ms}:${to_ms}`,{from_ms,to_ms,bucket_ms:to_ms-from_ms,totals,buckets:[bucket],...rows,traces_truncated:totals.traces>rows.traces.length,changes_truncated:totals.changes>rows.changes.length});
  }
  const totals = buckets.reduce((total, bucket) => Object.fromEntries(Object.keys(total).map(field => [field,total[field]+bucket[field]])),zeroCounts());
  const rows = records(buckets[4]);
  return {from_ms:from,to_ms:to,bucket_ms:Math.ceil((to-from)/length),totals,buckets,...rows,traces_truncated:totals.traces>rows.traces.length,changes_truncated:totals.changes>rows.changes.length};
}

function fixtureResponse(url) {
  const path = url.pathname;
  if (path === "/api/v1/targets") return fixtures.targets;
  if (path === "/api/v1/interfaces") return fixtures.interfaces;
  if (/^\/api\/v1\/targets\/[^/]+\/ping$/.test(path)) {
    const from = Number(url.searchParams.get("from"));
    const to = Number(url.searchParams.get("to"));
    return {distribution_method:"midpoint_quantiles", distribution_cap:17, points:points(from, to)};
  }
  const history = path.match(/^\/api\/v1\/targets\/([^/]+)\/route-history$/);
  if (history) return routeFixture(history[1],Number(url.searchParams.get("from")),Number(url.searchParams.get("to")),Number(url.searchParams.get("max_points")));
  const rawTrace = path.match(/^\/api\/v1\/traces\/(\d+)$/);
  if (rawTrace) {
    const id = Number(rawTrace[1]), target = id < 200 ? "gateway" : "backup";
    return {id,target_id:target,started_ms:Date.now()-30_000,ended_ms:Date.now()-29_000,endpoint:fixtures.targets.find(item=>item.id===target).endpoint,method:"udp",flow_id:`flow-${target}`,status:"completed",reached:true,reached_hop:5,probes:[
      {ttl:1,index:0,responder:"192.0.2.2",rtt_ms:1.2,icmp_type:11,icmp_code:0},
      {ttl:2,index:0,responder:"203.0.113.9",rtt_ms:9.8,icmp_type:11,icmp_code:0},
      {ttl:2,index:1},{ttl:2,index:2,responder:"2001:db8:1234:5678:90ab:cdef:1234:5678",rtt_ms:0,icmp_type:3,icmp_code:3},
    ]};
  }
  if (path === "/api/v1/interfaces/eth0/series") {
    const from = Number(url.searchParams.get("from"));
    const to = Number(url.searchParams.get("to"));
    return {points:[
      {time_ms:from,rx_mbps:10,tx_mbps:2,rx_errors:0,tx_errors:0,rx_dropped:0,tx_dropped:0,rx_missed:0,reset:false},
      {time_ms:to,rx_mbps:14,tx_mbps:3,rx_errors:1,tx_errors:0,rx_dropped:0,tx_dropped:0,rx_missed:0,reset:false},
    ]};
  }
  return undefined;
}

const automation = `<script>
(() => {
  document.body.dataset.smoke = "registered";
  const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
  const until = async (test, label="browser smoke automation") => { for (let i=0; i<60; i++) { const value=await test(); if (value) return value; await wait(100); } throw new Error(label + " timed out"); };
  const assert = (condition, message) => { if (!condition) throw new Error(message); };
  const control = fields => fetch("/__smoke/control?" + new URLSearchParams(fields)).then(response=>response.json());
  const routes = () => document.querySelector("#route-history");
  const buckets = () => [...document.querySelectorAll("#route-buckets button")];
  const inspector = () => document.querySelector("#route-inspector");
  const routeReady = () => until(()=>routes().dataset.routeTraces === "360" && !routes().hasAttribute("aria-busy"), "route overview");
  const alignedDomains = (withInterface=false) => until(()=>["#flame",...(withInterface?["#interface-chart"]:[])].every(selector=>["from","to"].every(edge=>Math.abs(Number(document.querySelector(selector).dataset[edge])-Number(routes().dataset[edge]))<=1)), "rendered chart time domains");
  const switchTarget = async target => { document.querySelector("#targets button[data-id="+target+"]").click(); await routeReady(); };
  const inspectDense = async () => { buckets()[4].click(); await until(()=>document.querySelector("#route-records [data-change]"), "dense interval records"); };
  const showChange = () => document.querySelector("#route-records [data-change]").click();
  addEventListener("load", async () => {
    try {
    document.body.dataset.smoke = "loaded";
    await until(() => [...document.querySelectorAll("#ranges button")].find(button => button.textContent === "24h"));
    const combined = await until(() => document.querySelector("#targets button[data-id=gateway]") && document.querySelector("#flame .u-over") && document.querySelector("#flame"));
    if (document.querySelector("#latency") || document.querySelector("#loss") || document.querySelectorAll("#flame .uplot").length !== 1) throw new Error("latency and loss were not consolidated into one plot");
    if (combined.getAttribute("role") !== "group") throw new Error("combined chart does not preserve legend descendant semantics");
    if (combined.dataset.distributionMethod !== "midpoint_quantiles" || combined.dataset.distributionCap !== "17" || combined.dataset.maxGrains !== "17") throw new Error("distribution metadata or adaptive grains are missing");
    await until(() => combined.dataset.flameRendered === "distribution" && combined.dataset.lossRail === "full-no-rtt" && combined.dataset.localGaps === "rendered");
    const fixtureSeries = await fetch("/api/v1/targets/gateway/ping?from=0&to=100000&max_points=61").then(response => response.json());
    const bimodal = fixtureSeries.points.find(point => point.distribution_ms?.some((value, index, values) => index > 0 && value-values[index-1] > 4));
    if (!bimodal || fixtureSeries.distribution_method !== "midpoint_quantiles" || fixtureSeries.distribution_cap !== 17) throw new Error("bimodal midpoint-quantile fixture is missing");
    for (const label of ["1h", "6h", "7d", "30d", "24h"]) {
      document.body.dataset.smokeStage = "preset " + label;
      [...document.querySelectorAll("#ranges button")].find(button => button.textContent === label).click();
      await wait(150);
      await alignedDomains();
    }
    document.querySelector("#targets button[data-id=backup]").click();
    await wait(300);
    document.querySelector("#interfaces button[data-name=eth0]").click();
    await wait(300);
    [...document.querySelectorAll("#ranges button")].find(button => button.textContent === "1h").click();
    await until(() => document.querySelector("#interface-title").textContent.includes("injected interface failure"));
    [...document.querySelectorAll("#ranges button")].find(button => button.textContent === "24h").click();
    await until(() => document.querySelector("#interface-title").textContent === "eth0");
    await routeReady();
    document.body.dataset.smokeStage = "route overview";
    await alignedDomains(true);
    assert(routes().dataset.routeChanges === "127", "route totals undercounted dense changes");
    assert(document.querySelector("#route-summary").textContent.includes("360 retained traces"), "route trace total is missing");
    assert(buckets().length >= 12 && buckets()[4].dataset.changes === "121" && buckets()[0].classList.contains("no-traces"), "dense or empty intervals are missing");
    assert(buckets()[4].getAttribute("aria-label").includes("121 confirmed changes") && buckets()[4].getAttribute("aria-label").includes("15 not reached"), "interval counts are not accessible");
    const flameBox = document.querySelector("#flame .u-over").getBoundingClientRect(), routeBox = document.querySelector("#route-buckets").getBoundingClientRect();
    assert(Math.abs(flameBox.left-routeBox.left) <= 2 && Math.abs(flameBox.right-routeBox.right) <= 2, "route intervals are not aligned with the flame plot");
    buckets()[0].focus();
    const key = value => document.activeElement.dispatchEvent(new KeyboardEvent("keydown",{key:value,bubbles:true,cancelable:true}));
    key("ArrowRight"); assert(document.activeElement === buckets()[1], "ArrowRight did not navigate intervals");
    key("End"); assert(document.activeElement === buckets().at(-1), "End did not navigate to the last interval");
    key("Home"); assert(document.activeElement === buckets()[0] && buckets().filter(button=>button.tabIndex===0).length===1, "interval keyboard tab stop is invalid");
    buckets()[0].click();
    await until(()=>document.querySelector("#route-records").textContent.includes("No retained trace runs"), "empty interval");
    assert(!inspector().hidden && document.querySelector("#route-records").textContent.includes("No confirmed changes recorded in this interval"), "empty interval does not explain missing history");
    await inspectDense();
    document.body.dataset.smokeStage = "route comparison";
    assert(document.querySelector("#route-records").textContent.includes("Showing the latest 3 of 121 changes"), "change truncation is hidden");
    document.querySelector(".route-trace-list").open = true;
    assert(document.querySelector("#route-records").textContent.includes("Showing the latest 3 of 330 traces"), "trace truncation is hidden");
    showChange();
    const comparison = document.querySelector("#route-comparison");
    assert(comparison.textContent.includes("First observed") && comparison.textContent.includes("confirmed") && comparison.textContent.includes("exact transition time is unknown"), "first observation and confirmation semantics are missing");
    const intervalFixture = await fetch("/__smoke/last-history").then(response=>response.json());
    const eventFixture = intervalFixture.changes[0];
    assert(eventFixture.first_seen_ms !== eventFixture.confirmed_ms && comparison.textContent.includes(new Date(eventFixture.first_seen_ms).toLocaleString()) && comparison.textContent.includes(new Date(eventFixture.confirmed_ms).toLocaleString()), "first observed and confirmed timestamps were collapsed");
    const changedRows = [...comparison.querySelectorAll(".route-hop-changed")], observationRows = [...comparison.querySelectorAll(".route-hop-observation")];
    assert(changedRows.length===1 && changedRows[0].textContent.includes("192.0.2.3") && changedRows[0].textContent.includes("198.51.100.2"), "disjoint hop before/after comparison is missing");
    assert(observationRows.length===2 && observationRows.every(row=>row.textContent.includes("Observation differs")), "overlapping responders or silent hops were labeled path changes");
    assert(comparison.querySelector(".route-unchanged") && !comparison.querySelector(".route-unchanged").open, "unchanged TTL disclosure is missing");
    comparison.querySelector("[data-step='1']").focus();
    comparison.querySelector("[data-step='1']").click();
    assert(document.querySelector("#route-records [data-change='212']").getAttribute("aria-pressed")==="true", "older change navigation selected the wrong event");
    assert(document.activeElement === comparison.querySelector("[data-step='1']"), "older change navigation lost keyboard focus");
    comparison.querySelector("[data-step='-1']").focus();
    comparison.querySelector("[data-step='-1']").click();
    assert(document.activeElement === comparison.querySelector("[data-step='1']") && !document.activeElement.disabled, "newest change did not preserve focus on enabled navigation");
    comparison.querySelector("[data-evidence='203']").click();
    await until(()=>document.querySelector("#trace-detail").textContent.includes("203.0.113.9"), "raw probes");
    const raw = document.querySelector("#trace-detail");
    assert(raw.textContent.includes("2 / 2") && raw.textContent.includes("No reply observed") && raw.textContent.includes("0.00 ms") && raw.textContent.includes("11 / 0") && raw.textContent.includes("flow-backup"), "raw per-probe observations lost TTL, silence, zero RTT, ICMP, or flow detail");
    await control({trace:"failure"});
    document.body.dataset.smokeStage = "route failures";
    comparison.querySelector("[data-evidence='202']").click();
    await until(()=>document.querySelector(".route-trace-retry"), "raw trace error");
    assert(raw.textContent.includes("no longer retained"), "retained trace 404 is unexplained");
    document.querySelector(".route-trace-retry").click();
    await until(()=>raw.textContent.includes("Trace 202"), "raw trace retry");
    await control({history:"failure"});
    buckets()[8].click();
    await until(()=>document.querySelector(".route-retry"), "interval error");
    document.querySelector(".route-retry").click();
    await until(()=>document.querySelector("#route-records [data-change]"), "interval retry");
    await control({history:"failure"});
    document.querySelector("#targets button[data-id=gateway]").click();
    await until(()=>document.querySelector("#route-summary").textContent.includes("Could not load route history"), "overview error");
    assert(inspector().hidden && !document.querySelector("#trace-detail").textContent, "target switch retained old route details");
    await switchTarget("backup");
    await control({ping:"failure"});
    await switchTarget("gateway");
    await until(()=>document.querySelector("#flame-status")?.textContent.includes("Could not load latency observations for this window"), "changed target ping failure");
    assert(!document.querySelector("#flame-status").hidden && [...document.querySelectorAll("#stats strong")].every(value=>value.textContent==="—"), "failed target retained old ping statistics");
    assert(document.querySelector("#flame").dataset.maxGrains === "0" && document.querySelector("#flame").dataset.distributionMethod === "", "failed target retained old flame observations");
    await switchTarget("backup");
    await control({mode:"empty"});
    document.querySelector("#targets button[data-id=gateway]").click();
    await until(()=>routes().dataset.routeTraces === "0" && !routes().hasAttribute("aria-busy"), "empty overview");
    assert(document.querySelector("#route-hint").textContent.includes("No retained traces in this window"), "empty overview implies a stable route");
    document.querySelector("#route-inspect").click();
    assert(document.querySelector("#route-records").textContent.includes("No retained trace runs"), "empty window inspector is missing");
    await control({mode:"normal"});
    await switchTarget("backup");
    await switchTarget("gateway");
    document.body.dataset.smokeStage = "stale route requests";
    const beforeDetail = await control({history:"delay"});
    buckets()[4].click();
    await until(async()=> (await control({})).delayedHistory > beforeDetail.delayedHistory, "delayed interval request");
    await switchTarget("backup");
    await fetch("/__smoke/settled");
    assert(inspector().hidden && !document.querySelector("#route-records").textContent && !buckets().some(button=>button.classList.contains("selected")), "stale interval response replaced the current target");
    await switchTarget("gateway");
    await inspectDense(); showChange();
    const beforeRaw = await control({trace:"delay"});
    document.querySelector("[data-evidence='103']").click();
    await until(async()=> (await control({})).delayedTrace > beforeRaw.delayedTrace, "delayed raw request");
    await switchTarget("backup");
    await fetch("/__smoke/settled");
    assert(inspector().hidden && !document.querySelector("#trace-detail").textContent, "stale raw trace response replaced the current target");
    await inspectDense();
    document.body.dataset.smokeStage = "route zoom";
    const selectedBounds = await fetch("/__smoke/last-history").then(response=>response.json());
    document.querySelector("#route-zoom").click();
    await until(()=>Math.abs(Number(routes().dataset.from)-selectedBounds.from_ms)<1000 && Math.abs(Number(routes().dataset.to)-selectedBounds.to_ms)<1000 && !routes().hasAttribute("aria-busy"), "route interval zoom");
    await alignedDomains(true);
    [...document.querySelectorAll("#ranges button")].find(button => button.textContent === "24h").click();
    await routeReady();
    await alignedDomains(true);
    await control({interfaceZoom:"arm"});
    document.body.dataset.routeChecks = "true";
    const over = await until(() => document.querySelector("#flame .u-over"));
    document.body.dataset.smokeStage = "flame zoom";
    const box = over.getBoundingClientRect();
    const event = (name, x) => { const value=new MouseEvent(name, {bubbles:true, clientX:x, clientY:box.top+Math.max(10,box.height/2), buttons:name === "mouseup" ? 0 : 1}); if (name === "mousemove") Object.defineProperty(value,"movementX",{value:25}); return value; };
    over.dispatchEvent(event("mousedown", box.left+Math.max(15,box.width*.2)));
    over.dispatchEvent(event("mousemove", box.left+Math.max(40,box.width*.65)));
    document.dispatchEvent(event("mouseup", box.left+Math.max(40,box.width*.65)));
    await wait(100);
    document.querySelector("#targets button[data-id=gateway]").click();
    await wait(250);
    document.querySelector("#targets button[data-id=backup]").click();
    await wait(500);
    if (document.querySelector("#interface-title").textContent !== "eth0") throw new Error("stale interface failure replaced the current request");
    await alignedDomains(true);
    const flameRows = () => [...document.querySelectorAll("#flame .u-series")];
    if (flameRows().some(row => row.querySelector(".u-label")?.textContent === "timeout")) throw new Error("timeout series still controls the latency plot");
    for (const label of ["mean RTT", "p50", "p95", "best", "deadline miss", "no reply", "late reply", "local send error", "scheduler gap", "partial bucket"]) {
      if (!flameRows().some(row => row.querySelector(".u-label")?.textContent === label)) throw new Error("combined legend is missing " + label);
    }
    const p99 = flameRows().find(row => row.querySelector(".u-label")?.textContent === "p99");
    const max = flameRows().find(row => row.querySelector(".u-label")?.textContent === "max");
    const late = flameRows().find(row => row.querySelector(".u-label")?.textContent === "late reply");
    if (!p99?.classList.contains("hover-only") || !max?.classList.contains("hover-only")) throw new Error("p99/max are not marked hover-only");
    if (!late?.classList.contains("tooltip-only") || getComputedStyle(late.querySelector("th")).pointerEvents !== "none") throw new Error("late reply is not tooltip-only and noninteractive");
    const hover = new MouseEvent("mousemove", {bubbles:true, clientX:box.left+Math.max(40,box.width*.55), clientY:box.top+Math.max(10,box.height/2)});
    over.dispatchEvent(hover);
    await until(() => [p99,max,late].every(row => row.querySelector(".u-value")?.textContent !== "--"));
    p99.querySelector("th").click();
    document.body.dataset.smokeStage = "refresh persistence";
    await routeReady(); await inspectDense(); showChange();
    document.querySelector(".route-trace-list").open = true;
    document.querySelector(".route-unchanged").open = true;
    document.querySelector("[data-evidence='203']").click();
    await until(()=>document.querySelector("#trace-detail").textContent.includes("203.0.113.9"));
    const snapshot = document.querySelector("#route-inspector").innerHTML;
    const selectedTitle = document.querySelector("#route-detail-title").textContent;
    await wait(5500);
    assert(!inspector().hidden && document.querySelector("#route-detail-title").textContent === selectedTitle && document.querySelector("#route-inspector").innerHTML === snapshot && buckets().some(button=>button.classList.contains("selected")), "periodic refresh replaced the inspector selection or disclosures");
    const refreshedP99 = flameRows().find(row => row.querySelector(".u-label")?.textContent === "p99");
    if (!refreshedP99?.classList.contains("u-off")) throw new Error("legend visibility reset during periodic refresh");
    if (![...document.querySelectorAll("#flame .u-value")].some(value => value.textContent !== "--")) throw new Error("selected legend values reset during periodic refresh");
    document.body.dataset.legendPreserved = "true";
    document.body.dataset.smoke = "complete";
    } catch(error) { document.body.dataset.smoke = "error:" + error.message; }
  });
})();
</script>`;

const screenshotAutomation = `<script>
(() => {
  const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
  const until = async test => { for (let i=0; i<50; i++) { const value=test(); if (value) return value; await wait(100); } throw new Error("screenshot fixture timed out"); };
  addEventListener("load", async () => {
    try {
    await until(() => document.querySelector("#flame[data-flame-rendered=distribution]") && document.querySelector("#targets button[data-id=backup]"));
    document.querySelector("#targets button[data-id=backup]").click();
    await until(() => document.querySelector("#title").textContent === "Backup" && document.querySelector("#flame[data-loss-rail=full-no-rtt]") && document.querySelector("#route-history[data-route-traces='360']:not([aria-busy])"));
    if (new URL(location.href).searchParams.get("screenshot") === "inspector") {
      document.querySelectorAll("#route-buckets button")[4].click();
      await until(()=>document.querySelector("#route-records [data-change]"));
      document.querySelector("#route-records [data-change]").click();
      document.querySelector("[data-evidence='203']").click();
      await until(()=>document.querySelector("#trace-detail").textContent.includes("203.0.113.9"));
      document.querySelector("#route-history").scrollIntoView();
    }
    if (document.documentElement.scrollWidth > innerWidth) throw new Error("route inspector overflows viewport");
    await wait(200);
    document.body.dataset.screenshotReady = "true";
    } catch(error) { document.body.dataset.screenshotReady = "error:" + error.message; }
  });
})();
</script>`;

const server = createServer(async (request, response) => {
  const url = new URL(request.url, "http://localhost");
  const path = url.pathname;
  if (path === "/__smoke/control") {
    if (url.searchParams.has("mode")) { routeMode = url.searchParams.get("mode"); routeWindows.clear(); }
    if (url.searchParams.has("history")) nextHistoryAction = url.searchParams.get("history");
    if (url.searchParams.has("trace")) nextTraceAction = url.searchParams.get("trace");
    if (url.searchParams.has("ping")) nextPingAction = url.searchParams.get("ping");
    if (url.searchParams.has("interfaceZoom")) interfaceZoomFailureArmed = true;
    response.writeHead(200, {"content-type":"application/json"});
    response.end(json({delayedHistory,delayedTrace}));
    return;
  }
  if (path === "/__smoke/last-history") {
    response.writeHead(200, {"content-type":"application/json"}); response.end(json(lastHistory)); return;
  }
  if (path === "/__smoke/settled") {
    await Promise.all(pendingDelays);
    response.writeHead(200); response.end("settled"); return;
  }
  if (path === "/api/v1/status") {
    response.writeHead(200, {"content-type":"application/json"});
    response.end(json(status));
    return;
  }
  if (fixtureMode && path === "/api/v1/interfaces/eth0/series") {
    const maxPoints = Number(url.searchParams.get("max_points"));
    const logged = {path, from:Number(url.searchParams.get("from")), to:Number(url.searchParams.get("to")), maxPoints};
    if (maxPoints === 61 && !interfacePresetFailureUsed) {
      interfacePresetFailureUsed = true;
      requestLog.push(logged);
      response.writeHead(500, {"content-type":"text/plain"});
      response.end("injected interface failure");
      return;
    }
    if (maxPoints === 900 && interfaceZoomFailureArmed && !interfaceZoomFailureUsed) {
      interfaceZoomFailureUsed = true;
      requestLog.push(logged);
      await new Promise(resolve => setTimeout(resolve, 800));
      response.writeHead(500, {"content-type":"text/plain"});
      response.end("stale interface failure");
      return;
    }
  }
  if (fixtureMode) {
    const fixture = fixtureResponse(url);
    if (fixture !== undefined) {
      requestLog.push({path, from:Number(url.searchParams.get("from")), to:Number(url.searchParams.get("to")), maxPoints:Number(url.searchParams.get("max_points"))});
      const isHistory = path.endsWith("/route-history"), isTrace = /^\/api\/v1\/traces\/\d+$/.test(path), isPing = path.endsWith("/ping");
      if (isHistory) lastHistory = fixture;
      const action = isHistory ? nextHistoryAction : isTrace ? nextTraceAction : isPing ? nextPingAction : "";
      if (isHistory) nextHistoryAction = "";
      if (isTrace) nextTraceAction = "";
      if (isPing) nextPingAction = "";
      if (action === "failure") {
        response.writeHead(isTrace ? 404 : 500, {"content-type":"text/plain"}); response.end(isPing ? "injected ping failure" : "injected route failure"); return;
      }
      if (action === "delay") {
        if (isHistory) delayedHistory++; else delayedTrace++;
        const pending = new Promise(resolve=>setTimeout(resolve,250));
        pendingDelays.add(pending); await pending; pendingDelays.delete(pending);
      }
      response.writeHead(200, {"content-type":"application/json"});
      response.end(json(fixture));
      return;
    }
  } else if (path === "/api/v1/targets" || path === "/api/v1/interfaces") {
    response.writeHead(200, {"content-type":"application/json"});
    response.end(json([]));
    return;
  }
  const file = path === "/" ? "index.html" : path.slice(1);
  try {
    let body = await readFile(join(dist, file));
    if (fixtureMode && file === "index.html") body = Buffer.from(body.toString().replace("</body>", `${url.searchParams.has("screenshot")?screenshotAutomation:automation}</body>`));
    response.writeHead(200, {"content-type":extname(file)===".js"?"text/javascript":extname(file)===".css"?"text/css":"text/html"});
    response.end(body);
  } catch {
    response.writeHead(404).end();
  }
});

await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
const {port} = server.address();
const chrome = process.env.CHROME || "google-chrome";
async function render(virtualTime=3000) {
  const child = spawn(chrome, ["--headless", "--disable-gpu", "--no-sandbox", "--disable-crash-reporter", "--disable-crashpad", `--virtual-time-budget=${virtualTime}`, "--dump-dom", `http://127.0.0.1:${port}/`], {stdio:["ignore", "pipe", "pipe"]});
  let output = "", errors = "";
  child.stdout.on("data", chunk => output += chunk);
  child.stderr.on("data", chunk => errors += chunk);
  const code = await new Promise(resolve => child.on("close", resolve));
  if (code !== 0) throw new Error(`Chrome exited ${code}: ${errors}`);
  return output;
}

async function captureScreenshot(path, width, height, mode="overview") {
  // Capture the settled compositor surface via CDP. The CLI screenshot path
  // can capture a blank surface after scrollIntoView under virtual time.
  const profile = await mkdtemp(join(tmpdir(), "flameping-browser-smoke-"));
  const child = spawn(chrome, ["--headless", "--disable-gpu", "--no-sandbox", "--disable-crash-reporter", "--disable-crashpad", "--hide-scrollbars", "--remote-debugging-port=0", `--user-data-dir=${profile}`, "about:blank"], {stdio:["ignore", "ignore", "pipe"]});
  const closed = new Promise(resolve=>child.once("close",resolve));
  let socket;
  try {
    const endpoint = await new Promise((resolve,reject)=>{
      let errors = "";
      const timer = setTimeout(()=>reject(new Error("Chrome debugging endpoint timed out: "+errors)),10_000);
      child.once("error",error=>{clearTimeout(timer);reject(error)});
      child.once("exit",code=>{clearTimeout(timer);reject(new Error("Chrome screenshot exited "+code+": "+errors))});
      child.stderr.on("data",chunk=>{errors+=chunk;const match=errors.match(/DevTools listening on (ws:\/\/[^\s]+)/);if(match){clearTimeout(timer);resolve(match[1])}});
    });
    const origin = new URL(endpoint); origin.protocol = "http:";
    const pages = await fetch(origin.origin+"/json/list").then(response=>response.json());
    socket = new WebSocket(pages.find(page=>page.type==="page").webSocketDebuggerUrl);
    await new Promise((resolve,reject)=>{socket.addEventListener("open",resolve,{once:true});socket.addEventListener("error",reject,{once:true})});
    let sequence = 0;
    const pending = new Map();
    socket.addEventListener("message",event=>{
      const message = JSON.parse(event.data), request = pending.get(message.id);
      if(request){pending.delete(message.id);message.error?request.reject(new Error(message.error.message)):request.resolve(message.result)}
    });
    const command = (method,params={}) => new Promise((resolve,reject)=>{const id=++sequence;pending.set(id,{resolve,reject});socket.send(json({id,method,params}))});
    await command("Page.enable");
    await command("Emulation.setDeviceMetricsOverride",{width,height,deviceScaleFactor:1,mobile:false});
    await command("Page.navigate",{url:`http://127.0.0.1:${port}/?screenshot=${mode}`});
    const ready = await command("Runtime.evaluate",{expression:`new Promise((resolve,reject)=>{let tries=0;const poll=()=>{const state=document.body?.dataset.screenshotReady;if(state)return resolve(state);if(++tries>100)return reject(new Error("Screenshot fixture timed out"));setTimeout(poll,100)};poll()})`,awaitPromise:true,returnByValue:true});
    if (ready.exceptionDetails || ready.result.value !== "true") throw new Error("Screenshot workflow did not complete: "+json(ready));
    const capture = await command("Page.captureScreenshot",{format:"png",fromSurface:true,captureBeyondViewport:false});
    await writeFile(path,Buffer.from(capture.data,"base64"));
  } finally {
    socket?.close();
    child.kill(); await closed;
    await rm(profile,{recursive:true,force:true});
  }
}

try {
  let output = await render();
  if (!output.includes('id="health" class="pill ready"') || !output.includes("No interfaces configured")) {
    throw new Error("dashboard did not reach its ready empty state");
  }

  status = {...status, ready:false, pressure:"critical", writer_error:"filesystem reserve reached"};
  output = await render();
  if (!output.includes('id="health" class="pill"') || !output.includes(">paused</span>")) {
    throw new Error("dashboard hid its pressure-paused state");
  }

  fixtureMode = true;
  status = {...status, ready:true, pressure:"normal", writer_error:undefined};
  output = await render(25000);
  if (!output.includes('data-smoke="complete"')) throw new Error(`fixture workflow did not complete: ${output.match(/data-smoke="([^"]+)/)?.[1]??"no smoke state"}; stage=${output.match(/data-smoke-stage="([^"]+)/)?.[1]??"unknown"}; flame=${output.match(/<div id="flame"[^>]*>/)?.[0]??"missing"}; routes=${output.match(/<section id="route-history"[^>]*>/)?.[0]??"missing"}; interface=${output.match(/<div id="interface-chart"[^>]*>/)?.[0]??"missing"}`);
  if (!output.includes('data-legend-preserved="true"')) throw new Error("chart interaction state was not preserved");
  if (!output.includes('data-route-checks="true"')) throw new Error("route history workflow did not complete");
  if (!output.includes('<h3 id="interface-title">eth0</h3>')) throw new Error("interface request failure did not recover or a stale failure replaced current data");
  for (const text of [">Backup</h2>", ">eth0</h3>", "missing", "203.0.113.9", "192.0.2.3", "198.51.100.2"]) {
    if (!output.includes(text)) throw new Error(`fixture workflow did not render ${text}`);
  }

  const pings = requestLog.filter(request => request.path.endsWith("/ping"));
  for (const [duration, maxPoints] of [[3600e3,61],[6*3600e3,73],[86400e3,97],[7*86400e3,169],[30*86400e3,181]]) {
    if (!pings.some(request => Math.abs(request.to-request.from-duration) < 1000 && request.maxPoints === maxPoints)) {
      throw new Error(`missing preset density ${duration}/${maxPoints}: ${JSON.stringify(pings)}`);
    }
  }
  const day = pings.find(request => request.to-request.from > 23*3600e3);
  const zoom = day && pings.find(request => request.path === "/api/v1/targets/backup/ping" && request.to-request.from < 23*3600e3 && request.to-request.from > 0 && request.maxPoints === 900);
  if (!day) throw new Error("24h range preset did not requery the series");
  if (!zoom) throw new Error(`chart zoom did not issue a bounded requery: ${JSON.stringify(pings)}`);
  const interfaceZoom = requestLog.find(request => request.path === "/api/v1/interfaces/eth0/series" && request.maxPoints === 900 && Math.abs(request.from-zoom.from) < 1000 && Math.abs(request.to-zoom.to) < 1000);
  if (!interfaceZoom) throw new Error(`interface chart did not follow the detailed zoom: ${JSON.stringify(requestLog)}`);
  for (const path of ["/api/v1/targets/backup/ping", "/api/v1/interfaces/eth0/series", "/api/v1/traces/203", "/api/v1/traces/103"]) {
    if (!requestLog.some(request => request.path === path)) throw new Error(`workflow did not request ${path}`);
  }
  const histories = requestLog.filter(request=>request.path.endsWith("/route-history"));
  for (const ping of pings.filter(request=>request.from>0)) {
    if (!histories.some(history=>history.path.replace("/route-history","/ping")===ping.path && Math.abs(history.from-ping.from)<1000 && Math.abs(history.to-ping.to)<1000)) throw new Error(`route overview did not share ping bounds: ${JSON.stringify(ping)}`);
  }
  if (requestLog.some(request=>request.path==="/api/v1/route-changes" || /\/targets\/[^/]+\/traces$/.test(request.path))) throw new Error("route UI still requested unbounded legacy lists");
  if (process.env.FLAMEPING_SCREENSHOT) {
    await captureScreenshot(process.env.FLAMEPING_SCREENSHOT, 1440, 1100);
    console.log(`browser smoke: wrote desktop screenshot to ${process.env.FLAMEPING_SCREENSHOT}`);
  }
  if (process.env.FLAMEPING_SCREENSHOT_MOBILE) {
    await captureScreenshot(process.env.FLAMEPING_SCREENSHOT_MOBILE, 390, 844, "inspector");
    console.log(`browser smoke: wrote mobile screenshot to ${process.env.FLAMEPING_SCREENSHOT_MOBILE}`);
  }
  if (process.env.FLAMEPING_SCREENSHOT_INSPECTOR) {
    await captureScreenshot(process.env.FLAMEPING_SCREENSHOT_INSPECTOR, 1440, 1400, "inspector");
    console.log(`browser smoke: wrote inspector screenshot to ${process.env.FLAMEPING_SCREENSHOT_INSPECTOR}`);
  }
  console.log("browser smoke: flame density/loss rail, local gaps, preset/zoom bounds, legend persistence, interface failure recovery; aligned route counts, dense/empty intervals, keyboard navigation, before/after semantics, raw probes, truncation, error retries, stale-response isolation, route zoom, and inspector refresh persistence passed");
} finally {
  server.close();
}
