import {createServer} from "node:http";
import {readFile} from "node:fs/promises";
import {spawn} from "node:child_process";
import {join, extname} from "node:path";

const dist = new URL("../internal/webui/dist/", import.meta.url).pathname;
const json = value => JSON.stringify(value);
let status = {ready:true, pressure:"normal", live_bytes:0, dirty_buckets:0, available_bytes:1_000_000, trace_capabilities:{ipv4:"disabled",ipv6:"disabled"}, interface_capability:"disabled"};
let fixtureMode = false;
let interfacePresetFailureUsed = false;
let interfaceZoomFailureUsed = false;
const requestLog = [];

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

function fixtureResponse(url) {
  const path = url.pathname;
  if (path === "/api/v1/targets") return fixtures.targets;
  if (path === "/api/v1/interfaces") return fixtures.interfaces;
  if (/^\/api\/v1\/targets\/[^/]+\/ping$/.test(path)) {
    const from = Number(url.searchParams.get("from"));
    const to = Number(url.searchParams.get("to"));
    return {distribution_method:"midpoint_quantiles", distribution_cap:17, points:points(from, to)};
  }
  if (/^\/api\/v1\/targets\/[^/]+\/traces$/.test(path)) {
    return [{id:7, started_ms:Date.now()-30_000, method:"udp", reached:true, reached_hop:3, status:"complete"}];
  }
  if (path === "/api/v1/route-changes") {
    return [{
      confirmed_ms:Date.now()-20_000, old_reached_hop:3, new_reached_hop:4,
      old_route:[{ttl:1,responders:["192.0.2.2"]},{ttl:2,responders:["192.0.2.3"]}],
      new_route:[{ttl:1,responders:["192.0.2.2"]},{ttl:2,responders:["198.51.100.2"]}],
    }];
  }
  if (path === "/api/v1/traces/7") {
    return {probes:[{ttl:1,responder:"192.0.2.2",rtt_ms:1.2},{ttl:2,responder:"203.0.113.9",rtt_ms:9.8}]};
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
  const until = async test => { for (let i=0; i<40; i++) { const value=test(); if (value) return value; await wait(100); } throw new Error("browser smoke automation timed out"); };
  addEventListener("load", async () => {
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
      [...document.querySelectorAll("#ranges button")].find(button => button.textContent === label).click();
      await wait(150);
    }
    document.querySelector("#targets button[data-id=backup]").click();
    await wait(300);
    document.querySelector("#interfaces button[data-name=eth0]").click();
    await wait(300);
    [...document.querySelectorAll("#ranges button")].find(button => button.textContent === "1h").click();
    await until(() => document.querySelector("#interface-title").textContent.includes("injected interface failure"));
    [...document.querySelectorAll("#ranges button")].find(button => button.textContent === "24h").click();
    await until(() => document.querySelector("#interface-title").textContent === "eth0");
    await until(() => document.querySelector("#routes button[data-id='7']"));
    document.querySelector("#routes button[data-id='7']").click();
    await wait(300);
    const over = await until(() => document.querySelector("#flame .u-over"));
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
    await wait(4200);
    const refreshedP99 = flameRows().find(row => row.querySelector(".u-label")?.textContent === "p99");
    if (!refreshedP99?.classList.contains("u-off")) throw new Error("legend visibility reset during periodic refresh");
    if (![...document.querySelectorAll("#flame .u-value")].some(value => value.textContent !== "--")) throw new Error("selected legend values reset during periodic refresh");
    document.body.dataset.legendPreserved = "true";
    document.body.dataset.smoke = "complete";
  }).catch(error => document.body.dataset.smoke = "error:" + error.message);
})();
</script>`;

const screenshotAutomation = `<script>
(() => {
  const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
  const until = async test => { for (let i=0; i<50; i++) { const value=test(); if (value) return value; await wait(100); } throw new Error("screenshot fixture timed out"); };
  addEventListener("load", async () => {
    await until(() => document.querySelector("#flame[data-flame-rendered=distribution]") && document.querySelector("#targets button[data-id=backup]"));
    document.querySelector("#targets button[data-id=backup]").click();
    await until(() => document.querySelector("#title").textContent === "Backup" && document.querySelector("#flame[data-loss-rail=full-no-rtt]"));
    document.body.dataset.screenshotReady = "true";
  }).catch(error => document.body.dataset.screenshotReady = "error:" + error.message);
})();
</script>`;

const server = createServer(async (request, response) => {
  const url = new URL(request.url, "http://localhost");
  const path = url.pathname;
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
    if (maxPoints === 900 && !interfaceZoomFailureUsed) {
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

async function captureScreenshot(path, width, height) {
  const child = spawn(chrome, ["--headless", "--disable-gpu", "--no-sandbox", "--disable-crash-reporter", "--disable-crashpad", "--hide-scrollbars", "--force-device-scale-factor=1", `--window-size=${width},${height}`, "--virtual-time-budget=6000", `--screenshot=${path}`, `http://127.0.0.1:${port}/?screenshot=1`], {stdio:["ignore", "ignore", "pipe"]});
  let errors = "";
  child.stderr.on("data", chunk => errors += chunk);
  const code = await new Promise(resolve => child.on("close", resolve));
  if (code !== 0) throw new Error(`Chrome screenshot exited ${code}: ${errors}`);
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
  output = await render(9000);
  if (!output.includes('data-smoke="complete"')) throw new Error(`fixture workflow did not complete: ${output.match(/data-smoke="([^"]+)/)?.[1]??"no smoke state"}; target=${output.includes("data-id=\"gateway\"")} over=${output.includes("class=\"u-over\"")} flame=${output.match(/<div id="flame"[^>]*>/)?.[0]??"missing"} subtitle=${output.match(/<p id="subtitle"[^>]*>[^<]*/)?.[0]??"missing"}`);
  if (!output.includes('data-legend-preserved="true"')) throw new Error("chart interaction state was not preserved");
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
  for (const path of ["/api/v1/targets/backup/ping", "/api/v1/interfaces/eth0/series", "/api/v1/traces/7"]) {
    if (!requestLog.some(request => request.path === path)) throw new Error(`workflow did not request ${path}`);
  }
  if (process.env.FLAMEPING_SCREENSHOT) {
    await captureScreenshot(process.env.FLAMEPING_SCREENSHOT, 1440, 1100);
    console.log(`browser smoke: wrote desktop screenshot to ${process.env.FLAMEPING_SCREENSHOT}`);
  }
  if (process.env.FLAMEPING_SCREENSHOT_MOBILE) {
    await captureScreenshot(process.env.FLAMEPING_SCREENSHOT_MOBILE, 390, 844);
    console.log(`browser smoke: wrote mobile screenshot to ${process.env.FLAMEPING_SCREENSHOT_MOBILE}`);
  }
  console.log("browser smoke: combined flame density/loss rail, full no-RTT loss and local gaps, p99/max hover-only values, preset densities and detailed zoom, persistent legend visibility/values, target/interface switching and missing state, trace selection, and route diff rendered");
} finally {
  server.close();
}
