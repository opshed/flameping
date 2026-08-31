import {createServer} from "node:http";
import {readFile} from "node:fs/promises";
import {spawn} from "node:child_process";
import {join, extname} from "node:path";

const dist = new URL("../internal/webui/dist/", import.meta.url).pathname;
const json = value => JSON.stringify(value);
let status = {ready:true, pressure:"normal", live_bytes:0, dirty_buckets:0, available_bytes:1_000_000, trace_capabilities:{ipv4:"disabled",ipv6:"disabled"}, interface_capability:"disabled"};
let fixtureMode = false;
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
  return Array.from({length:21}, (_, i) => ({
    time_ms: Math.round(from + step * i), scheduled:60, attempted:60, sent:60,
    late:i % 7 === 0 ? 1 : 0, unanswered:i % 11 === 0 ? 1 : 0,
    send_errors:0, scheduler_missed:0, min_ms:1+i/10, p50_ms:2+i/10,
    p95_ms:4+i/10, p99_ms:6+i/10, max_ms:8+i/10, timeout_max_ms:10,
    deadline_miss_pct:1.6, no_reply_pct:0.8,
  }));
}

function fixtureResponse(url) {
  const path = url.pathname;
  if (path === "/api/v1/targets") return fixtures.targets;
  if (path === "/api/v1/interfaces") return fixtures.interfaces;
  if (/^\/api\/v1\/targets\/[^/]+\/ping$/.test(path)) {
    const from = Number(url.searchParams.get("from"));
    const to = Number(url.searchParams.get("to"));
    return {points:points(from, to)};
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
  const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
  const until = async test => { for (let i=0; i<40; i++) { const value=test(); if (value) return value; await wait(100); } throw new Error("browser smoke automation timed out"); };
  addEventListener("load", async () => {
    const range = await until(() => [...document.querySelectorAll("#ranges button")].find(button => button.textContent === "24h"));
    await until(() => document.querySelector("#targets button[data-id=gateway]") && document.querySelector("#latency .u-over"));
    range.click();
    await wait(300);
    document.querySelector("#targets button[data-id=backup]").click();
    await wait(300);
    document.querySelector("#interfaces button[data-name=eth0]").click();
    await wait(300);
    await until(() => document.querySelector("#routes button[data-id='7']"));
    document.querySelector("#routes button[data-id='7']").click();
    await wait(300);
    const over = await until(() => document.querySelector("#latency .u-over"));
    const box = over.getBoundingClientRect();
    const event = (name, x) => { const value=new MouseEvent(name, {bubbles:true, clientX:x, clientY:box.top+Math.max(10,box.height/2), buttons:name === "mouseup" ? 0 : 1}); if (name === "mousemove") Object.defineProperty(value,"movementX",{value:25}); return value; };
    over.dispatchEvent(event("mousedown", box.left+Math.max(15,box.width*.2)));
    over.dispatchEvent(event("mousemove", box.left+Math.max(40,box.width*.65)));
    document.dispatchEvent(event("mouseup", box.left+Math.max(40,box.width*.65)));
    await wait(500);
    document.body.dataset.smoke = "complete";
  }).catch(error => document.body.dataset.smoke = "error:" + error.message);
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
  if (fixtureMode) {
    const fixture = fixtureResponse(url);
    if (fixture !== undefined) {
      requestLog.push({path, from:Number(url.searchParams.get("from")), to:Number(url.searchParams.get("to"))});
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
    if (fixtureMode && file === "index.html") body = Buffer.from(body.toString().replace("</body>", `${automation}</body>`));
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
  output = await render(7000);
  if (!output.includes('data-smoke="complete"')) throw new Error("fixture workflow did not complete");
  for (const text of [">Backup</h2>", ">eth0</h3>", "missing", "203.0.113.9", "192.0.2.3", "198.51.100.2"]) {
    if (!output.includes(text)) throw new Error(`fixture workflow did not render ${text}`);
  }

  const pings = requestLog.filter(request => request.path.endsWith("/ping"));
  const day = pings.find(request => request.to-request.from > 23*3600e3);
  const zoom = day && pings.find(request => request.path === "/api/v1/targets/backup/ping" && request.to-request.from < 23*3600e3 && request.to-request.from > 0);
  if (!day) throw new Error("24h range preset did not requery the series");
  if (!zoom) throw new Error(`chart zoom did not issue a bounded requery: ${JSON.stringify(pings)}`);
  for (const path of ["/api/v1/targets/backup/ping", "/api/v1/interfaces/eth0/series", "/api/v1/traces/7"]) {
    if (!requestLog.some(request => request.path === path)) throw new Error(`workflow did not request ${path}`);
  }
  console.log("browser smoke: health states, range and zoom, target/interface switching and missing state, trace selection, and route diff rendered");
} finally {
  server.close();
}
