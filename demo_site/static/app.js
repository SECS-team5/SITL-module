const POLL_MS = 400;
const METERS_PER_DEG_LAT = 111111;

const $ = (id) => document.getElementById(id);
const dom = {
  canvas: $("canvas"),
  countInput: $("countInput"),
  scenarioSelect: $("scenarioSelect"),
  startBtn: $("startBtn"),
  stopBtn: $("stopBtn"),
  badge: $("badge"),
  droneCount: $("droneCount"),
  droneList: $("droneList"),
  flowCommands: $("flowCommands"),
  flowResponses: $("flowResponses"),
  flowRate: $("flowRate"),
  pipeline: $("pipeline"),
  manualPanel: $("manualPanel"),
  manualForm: $("manualForm"),
  manualDrone: $("manualDrone"),
  manualStatus: $("manualStatus"),
  mVx: $("mVx"),
  mVy: $("mVy"),
  mVz: $("mVz"),
  mHdg: $("mHdg"),
  mLat: $("mLat"),
  mLon: $("mLon"),
  mAlt: $("mAlt"),
  rawTopic: $("rawTopic"),
  rawPreset: $("rawPreset"),
  rawPayload: $("rawPayload"),
};

const RAW_PRESETS = [
  {
    id: "valid_cmd",
    label: "✓ Валидная команда",
    topic: "sitl.commands",
    payload: { drone_id: "drone_901", vx: 5.0, vy: 2.0, vz: 0.0, mag_heading: 68.2 },
  },
  {
    id: "cmd_out_of_range",
    label: "✗ vx вне диапазона (>50)",
    topic: "sitl.commands",
    payload: { drone_id: "drone_901", vx: 999.0, vy: 0.0, vz: 0.0, mag_heading: 0.0 },
  },
  {
    id: "cmd_no_drone_id",
    label: "✗ пропущен drone_id",
    topic: "sitl.commands",
    payload: { vx: 1.0, vy: 1.0, vz: 0.0, mag_heading: 0.0 },
  },
  {
    id: "cmd_bad_id",
    label: "✗ неверный формат drone_id",
    topic: "sitl.commands",
    payload: { drone_id: "UFO-1", vx: 1.0, vy: 0.0, vz: 0.0, mag_heading: 0.0 },
  },
  {
    id: "cmd_garbage",
    label: "✗ мусор",
    topic: "sitl.commands",
    payload: { foo: "bar", answer: 42 },
  },
  {
    id: "valid_home",
    label: "✓ Валидный HOME",
    topic: "sitl-drone-home",
    payload: { drone_id: "drone_901", home_lat: 59.9386, home_lon: 30.3141, home_alt: 120.0 },
  },
  {
    id: "home_no_alt",
    label: "✗ HOME без home_alt",
    topic: "sitl-drone-home",
    payload: { drone_id: "drone_901", home_lat: 59.9386, home_lon: 30.3141 },
  },
  {
    id: "valid_request",
    label: "✓ Запрос позиции",
    topic: "sitl.telemetry.request",
    payload: { drone_id: "drone_901" },
  },
];

let activeManualTab = "cmd";
let scenarioOptionsFilled = false;

const pipeNodes = Array.from(dom.pipeline.querySelectorAll(".pipe-node")).reduce((acc, el) => {
  acc[el.dataset.node] = el;
  return acc;
}, {});

const ctx = dom.canvas.getContext("2d");

let prev = null;
let next = null;
let lastUpdate = performance.now();

function resizeCanvas() {
  const r = dom.canvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  dom.canvas.width = Math.max(1, Math.floor(r.width * dpr));
  dom.canvas.height = Math.max(1, Math.floor(r.height * dpr));
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
}

function lerp(a, b, t) { return a + (b - a) * t; }

function projectAll(drones, w, h) {
  if (!drones.length) return null;
  const lats = [], lons = [];
  for (const d of drones) {
    lats.push(d.lat, d.home_lat);
    lons.push(d.lon, d.home_lon);
    for (const p of d.trail || []) { lats.push(p.lat); lons.push(p.lon); }
  }
  const minLat = Math.min(...lats), maxLat = Math.max(...lats);
  const minLon = Math.min(...lons), maxLon = Math.max(...lons);
  const midLat = (minLat + maxLat) / 2;
  const midLon = (minLon + maxLon) / 2;
  const mLat = METERS_PER_DEG_LAT;
  const mLon = METERS_PER_DEG_LAT * Math.cos((midLat * Math.PI) / 180);
  const spanX = Math.max((maxLon - minLon) * mLon, 20);
  const spanY = Math.max((maxLat - minLat) * mLat, 20);
  const pad = 60;
  const scale = Math.min((w - pad * 2) / spanX, (h - pad * 2) / spanY);
  const cx = w / 2, cy = h / 2;
  return (lat, lon) => ({
    x: (lon - midLon) * mLon * scale + cx,
    y: cy - (lat - midLat) * mLat * scale,
  });
}

function draw() {
  const w = dom.canvas.clientWidth;
  const h = dom.canvas.clientHeight;

  ctx.fillStyle = "#0b0f0b";
  ctx.fillRect(0, 0, w, h);

  const grad = ctx.createRadialGradient(w / 2, h / 2, 0, w / 2, h / 2, Math.max(w, h) / 1.2);
  grad.addColorStop(0, "rgba(46, 196, 182, 0.06)");
  grad.addColorStop(1, "rgba(0,0,0,0)");
  ctx.fillStyle = grad;
  ctx.fillRect(0, 0, w, h);

  const snap = next;
  if (!snap || !snap.drones.length) {
    ctx.fillStyle = "#6b7269";
    ctx.font = "500 14px Inter, system-ui";
    ctx.textAlign = "center";
    ctx.fillText(
      snap && !snap.connected
        ? "Нет подключения к broker"
        : "Нажмите «Запустить» чтобы поднять рой",
      w / 2,
      h / 2
    );
    return;
  }

  const project = projectAll(snap.drones, w, h);
  if (!project) return;

  // grid
  ctx.strokeStyle = "rgba(255,255,255,0.04)";
  ctx.lineWidth = 1;
  const step = 60;
  for (let x = step; x < w; x += step) {
    ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, h); ctx.stroke();
  }
  for (let y = step; y < h; y += step) {
    ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w, y); ctx.stroke();
  }

  const dt = performance.now() - lastUpdate;
  const t = Math.min(1, dt / POLL_MS);
  const prevMap = new Map((prev?.drones || []).map((d) => [d.drone_id, d]));

  // trails
  for (const d of snap.drones) {
    if (!d.trail || d.trail.length < 2) continue;
    ctx.strokeStyle = d.color + "55";
    ctx.lineWidth = 2;
    ctx.beginPath();
    d.trail.forEach((p, i) => {
      const pt = project(p.lat, p.lon);
      if (i === 0) ctx.moveTo(pt.x, pt.y);
      else ctx.lineTo(pt.x, pt.y);
    });
    ctx.stroke();
  }

  // homes
  for (const d of snap.drones) {
    const hp = project(d.home_lat, d.home_lon);
    ctx.strokeStyle = d.color + "55";
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    ctx.moveTo(hp.x - 5, hp.y); ctx.lineTo(hp.x + 5, hp.y);
    ctx.moveTo(hp.x, hp.y - 5); ctx.lineTo(hp.x, hp.y + 5);
    ctx.stroke();
  }

  // drones
  for (const d of snap.drones) {
    const p0 = prevMap.get(d.drone_id);
    const lat = p0 ? lerp(p0.lat, d.lat, t) : d.lat;
    const lon = p0 ? lerp(p0.lon, d.lon, t) : d.lon;
    const pt = project(lat, lon);

    ctx.fillStyle = d.color + "22";
    ctx.beginPath();
    ctx.arc(pt.x, pt.y, 18, 0, Math.PI * 2);
    ctx.fill();

    ctx.fillStyle = d.color;
    ctx.strokeStyle = "#0b0f0b";
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.arc(pt.x, pt.y, 7, 0, Math.PI * 2);
    ctx.fill();
    ctx.stroke();

    if (d.heading != null) {
      const rad = (d.heading * Math.PI) / 180;
      const ax = pt.x + Math.sin(rad) * 16;
      const ay = pt.y - Math.cos(rad) * 16;
      ctx.strokeStyle = d.color;
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.moveTo(pt.x, pt.y);
      ctx.lineTo(ax, ay);
      ctx.stroke();
    }

    ctx.fillStyle = "#e6ebe2";
    ctx.font = "600 11px Inter, system-ui";
    ctx.textAlign = "center";
    ctx.fillText(d.label, pt.x, pt.y - 14);
  }
}

function animate() {
  draw();
  requestAnimationFrame(animate);
}

function renderList(snap) {
  dom.droneList.innerHTML = "";
  dom.droneCount.textContent = String(snap.drones.length);
  for (const d of snap.drones) {
    const row = document.createElement("div");
    row.className = "drone-item";
    const speed = (d.speed ?? 0).toFixed(1);
    const alt = (d.alt ?? 0).toFixed(0);
    const heading = d.heading != null ? Math.round(d.heading) : 0;
    const manualTag = d.auto === false ? `<span class="drone-tag" title="Заведён ручной HOME">manual</span>` : "";
    row.innerHTML = `
      <span class="drone-dot" style="background:${d.color}"></span>
      <div class="drone-info">
        <span class="drone-label">${d.label}${manualTag}</span>
        <span class="drone-id">${d.drone_id}</span>
      </div>
      <span class="drone-lat">${d.lat.toFixed(5)}<br>${d.lon.toFixed(5)}</span>
      <div class="drone-metrics">
        <span><b>${speed}</b>m/s</span><span class="sep">·</span>
        <span><b>↑${alt}</b>m</span><span class="sep">·</span>
        <span><b>${heading}°</b></span>
      </div>
    `;
    dom.droneList.appendChild(row);
  }
}

function renderScenarioOptions(snap) {
  const list = snap.scenarios;
  if (!list || !list.length || scenarioOptionsFilled) {
    if (snap.scenario && dom.scenarioSelect.value !== snap.scenario) {
      dom.scenarioSelect.value = snap.scenario;
    }
    return;
  }
  dom.scenarioSelect.innerHTML = "";
  for (const s of list) {
    const opt = document.createElement("option");
    opt.value = s.id;
    opt.textContent = s.label;
    dom.scenarioSelect.appendChild(opt);
  }
  if (snap.scenario) dom.scenarioSelect.value = snap.scenario;
  scenarioOptionsFilled = true;
}

function renderManualDroneList(snap) {
  const drones = snap.drones || [];
  const current = dom.manualDrone.value;
  const ids = drones.map((d) => d.drone_id);
  const existing = Array.from(dom.manualDrone.options).map((o) => o.value);
  if (ids.length === existing.length && ids.every((v, i) => v === existing[i])) return;
  dom.manualDrone.innerHTML = "";
  for (const d of drones) {
    const opt = document.createElement("option");
    opt.value = d.drone_id;
    const tag = d.auto === false ? " · manual" : "";
    opt.textContent = `${d.label} · ${d.drone_id}${tag}`;
    dom.manualDrone.appendChild(opt);
  }
  if (ids.includes(current)) dom.manualDrone.value = current;
}

function renderMetrics(snap) {
  const m = snap.metrics;
  if (!m) return;
  dom.flowCommands.textContent = String(m.commands_sent);
  dom.flowResponses.textContent = String(m.responses_received);
  dom.flowRate.textContent = (m.rate_msg_per_sec ?? 0).toFixed(1);

  const pipe = m.pipeline || {};
  const running = snap.running;
  for (const key of Object.keys(pipeNodes)) {
    const node = pipe[key];
    const el = pipeNodes[key];
    if (!node) continue;
    el.classList.toggle("active", !!node.active);
    el.classList.toggle("stale", running && !node.active);
    const countEl = el.querySelector(".pipe-count");
    if (countEl) countEl.textContent = String(node.count ?? 0);
  }
}

function updateBadge(snap) {
  if (snap.connected) {
    dom.badge.textContent = "broker на связи";
    dom.badge.dataset.state = "ok";
  } else {
    dom.badge.textContent = "broker недоступен";
    dom.badge.dataset.state = "err";
  }
}

async function fetchState() {
  try {
    const r = await fetch("/api/state", { cache: "no-store" });
    const snap = await r.json();
    prev = next;
    next = snap;
    lastUpdate = performance.now();
    updateBadge(snap);
    renderList(snap);
    renderMetrics(snap);
    renderScenarioOptions(snap);
    renderManualDroneList(snap);
    dom.stopBtn.disabled = !snap.running;
  } catch (e) {
    dom.badge.textContent = "ошибка связи";
    dom.badge.dataset.state = "err";
  }
}

async function post(url, body) {
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  const data = await r.json();
  if (!r.ok) throw new Error(data.error || "ошибка");
  prev = next;
  next = data;
  lastUpdate = performance.now();
  updateBadge(data);
  renderList(data);
  renderMetrics(data);
  renderScenarioOptions(data);
  renderManualDroneList(data);
}

dom.startBtn.addEventListener("click", async () => {
  dom.startBtn.disabled = true;
  const old = dom.startBtn.textContent;
  dom.startBtn.textContent = "Запуск…";
  try {
    await post("/api/start", { count: Number(dom.countInput.value || 6) });
    dom.stopBtn.disabled = false;
  } catch (e) {
    dom.badge.textContent = String(e.message || e);
    dom.badge.dataset.state = "err";
  } finally {
    dom.startBtn.disabled = false;
    dom.startBtn.textContent = old;
  }
});

dom.stopBtn.addEventListener("click", async () => {
  dom.stopBtn.disabled = true;
  try { await post("/api/stop"); } catch (_) {}
});

dom.scenarioSelect.addEventListener("change", async () => {
  try {
    await post("/api/scenario", { id: dom.scenarioSelect.value });
  } catch (e) {
    dom.badge.textContent = String(e.message || e);
    dom.badge.dataset.state = "err";
  }
});

// tabs in manual panel
dom.manualForm.querySelectorAll(".m-tab").forEach((tab) => {
  tab.addEventListener("click", () => {
    activeManualTab = tab.dataset.tab;
    dom.manualForm.querySelectorAll(".m-tab").forEach((t) =>
      t.classList.toggle("active", t === tab)
    );
    dom.manualForm.querySelectorAll(".m-section").forEach((s) => {
      s.hidden = s.dataset.section !== activeManualTab;
    });
    dom.manualStatus.textContent = "";
    dom.manualStatus.dataset.state = "";
  });
});

function setManualStatus(text, state) {
  dom.manualStatus.textContent = text;
  dom.manualStatus.dataset.state = state || "";
}

function fillRawPresets() {
  dom.rawPreset.innerHTML = "";
  for (const p of RAW_PRESETS) {
    const opt = document.createElement("option");
    opt.value = p.id;
    opt.textContent = p.label;
    dom.rawPreset.appendChild(opt);
  }
  applyRawPreset(RAW_PRESETS[0].id);
}

function applyRawPreset(presetId) {
  const preset = RAW_PRESETS.find((p) => p.id === presetId);
  if (!preset) return;
  dom.rawTopic.value = preset.topic;
  dom.rawPayload.value = JSON.stringify(preset.payload, null, 2);
}

dom.rawPreset.addEventListener("change", () => applyRawPreset(dom.rawPreset.value));
fillRawPresets();

dom.manualForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  if (activeManualTab === "raw") {
    let payload;
    try { payload = JSON.parse(dom.rawPayload.value); }
    catch (err) { setManualStatus("payload — не JSON: " + err.message, "err"); return; }
    try {
      const r = await fetch("/api/raw/publish", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ topic: dom.rawTopic.value, payload }),
      });
      const data = await r.json();
      if (!r.ok) throw new Error(data.error || "ошибка");
      if (data.local_valid) {
        setManualStatus(`✓ схема ${data.schema}: OK → верификатор примет`, "ok");
      } else {
        setManualStatus(`✗ отправлено, но верификатор отклонит: ${data.reason}`, "err");
      }
    } catch (err) {
      setManualStatus(String(err.message || err), "err");
    }
    return;
  }
  const droneId = dom.manualDrone.value;
  if (!droneId) { setManualStatus("выберите дрон", "err"); return; }
  try {
    if (activeManualTab === "cmd") {
      const hdg = dom.mHdg.value.trim();
      const body = {
        drone_id: droneId,
        vx: Number(dom.mVx.value || 0),
        vy: Number(dom.mVy.value || 0),
        vz: Number(dom.mVz.value || 0),
      };
      if (hdg !== "") body.mag_heading = Number(hdg);
      const r = await fetch("/api/manual/command", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await r.json();
      if (!r.ok) throw new Error(data.error || "ошибка");
      setManualStatus(`отправлено → ${droneId}`, "ok");
    } else {
      const body = {
        drone_id: droneId,
        home_lat: Number(dom.mLat.value),
        home_lon: Number(dom.mLon.value),
        home_alt: Number(dom.mAlt.value),
      };
      const r = await fetch("/api/manual/home", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await r.json();
      if (!r.ok) throw new Error(data.error || "ошибка");
      setManualStatus(`HOME задан для ${droneId}`, "ok");
    }
  } catch (err) {
    setManualStatus(String(err.message || err), "err");
  }
});

window.addEventListener("resize", resizeCanvas);
resizeCanvas();
animate();
fetchState();
setInterval(fetchState, POLL_MS);
