/* ============================================================
   Distributed Task Scheduler — Dashboard
   Architecture SVG  |  Results table  |  Servers
   ============================================================ */

'use strict';

// ── State ─────────────────────────────────────────────────────────────────────
let state = {
  servers: [],
  tasks: [],
  executions: [],
  results: [],
  control: { paused: false },
  runtime: { status: 'running', active: true, restart_available: false },
  timestamp: null,
};
let expandedRows = new Set();
let previousTrafficState = null;
let trafficFrames = [];
let recentTraffic = null;
let wsConnected = false;
let lastArchMetrics = null;
let selectedArchNodeId = null;
let controlRequestInFlight = false;

const TRAFFIC_WINDOW_MS = 3000;

function normalizeState(rawState) {
  return {
    servers: Array.isArray(rawState.servers) ? rawState.servers : [],
    tasks: Array.isArray(rawState.tasks) ? rawState.tasks : [],
    executions: Array.isArray(rawState.executions) ? rawState.executions : [],
    results: Array.isArray(rawState.results) ? rawState.results : [],
    control: rawState.control || { paused: false },
    runtime: rawState.runtime || { status: 'running', active: true, restart_available: false },
    timestamp: rawState.timestamp || new Date().toISOString(),
  };
}

// ── WebSocket ─────────────────────────────────────────────────────────────────
function connectWS() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  const badge = document.getElementById('ws-badge');

  ws.onopen = () => {
    wsConnected = true;
    badge.textContent = 'Live';
    badge.className = 'ws-badge connected';
    updateSimulationControls();
  };

  ws.onmessage = (e) => {
    const nextState = normalizeState(JSON.parse(e.data));
    updateTrafficModel(nextState);
    state = nextState;
    render();
  };

  ws.onclose = () => {
    wsConnected = false;
    controlRequestInFlight = false;
    badge.textContent = 'Reconnecting…';
    badge.className = 'ws-badge disconnected';
    previousTrafficState = null;
    trafficFrames = [];
    recentTraffic = emptyTrafficSnapshot();
    hideArchTooltip();
    render();
    setTimeout(connectWS, 2000);
  };

  ws.onerror = () => ws.close();
}

// ── Tab switching ─────────────────────────────────────────────────────────────
document.querySelectorAll('.tab-btn').forEach((btn) => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab-btn').forEach((b) => b.classList.remove('active'));
    document.querySelectorAll('.tab-pane').forEach((p) => p.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById(`pane-${btn.dataset.tab}`).classList.add('active');
    render();
  });
});

document.getElementById('pause-toggle').addEventListener('click', toggleSimulationPause);
document.getElementById('restart-toggle').addEventListener('click', restartSimulation);
window.addEventListener('resize', () => {
  if (selectedArchNodeId) {
    syncArchTooltip();
  }
});

// ── Main render dispatcher ────────────────────────────────────────────────────
function render() {
  updateSimulationControls();
  const activeTab = document.querySelector('.tab-btn.active')?.dataset.tab || 'arch';
  if (activeTab === 'arch') renderArchTab();
  if (activeTab === 'results') renderResultsTab();
  if (activeTab === 'servers') renderServersTab();
  if (activeTab === 'arch') {
    syncArchTooltip();
  } else {
    hideArchTooltip();
  }
}

function updateSimulationControls() {
  const btn = document.getElementById('pause-toggle');
  const restartBtn = document.getElementById('restart-toggle');
  const badge = document.getElementById('sim-badge');
  if (!btn || !restartBtn || !badge) return;

  const paused = !!(state.control && state.control.paused);
  const runtime = state.runtime || { status: 'running', active: true, restart_available: false };
  const pauseDisabled = !wsConnected || controlRequestInFlight || !runtime.active;
  const restartDisabled = !wsConnected || controlRequestInFlight || !runtime.restart_available;

  btn.disabled = pauseDisabled;
  restartBtn.disabled = restartDisabled;
  btn.textContent = paused ? 'Resume Simulation' : 'Pause Simulation';
  btn.classList.toggle('paused', paused);
  restartBtn.textContent = controlRequestInFlight ? 'Restarting…' : 'Restart Simulation';

  badge.textContent = simulationStatusLabel(runtime.status, paused);
  badge.className = `sim-badge ${simulationBadgeClass(runtime.status, paused)}`;
}

async function toggleSimulationPause() {
  if (!wsConnected || controlRequestInFlight || !(state.runtime && state.runtime.active)) return;
  const paused = !!(state.control && state.control.paused);
  controlRequestInFlight = true;
  updateSimulationControls();
  try {
    const response = await fetch('/api/control', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ paused: !paused }),
    });
    if (!response.ok) {
      throw new Error(`control request failed: ${response.status}`);
    }
    state.control = await response.json();
    render();
  } catch (error) {
    console.error('pause toggle failed', error);
  } finally {
    controlRequestInFlight = false;
    updateSimulationControls();
  }
}

async function restartSimulation() {
  if (!wsConnected || controlRequestInFlight || !(state.runtime && state.runtime.restart_available)) return;
  controlRequestInFlight = true;
  updateSimulationControls();
  try {
    const response = await fetch('/api/control/restart', {
      method: 'POST',
    });
    if (!response.ok) {
      throw new Error(`restart request failed: ${response.status}`);
    }
    state.control = await response.json();
    resetClientSimulationView();
    render();
  } catch (error) {
    console.error('restart failed', error);
  } finally {
    controlRequestInFlight = false;
    updateSimulationControls();
  }
}

function resetClientSimulationView() {
  state = {
    servers: [],
    tasks: [],
    executions: [],
    results: [],
    control: { paused: false },
    runtime: { status: 'running', active: true, restart_available: false },
    timestamp: null,
  };
  previousTrafficState = null;
  trafficFrames = [];
  recentTraffic = emptyTrafficSnapshot();
  lastArchMetrics = null;
  selectedArchNodeId = null;
  expandedRows = new Set();
  hideArchTooltip(true);
}

function simulationStatusLabel(status, paused) {
  switch (status) {
    case 'finished':
      return 'Finished';
    case 'draining':
      return 'Draining';
    case 'paused-draining':
      return 'Paused / Draining';
    case 'paused':
      return 'Paused';
    default:
      return paused ? 'Paused' : 'Running';
  }
}

function simulationBadgeClass(status, paused) {
  switch (status) {
    case 'finished':
      return 'finished';
    case 'draining':
      return 'draining';
    case 'paused-draining':
    case 'paused':
      return 'paused';
    default:
      return paused ? 'paused' : 'running';
  }
}

// ══════════════════════════════════════════════════════════════════════════════
// TAB 1 — ARCHITECTURE
// ══════════════════════════════════════════════════════════════════════════════
const ARCH_WIDTH = 1650;
const ARCH_HEIGHT = 690;

const ARCH_COLUMNS = [
  { id: 'source', label: 'Source', x: 44, width: 170 },
  { id: 'state', label: 'State', x: 248, width: 180 },
  { id: 'control', label: 'Control', x: 468, width: 200 },
  { id: 'messaging', label: 'Messaging', x: 704, width: 388 },
  { id: 'execution', label: 'Execution', x: 1120, width: 252 },
  { id: 'output', label: 'Output', x: 1400, width: 220 },
];

const ARCH_NODES = [
  { id: 'gen', label: 'Task Generator', sub: 'seed tasks + label tier', x: 58, y: 251, w: 176, h: 90, color: '#60a5fa' },
  { id: 'redis', label: 'Redis', sub: 'task + state store', x: 248, y: 457, w: 204, h: 110, color: '#f97316' },
  { id: 'sched', label: 'Scheduler', sub: 'pick concrete server', x: 472, y: 251, w: 220, h: 90, color: '#f59e0b' },
  { id: 'taskex', label: 'task_exchange', sub: 'direct exchange', x: 726, y: 253, w: 170, h: 86, color: '#3b82f6', titleSize: 11 },
  { id: 'edgeq', label: 'edge_queue', sub: 'queue', x: 920, y: 170, w: 150, h: 72, color: '#3b82f6', titleSize: 11 },
  { id: 'fogq', label: 'fog_queue', sub: 'queue', x: 920, y: 260, w: 150, h: 72, color: '#3b82f6', titleSize: 11 },
  { id: 'cloudq', label: 'cloud_queue', sub: 'queue', x: 920, y: 350, w: 150, h: 72, color: '#3b82f6', titleSize: 11 },
  { id: 'edgew', label: 'Edge Workers', sub: 'tier consumers', x: 1118, y: 170, w: 222, h: 72, color: '#3b82f6' },
  { id: 'fogw', label: 'Fog Workers', sub: 'tier consumers', x: 1118, y: 260, w: 222, h: 72, color: '#3b82f6' },
  { id: 'cloudw', label: 'Cloud Workers', sub: 'tier consumers', x: 1118, y: 350, w: 222, h: 72, color: '#3b82f6' },
  { id: 'resex', label: 'result_exchange', sub: 'fanout exchange', x: 1402, y: 254, w: 194, h: 84, color: '#3b82f6', titleSize: 11 },
  { id: 'resultq', label: 'result_queue', sub: 'queue', x: 1402, y: 360, w: 194, h: 68, color: '#3b82f6', titleSize: 11 },
  { id: 'collector', label: 'Collector', sub: 'final result sink', x: 1418, y: 468, w: 162, h: 88, color: '#10b981' },
];

const ARCH_LINKS = [
  { s: 'gen', t: 'sched', metricKey: 'gen-sched' },
  { s: 'gen', t: 'redis', metricKey: 'gen-redis', fromSide: 'bottom', toSide: 'left', via: [{ x: 146, y: 512 }] },
  { s: 'redis', t: 'sched', metricKey: 'redis-sched', fromSide: 'top', toSide: 'bottom', via: [{ x: 350, y: 376 }, { x: 582, y: 376 }] },
  { s: 'sched', t: 'taskex', metricKey: 'sched-taskex' },
  { s: 'taskex', t: 'edgeq', metricKey: 'taskex-edgeq', via: [{ x: 910, y: 296 }, { x: 910, y: 206 }] },
  { s: 'taskex', t: 'fogq', metricKey: 'taskex-fogq' },
  { s: 'taskex', t: 'cloudq', metricKey: 'taskex-cloudq', via: [{ x: 910, y: 296 }, { x: 910, y: 386 }] },
  { s: 'edgeq', t: 'edgew', metricKey: 'edgeq-edgew' },
  { s: 'fogq', t: 'fogw', metricKey: 'fogq-fogw' },
  { s: 'cloudq', t: 'cloudw', metricKey: 'cloudq-cloudw' },
  { s: 'edgew', t: 'resex', metricKey: 'edgew-resex', via: [{ x: 1372, y: 206 }, { x: 1372, y: 296 }] },
  { s: 'fogw', t: 'resex', metricKey: 'fogw-resex' },
  { s: 'cloudw', t: 'resex', metricKey: 'cloudw-resex', via: [{ x: 1372, y: 386 }, { x: 1372, y: 296 }] },
  { s: 'resex', t: 'resultq', metricKey: 'resex-resultq', fromSide: 'bottom', toSide: 'top' },
  { s: 'resultq', t: 'collector', metricKey: 'resultq-collector', fromSide: 'bottom', toSide: 'top' },
  { s: 'collector', t: 'redis', metricKey: 'collector-redis', fromSide: 'left', toSide: 'right' },
];

let archBuilt = false;

function renderArchTab() {
  if (!archBuilt) {
    buildArchSVG();
    archBuilt = true;
  }
  const metrics = topologyMetrics();
  lastArchMetrics = metrics;
  paintArchTraffic(metrics);
}

function buildArchSVG() {
  const svg = d3.select('#arch-svg');
  svg.selectAll('*').remove();
  svg.attr('width', ARCH_WIDTH).attr('height', ARCH_HEIGHT).attr('viewBox', `0 0 ${ARCH_WIDTH} ${ARCH_HEIGHT}`);
  svg.on('click', () => hideArchTooltip(true));
  const nodeMap = Object.fromEntries(ARCH_NODES.map((n) => [n.id, n]));

  svg.append('defs').append('marker')
    .attr('id', 'arch-arr').attr('viewBox', '0 -5 14 10')
    .attr('refX', 13).attr('refY', 0)
    .attr('markerWidth', 14).attr('markerHeight', 10)
    .attr('markerUnits', 'userSpaceOnUse')
    .attr('orient', 'auto')
    .append('path').attr('d', 'M0,-5L14,0L0,5').attr('fill', 'context-stroke');

  const columns = svg.append('g').attr('class', 'arch-columns');
  ARCH_COLUMNS.forEach((column, index) => {
    const g = columns.append('g').attr('class', 'arch-column');
    g.append('rect')
      .attr('x', column.x)
      .attr('y', 38)
      .attr('width', column.width)
      .attr('height', 30)
      .attr('rx', 999);
    g.append('text')
      .attr('x', column.x + column.width / 2)
      .attr('y', 57)
      .attr('text-anchor', 'middle')
      .text(column.label);

    if (index < ARCH_COLUMNS.length - 1) {
      const dividerX = column.x + column.width + 28;
      columns.append('line')
        .attr('class', 'arch-divider')
        .attr('x1', dividerX)
        .attr('y1', 90)
        .attr('x2', dividerX)
        .attr('y2', 632);
    }
  });

  ARCH_LINKS.forEach((link) => {
    const source = nodeMap[link.s];
    const target = nodeMap[link.t];
    const start = archAnchor(source, link.fromSide || 'right');
    const end = archAnchor(target, link.toSide || 'left');

    svg.append('path')
      .attr('class', 'arch-link')
      .attr('id', archLinkId(link))
      .attr('stroke', '#334155')
      .attr('stroke-opacity', 0.18)
      .attr('stroke-width', 2)
      .attr('marker-end', 'url(#arch-arr)')
      .attr('d', archLinkPath(start.x, start.y, end.x, end.y, link.via || []));
  });

  ARCH_NODES.forEach((node) => {
    const g = svg.append('g').attr('class', 'arch-box').attr('id', `arch-${node.id}`);
    g.on('click', (event) => toggleArchTooltip(event, node));
    g.append('rect')
      .attr('class', 'arch-box-backdrop')
      .attr('x', node.x)
      .attr('y', node.y)
      .attr('width', node.w)
      .attr('height', node.h)
      .attr('rx', 16);
    g.append('rect')
      .attr('class', 'arch-box-accent')
      .attr('x', node.x)
      .attr('y', node.y)
      .attr('width', node.w)
      .attr('height', 5)
      .attr('rx', 16);
    g.append('rect')
      .attr('class', 'arch-box-frame')
      .attr('x', node.x).attr('y', node.y).attr('width', node.w).attr('height', node.h)
      .attr('rx', 16)
      .attr('fill', '#1a1d27').attr('stroke', node.color).attr('stroke-width', 1.5);
    g.append('text').attr('class', 'title')
      .attr('x', node.x + node.w / 2).attr('y', node.y + node.h / 2 + 4)
      .attr('text-anchor', 'middle').attr('font-size', node.titleSize || 12).text(node.label);
  });
}

function topologyMetrics() {
  const taskByID = Object.fromEntries(state.tasks.map((task) => [task.id, task]));
  const serverById = Object.fromEntries(state.servers.map((server) => [server.id, server]));
  const serversByTier = emptyTierCounts();
  state.servers.forEach((server) => {
    if (server.type in serversByTier) {
      serversByTier[server.type] += 1;
    }
  });

  const metrics = {
    totalServers: state.servers.length,
    totalTasks: state.tasks.length,
    totalExecutions: state.executions.length,
    completedExecutions: 0,
    totalStoredResults: state.results.length,
    serversByTier,
    tasksByTier: emptyTierCounts(),
    pending: 0,
    assigned: 0,
    done: 0,
    totalInflight: 0,
    totalResults: state.results.length,
    queued: emptyTierCounts(),
    running: emptyTierCounts(),
    doneByTier: emptyTierCounts(),
    recentExecutions: 0,
    recentRouted: 0,
    recentResults: 0,
    recentQueuedByTier: emptyTierCounts(),
    recentStartedByTier: emptyTierCounts(),
    recentDoneByTier: emptyTierCounts(),
  };

  state.tasks.forEach((task) => {
    const tier = task.target_tier || '';
    if (tier) {
      metrics.tasksByTier[tier] += 1;
    }
  });

  state.executions.forEach((execution) => {
    const task = taskByID[execution.task_id];
    const tier = resolveExecutionTier(execution, task, serverById);

    switch (execution.status) {
      case 'PENDING':
        metrics.pending += 1;
        break;
      case 'QUEUED':
        metrics.assigned += 1;
        if (tier) metrics.queued[tier] += 1;
        break;
      case 'RUNNING':
        metrics.assigned += 1;
        if (tier) metrics.running[tier] += 1;
        break;
      case 'DONE':
        metrics.assigned += 1;
        metrics.done += 1;
        metrics.completedExecutions += 1;
        if (tier) metrics.doneByTier[tier] += 1;
        break;
      default:
        break;
    }
  });

  metrics.totalInflight = sum(Object.values(metrics.queued)) + sum(Object.values(metrics.running));

  const liveTraffic = recentTraffic || emptyTrafficSnapshot();
  metrics.recentExecutions = liveTraffic.executions;
  metrics.recentQueuedByTier = cloneTierCounts(liveTraffic.queuedByTier);
  metrics.recentStartedByTier = cloneTierCounts(liveTraffic.startedByTier);
  metrics.recentDoneByTier = cloneTierCounts(liveTraffic.doneByTier);
  metrics.recentRouted = sum(Object.values(metrics.recentQueuedByTier));
  metrics.recentResults = sum(Object.values(metrics.recentDoneByTier));

  metrics.linkValues = {
    'gen-sched': liveTraffic.links['gen-sched'],
    'gen-redis': liveTraffic.links['gen-redis'],
    'redis-sched': liveTraffic.links['redis-sched'],
    'sched-taskex': liveTraffic.links['sched-taskex'],
    'taskex-edgeq': liveTraffic.links['taskex-edgeq'],
    'taskex-fogq': liveTraffic.links['taskex-fogq'],
    'taskex-cloudq': liveTraffic.links['taskex-cloudq'],
    'edgeq-edgew': liveTraffic.links['edgeq-edgew'],
    'fogq-fogw': liveTraffic.links['fogq-fogw'],
    'cloudq-cloudw': liveTraffic.links['cloudq-cloudw'],
    'edgew-resex': liveTraffic.links['edgew-resex'],
    'fogw-resex': liveTraffic.links['fogw-resex'],
    'cloudw-resex': liveTraffic.links['cloudw-resex'],
    'resex-resultq': liveTraffic.links['resex-resultq'],
    'resultq-collector': liveTraffic.links['resultq-collector'],
    'collector-redis': liveTraffic.links['collector-redis'],
  };
  metrics.nodeValues = {
    gen: metrics.linkValues['gen-redis'] + metrics.linkValues['gen-sched'],
    redis: metrics.linkValues['gen-redis'] + metrics.linkValues['redis-sched'] + metrics.linkValues['collector-redis'],
    sched: metrics.linkValues['gen-sched'] + metrics.linkValues['redis-sched'] + metrics.linkValues['sched-taskex'],
    taskex: metrics.linkValues['sched-taskex'] + metrics.linkValues['taskex-edgeq'] + metrics.linkValues['taskex-fogq'] + metrics.linkValues['taskex-cloudq'],
    resex: metrics.totalResults,
    edgeq: metrics.queued.EDGE + metrics.linkValues['taskex-edgeq'] + metrics.linkValues['edgeq-edgew'],
    fogq: metrics.queued.FOG + metrics.linkValues['taskex-fogq'] + metrics.linkValues['fogq-fogw'],
    cloudq: metrics.queued.CLOUD + metrics.linkValues['taskex-cloudq'] + metrics.linkValues['cloudq-cloudw'],
    resultq: metrics.linkValues['resex-resultq'] + metrics.linkValues['resultq-collector'],
    edgew: metrics.running.EDGE + metrics.linkValues['edgeq-edgew'] + metrics.linkValues['edgew-resex'],
    fogw: metrics.running.FOG + metrics.linkValues['fogq-fogw'] + metrics.linkValues['fogw-resex'],
    cloudw: metrics.running.CLOUD + metrics.linkValues['cloudq-cloudw'] + metrics.linkValues['cloudw-resex'],
    collector: metrics.linkValues['resultq-collector'] + metrics.linkValues['collector-redis'],
  };
  metrics.nodeValues.resex = metrics.linkValues['edgew-resex'] + metrics.linkValues['fogw-resex'] + metrics.linkValues['cloudw-resex'] + metrics.linkValues['resex-resultq'];
  return metrics;
}

function paintArchTraffic(metrics) {
  const linkMax = Math.max(...Object.values(metrics.linkValues), 1);
  const nodeMax = Math.max(...Object.values(metrics.nodeValues), 1);

  ARCH_LINKS.forEach((link) => {
    const value = metrics.linkValues[link.metricKey] || 0;
    const intensity = trafficIntensity(value, linkMax);
    const path = document.getElementById(archLinkId(link));
    if (path) {
      const stroke = trafficColor(value, linkMax);
      const glow = d3.color(stroke);
      if (glow) glow.opacity = 0.28;
      path.setAttribute('stroke', stroke);
      path.setAttribute('stroke-opacity', value > 0 ? (0.24 + intensity * 0.62).toFixed(2) : '0.28');
      path.setAttribute('stroke-width', value > 0 ? (1.5 + intensity * 2.8).toFixed(2) : '1.2');
      path.setAttribute('stroke-dasharray', 'none');
      path.style.filter = value > 0 && glow
        ? `drop-shadow(0 0 ${4 + intensity * 7}px ${glow.formatRgb()})`
        : 'none';
    }
  });

  ARCH_NODES.forEach((node) => {
    const value = metrics.nodeValues[node.id] || 0;
    const intensity = trafficIntensity(value, nodeMax);
    const frame = document.querySelector(`#arch-${node.id} .arch-box-frame`);
    const backdrop = document.querySelector(`#arch-${node.id} .arch-box-backdrop`);
    const accent = document.querySelector(`#arch-${node.id} .arch-box-accent`);
    const title = document.querySelector(`#arch-${node.id} .title`);
    if (!frame || !backdrop || !accent) return;

    const heat = trafficColor(value, nodeMax);
    const glow = d3.color(heat);
    if (glow) glow.opacity = 0.18 + intensity * 0.12;
    const activeBorder = value > 0 ? heat : '#293548';
    const fill = value > 0
      ? d3.interpolateRgb('#101922', heat)(0.14 + intensity * 0.2)
      : '#0f1724';

    backdrop.setAttribute('fill', value > 0 ? d3.interpolateRgb('#0b1220', heat)(0.08) : 'rgba(15,23,36,0.72)');
    frame.setAttribute('fill', fill);
    frame.setAttribute('stroke', activeBorder);
    frame.setAttribute('stroke-width', value > 0 ? (1.3 + intensity * 1.5).toFixed(2) : '1.1');
    frame.style.filter = value > 0 && glow
      ? `drop-shadow(0 0 ${4 + intensity * 7}px ${glow.formatRgb()})`
      : 'none';
    accent.setAttribute('fill', value > 0 ? heat : '#243244');
    frame.setAttribute('opacity', value > 0 ? '1' : '0.58');
    backdrop.setAttribute('opacity', value > 0 ? '1' : '0.42');

    if (title) title.setAttribute('fill', value > 0 ? '#f8fafc' : '#cbd5e1');
  });
}

function archLinkPath(sx, sy, tx, ty, via) {
  const points = [{ x: sx, y: sy }, ...via, { x: tx, y: ty }];
  return points.map((point, index) => `${index === 0 ? 'M' : 'L'}${point.x},${point.y}`).join(' ');
}

function archAnchor(node, side) {
  switch (side) {
    case 'left':
      return { x: node.x, y: node.y + node.h / 2 };
    case 'top-left':
      return { x: node.x + node.w * 0.26, y: node.y };
    case 'top-right':
      return { x: node.x + node.w * 0.74, y: node.y };
    case 'top':
      return { x: node.x + node.w / 2, y: node.y };
    case 'bottom-left':
      return { x: node.x + node.w * 0.26, y: node.y + node.h };
    case 'bottom-right':
      return { x: node.x + node.w * 0.74, y: node.y + node.h };
    case 'bottom':
      return { x: node.x + node.w / 2, y: node.y + node.h };
    default:
      return { x: node.x + node.w, y: node.y + node.h / 2 };
  }
}

function archLinkId(link) {
  return `arch-link-${link.metricKey}`;
}

function trafficIntensity(value, max) {
  if (!max || value <= 0) return 0;
  return Math.sqrt(value / max);
}

function trafficColor(value, max) {
  if (value <= 0 || !max) return '#334155';
  const scale = d3.scaleLinear()
    .domain([0, max * 0.3, max * 0.7, max])
    .range(['#334155', '#22c55e', '#f59e0b', '#ef4444'])
    .clamp(true);
  return scale(value);
}

function sum(values) {
  return values.reduce((acc, value) => acc + value, 0);
}

function toggleArchTooltip(event, node) {
  event.stopPropagation();
  if (selectedArchNodeId === node.id) {
    hideArchTooltip(true);
    return;
  }
  selectedArchNodeId = node.id;
  syncArchTooltip();
}

function syncArchTooltip() {
  updateArchSelection();
  if (!selectedArchNodeId) {
    hideArchTooltip();
    return;
  }

  const node = ARCH_NODES.find((entry) => entry.id === selectedArchNodeId);
  const tooltip = document.getElementById('arch-tooltip');
  if (!tooltip || !node) return;

  const metrics = lastArchMetrics || topologyMetrics();
  const lines = archTooltipLines(node.id, metrics);
  tooltip.innerHTML = `
    <div class="arch-tooltip-title">${node.label}</div>
    ${lines.map((line) => `<div class="arch-tooltip-line">${line}</div>`).join('')}
  `;
  tooltip.hidden = false;
  positionArchTooltip(node.id);
}

function positionArchTooltip(nodeID) {
  const tooltip = document.getElementById('arch-tooltip');
  const node = document.getElementById(`arch-${nodeID}`);
  if (!tooltip || tooltip.hidden || !node) return;

  tooltip.style.visibility = 'hidden';
  const offset = 18;
  const rect = node.getBoundingClientRect();
  const maxLeft = window.innerWidth - tooltip.offsetWidth - 12;
  const maxTop = window.innerHeight - tooltip.offsetHeight - 12;
  let left = rect.right + offset;
  if (left > maxLeft) {
    left = rect.left - tooltip.offsetWidth - offset;
  }
  let top = rect.top + 8;
  if (top > maxTop) {
    top = maxTop;
  }
  tooltip.style.left = `${Math.max(12, left)}px`;
  tooltip.style.top = `${Math.max(12, top)}px`;
  tooltip.style.visibility = 'visible';
}

function hideArchTooltip(clearSelection = false) {
  const tooltip = document.getElementById('arch-tooltip');
  if (clearSelection) {
    selectedArchNodeId = null;
  }
  if (tooltip) {
    tooltip.hidden = true;
    tooltip.style.visibility = '';
  }
  updateArchSelection();
}

function updateArchSelection() {
  ARCH_NODES.forEach((node) => {
    const box = document.getElementById(`arch-${node.id}`);
    if (box) {
      box.classList.toggle('selected', node.id === selectedArchNodeId);
    }
  });
}

function archTooltipLines(nodeID, metrics) {
  const recentWindow = (TRAFFIC_WINDOW_MS / 1000).toFixed(0);
  const paused = !!(state.control && state.control.paused);
  const phase = paused ? 'Paused from dashboard' : 'Running';

  switch (nodeID) {
    case 'gen':
      return [
        `<strong>Status:</strong> ${phase}`,
        `<strong>Seeded task templates:</strong> ${metrics.totalTasks}`,
        `<strong>Execution requests in last ${recentWindow}s:</strong> ${metrics.recentExecutions}`,
        `<strong>Tier labels:</strong> E ${metrics.tasksByTier.EDGE} · F ${metrics.tasksByTier.FOG} · C ${metrics.tasksByTier.CLOUD}`,
      ];
    case 'redis':
      return [
        `<strong>Status:</strong> ${phase}`,
        `<strong>Stored task templates:</strong> ${metrics.totalTasks}`,
        `<strong>Stored executions:</strong> ${metrics.totalExecutions}`,
        `<strong>Pending executions:</strong> ${metrics.pending}`,
        `<strong>Inflight executions:</strong> ${metrics.totalInflight}`,
        `<strong>Stored task results:</strong> ${metrics.totalStoredResults}`,
      ];
    case 'sched':
      return [
        `<strong>Status:</strong> ${phase}`,
        `<strong>Pending executions:</strong> ${metrics.pending}`,
        `<strong>Inflight executions:</strong> ${metrics.totalInflight}`,
        `<strong>Completed executions:</strong> ${metrics.completedExecutions}`,
        `<strong>Dispatched in last ${recentWindow}s:</strong> ${metrics.recentRouted}`,
      ];
    case 'taskex':
      return [
        `<strong>Recent routed tasks:</strong> ${metrics.recentRouted}`,
        `<strong>Edge branch:</strong> ${metrics.recentQueuedByTier.EDGE}`,
        `<strong>Fog branch:</strong> ${metrics.recentQueuedByTier.FOG}`,
        `<strong>Cloud branch:</strong> ${metrics.recentQueuedByTier.CLOUD}`,
      ];
    case 'edgeq':
      return tierQueueTooltip('EDGE', metrics, recentWindow);
    case 'fogq':
      return tierQueueTooltip('FOG', metrics, recentWindow);
    case 'cloudq':
      return tierQueueTooltip('CLOUD', metrics, recentWindow);
    case 'edgew':
      return tierWorkerTooltip('EDGE', metrics, recentWindow);
    case 'fogw':
      return tierWorkerTooltip('FOG', metrics, recentWindow);
    case 'cloudw':
      return tierWorkerTooltip('CLOUD', metrics, recentWindow);
    case 'resex':
      return [
        `<strong>Recent result publishes:</strong> ${metrics.recentResults}`,
        `<strong>From edge workers:</strong> ${metrics.recentDoneByTier.EDGE}`,
        `<strong>From fog workers:</strong> ${metrics.recentDoneByTier.FOG}`,
        `<strong>From cloud workers:</strong> ${metrics.recentDoneByTier.CLOUD}`,
      ];
    case 'resultq':
      return [
        `<strong>Recent result messages:</strong> ${metrics.recentResults}`,
        `<strong>Delivered in last ${recentWindow}s:</strong> ${metrics.linkValues['resultq-collector'] || 0}`,
        `<strong>Total stored results:</strong> ${metrics.totalStoredResults}`,
      ];
    case 'collector':
      return [
        `<strong>Status:</strong> ${paused ? 'Paused from dashboard' : 'Persisting results'}`,
        `<strong>Recent results stored:</strong> ${metrics.recentResults}`,
        `<strong>Total stored results:</strong> ${metrics.totalStoredResults}`,
        `<strong>Completed executions:</strong> ${metrics.completedExecutions}`,
      ];
    default:
      return [`<strong>Status:</strong> ${phase}`];
  }
}

function tierQueueTooltip(tier, metrics, recentWindow) {
  return [
    `<strong>Queued now:</strong> ${metrics.queued[tier]}`,
    `<strong>Received in last ${recentWindow}s:</strong> ${metrics.recentQueuedByTier[tier]}`,
    `<strong>Workers started in last ${recentWindow}s:</strong> ${metrics.recentStartedByTier[tier]}`,
    `<strong>Workers currently running:</strong> ${metrics.running[tier]}`,
  ];
}

function tierWorkerTooltip(tier, metrics, recentWindow) {
  return [
    `<strong>Running now:</strong> ${metrics.running[tier]}`,
    `<strong>Started in last ${recentWindow}s:</strong> ${metrics.recentStartedByTier[tier]}`,
    `<strong>Completed in last ${recentWindow}s:</strong> ${metrics.recentDoneByTier[tier]}`,
    `<strong>Total completed this run:</strong> ${metrics.doneByTier[tier]}`,
  ];
}

function updateTrafficWindow(nextState) {
  const nextTimestamp = parseStateTimestamp(nextState);
  if (previousTrafficState) {
    trafficFrames.push({
      ts: nextTimestamp,
      traffic: buildTrafficFrame(previousTrafficState, nextState),
    });
  }
  previousTrafficState = nextState;
  const cutoff = nextTimestamp - TRAFFIC_WINDOW_MS;
  trafficFrames = trafficFrames.filter((frame) => frame.ts >= cutoff);
  recentTraffic = aggregateTrafficFrames(trafficFrames);
}

function updateTrafficModel(nextState) {
  const wasPaused = !!(state.control && state.control.paused);
  const paused = !!(nextState.control && nextState.control.paused);

  if (paused) {
    previousTrafficState = nextState;
    if (!recentTraffic) {
      recentTraffic = emptyTrafficSnapshot();
    }
    return;
  }

  if (wasPaused) {
    previousTrafficState = nextState;
    trafficFrames = [];
    recentTraffic = emptyTrafficSnapshot();
    return;
  }

  updateTrafficWindow(nextState);
}

function buildTrafficFrame(previousState, nextState) {
  const traffic = emptyTrafficSnapshot();
  const taskByID = Object.fromEntries((nextState.tasks || []).map((task) => [task.id, task]));
  const previousExecutions = Object.fromEntries((previousState.executions || []).map((execution) => [execution.id, execution]));
  const serverById = Object.fromEntries((nextState.servers || []).map((server) => [server.id, server]));

  (nextState.executions || []).forEach((execution) => {
    const previousExecution = previousExecutions[execution.id];
    if (!previousExecution) {
      traffic.executions += 1;
      traffic.links['gen-redis'] += 1;
      traffic.links['gen-sched'] += 1;
    }

    const currentStatus = execution.status || 'PENDING';
    const previousStatus = previousExecution?.status || '';
    if (currentStatus === previousStatus) {
      return;
    }

    const task = taskByID[execution.task_id];
    const tier = resolveExecutionTier(execution, task, serverById)
      || resolveExecutionTier(previousExecution, task, serverById);
    const scheduledNow = isScheduledTransition(previousStatus, currentStatus);
    const startedNow = isWorkerStartTransition(previousStatus, currentStatus);
    const completedNow = currentStatus === 'DONE' && previousStatus !== 'DONE';

    if (scheduledNow) {
      traffic.links['redis-sched'] += 1;
      traffic.links['sched-taskex'] += 1;
      if (tier) {
        traffic.queuedByTier[tier] += 1;
        traffic.links[`taskex-${tier.toLowerCase()}q`] += 1;
      }
    }

    if (startedNow && tier) {
      traffic.startedByTier[tier] += 1;
      traffic.links[`${tier.toLowerCase()}q-${tier.toLowerCase()}w`] += 1;
    }

    if (completedNow && tier) {
      traffic.doneByTier[tier] += 1;
      traffic.links[`${tier.toLowerCase()}w-resex`] += 1;
      traffic.links['resex-resultq'] += 1;
      traffic.links['resultq-collector'] += 1;
      traffic.links['collector-redis'] += 1;
    }
  });

  return traffic;
}

function aggregateTrafficFrames(frames) {
  const aggregate = emptyTrafficSnapshot();
  frames.forEach((frame) => {
    aggregate.executions += frame.traffic.executions;
    mergeTierCounts(aggregate.queuedByTier, frame.traffic.queuedByTier);
    mergeTierCounts(aggregate.startedByTier, frame.traffic.startedByTier);
    mergeTierCounts(aggregate.doneByTier, frame.traffic.doneByTier);
    Object.entries(frame.traffic.links).forEach(([key, value]) => {
      aggregate.links[key] += value;
    });
  });
  return aggregate;
}

function emptyTrafficSnapshot() {
  return {
    executions: 0,
    queuedByTier: emptyTierCounts(),
    startedByTier: emptyTierCounts(),
    doneByTier: emptyTierCounts(),
    links: Object.fromEntries(ARCH_LINKS.map((link) => [link.metricKey, 0])),
  };
}

function emptyTierCounts() {
  return { EDGE: 0, FOG: 0, CLOUD: 0 };
}

function cloneTierCounts(source) {
  return {
    EDGE: source.EDGE || 0,
    FOG: source.FOG || 0,
    CLOUD: source.CLOUD || 0,
  };
}

function mergeTierCounts(target, source) {
  target.EDGE += source.EDGE || 0;
  target.FOG += source.FOG || 0;
  target.CLOUD += source.CLOUD || 0;
}

function parseStateTimestamp(nextState) {
  const parsed = Date.parse(nextState.timestamp || '');
  return Number.isFinite(parsed) ? parsed : Date.now();
}

function resolveExecutionTier(execution, task, serverById) {
  if (task && task.target_tier) return task.target_tier;
  if (!execution) return '';
  if (execution.target_tier) return execution.target_tier;
  if (execution.server_type) return execution.server_type;
  const serverID = execution.assigned_server || execution.server_id;
  if (!serverID || !serverById[serverID]) return '';
  return serverById[serverID].type || '';
}

function isScheduledTransition(previousStatus, currentStatus) {
  return (previousStatus === '' || previousStatus === 'PENDING')
    && (currentStatus === 'QUEUED' || currentStatus === 'RUNNING' || currentStatus === 'DONE');
}

function isWorkerStartTransition(previousStatus, currentStatus) {
  if (currentStatus !== 'RUNNING' && currentStatus !== 'DONE') {
    return false;
  }
  return previousStatus === 'QUEUED' || previousStatus === '' || previousStatus === 'PENDING';
}

// ── Server cards ──────────────────────────────────────────────────────────────
function renderServersTab() {
  renderServerGrid();
}

function renderServerGrid() {
  const grid = document.getElementById('server-grid');
  const summary = document.getElementById('server-summary');
  if (!grid) return;

  const { servers } = state;
  if (!servers.length) {
    grid.innerHTML = '';
    if (summary) summary.innerHTML = '';
    return;
  }

  const groups = ['EDGE', 'FOG', 'CLOUD'].map((type) => ({
    type,
    servers: servers
      .filter((server) => server.type === type)
      .sort((a, b) => a.id.localeCompare(b.id)),
  })).filter((group) => group.servers.length);

  if (summary) {
    const totals = groups.map((group) => {
      const active = sum(group.servers.map((server) => server.active_tasks));
      return `<span class="summary-chip">${group.type} <b>${group.servers.length} nodes / ${active} active</b></span>`;
    });
    summary.innerHTML = totals.join('');
  }

  grid.innerHTML = groups.map((group) => {
    const active = sum(group.servers.map((server) => server.active_tasks));
    return `
      <section class="server-tier">
        <div class="server-tier-head">
          <span class="server-tier-name ${group.type}">${group.type}</span>
          <span class="server-tier-meta">${group.servers.length} nodes · ${active} active tasks</span>
        </div>
        <div class="server-tier-cards">
          ${group.servers.map(serverCardMarkup).join('')}
        </div>
      </section>
    `;
  }).join('');
}

function pct(used, max) {
  if (!max) return 0;
  return Math.min(100, Math.round((used / max) * 100));
}

function serverCardMarkup(server) {
  const cpu = pct(server.used_cpu, server.max_cpu);
  const mem = pct(server.used_memory, server.max_memory);
  const disk = pct(server.used_disk, server.max_disk);
  const activePressure = Math.min(100, server.active_tasks * 28);
  const pressure = Math.max(cpu, mem, disk, activePressure);
  const pressureColor = trafficColor(pressure, 100);

  return `
    <div class="server-card ${server.type}" style="border-color:${d3.interpolateRgb('#2e3347', pressureColor)(0.38)}; background:#151b24">
      <div class="card-header">
        <span class="card-id">${server.id}</span>
        <span class="card-type type-${server.type}">${server.type}</span>
      </div>
      <div class="server-pressure">
        <div class="pressure-header"><span>Pressure</span><b>${pressure}%</b></div>
        <div class="pressure-track"><div class="pressure-fill" style="width:${pressure}%;background:${pressureColor}"></div></div>
      </div>
      <div class="resource-bar bar-cpu">
        <div class="res-label"><span>CPU</span><span>${server.used_cpu}/${server.max_cpu} cores</span></div>
        <div class="bar-track"><div class="bar-fill" style="width:${cpu}%"></div></div>
      </div>
      <div class="resource-bar bar-mem">
        <div class="res-label"><span>MEM</span><span>${server.used_memory}/${server.max_memory} MB</span></div>
        <div class="bar-track"><div class="bar-fill" style="width:${mem}%"></div></div>
      </div>
      <div class="resource-bar bar-disk">
        <div class="res-label"><span>DISK</span><span>${server.used_disk}/${server.max_disk} GB</span></div>
        <div class="bar-track"><div class="bar-fill" style="width:${disk}%"></div></div>
      </div>
      <div class="card-footer">
        <span>Latency <b>${server.latency}ms</b></span>
        <span>Active <b>${server.active_tasks}</b></span>
        <span>Done <b>${server.completed_tasks}</b></span>
      </div>
    </div>`;
}

// ══════════════════════════════════════════════════════════════════════════════
// TAB 2 — RESULTS
// ══════════════════════════════════════════════════════════════════════════════
function renderResultsTab() {
  const statusFilter = document.getElementById('status-filter').value;
  const tierFilter = document.getElementById('tier-filter').value;
  const tbody = document.getElementById('results-tbody');
  const empty = document.getElementById('results-empty');
  const counter = document.getElementById('results-count');
  const taskByID = Object.fromEntries(state.tasks.map((task) => [task.id, task]));
  const serverById = Object.fromEntries(state.servers.map((server) => [server.id, server]));
  const resultsByExecution = Object.fromEntries(state.results.map((result) => [result.execution_id, result]));

  let executions = [...state.executions].sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
  if (statusFilter) {
    executions = executions.filter((execution) => execution.status === statusFilter);
  }
  if (tierFilter) {
    executions = executions.filter((execution) => {
      const task = taskByID[execution.task_id];
      return resolveExecutionTier(execution, task, serverById) === tierFilter;
    });
  }

  counter.textContent = `${executions.length} execution${executions.length !== 1 ? 's' : ''}`;

  if (!executions.length) {
    tbody.innerHTML = '';
    empty.style.display = '';
    return;
  }
  empty.style.display = 'none';

  tbody.innerHTML = executions.map((execution) => {
    const task = taskByID[execution.task_id] || null;
    const result = resultsByExecution[execution.id] || null;
    const tier = resolveExecutionTier(execution, task, serverById);
    const serverID = execution.assigned_server || result?.server_id || '';
    const serverType = result?.server_type || (serverID && serverById[serverID]?.type) || '';
    const expanded = expandedRows.has(execution.id);
    const makespan = result ? Math.max(0, new Date(result.end_time) - new Date(result.start_time)) : null;

    const rows = [`
      <tr>
        <td>
          <code>${escapeHTML(execution.task_id)}</code>
          <div class="table-subtext">${escapeHTML(task?.name || 'Task')}</div>
          <div class="table-subtext">exec ${escapeHTML(execution.id)}</div>
        </td>
        <td>${tier ? `<span class="tier-pill ${tier}">${tier}</span>` : '—'}</td>
        <td>
          <div class="cost-stack">
            ${costChip('CPU', `${task?.cpu ?? '—'}${task ? 'c' : ''}`)}
            ${costChip('MEM', task ? `${task.memory}MB` : '—')}
            ${costChip('DISK', task ? `${task.disk}GB` : '—')}
            ${costChip('BASE', task ? formatDuration(task.duration) : '—')}
          </div>
        </td>
        <td><span class="status-badge badge-${execution.status}">${execution.status}</span></td>
        <td>
          ${serverID ? `<code>${escapeHTML(serverID)}</code>` : '—'}
          ${serverType ? `<div class="table-subtext">${escapeHTML(serverType)}</div>` : ''}
        </td>
        <td>
          ${makespan !== null ? formatDuration(makespan) : '—'}
          ${result ? `<div class="table-subtext">effective ${formatDuration(result.effective_duration_ms)}</div>` : ''}
        </td>
        <td>${fmtTime(execution.created_at)}</td>
        <td>
          <button class="expand-btn" onclick="toggleExpand('${execution.id}')">
            ${expanded ? '▲ Hide' : '▼ Show'}
          </button>
        </td>
      </tr>`,
    ];

    if (expanded) {
      rows.push(renderTaskDetailRow(execution, task, result));
    }

    return rows.join('');
  }).join('');
}

function renderTaskDetailRow(execution, task, result) {
  if (!result) {
    return `
      <tr>
        <td colspan="8" style="padding:0">
          <div class="result-empty-note">No execution result stored yet for ${escapeHTML(execution.id)}.</div>
        </td>
      </tr>`;
  }

  return `
    <tr>
      <td colspan="8" style="padding:0">
        <table class="results-table sub-table" style="margin-left:24px;width:calc(100% - 24px)">
          <thead>
            <tr>
              <th>Started</th>
              <th>Ended</th>
              <th>Algorithm</th>
              <th>Base</th>
              <th>Effective</th>
              <th>Dataflow</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>${fmtTime(result.start_time)}</td>
              <td>${fmtTime(result.end_time)}</td>
              <td><code>${escapeHTML(result.algorithm || '—')}</code></td>
              <td>${task ? formatDuration(task.duration) : formatDuration(result.duration_ms)}</td>
              <td>${formatDuration(result.effective_duration_ms)}</td>
              <td>
                <div class="dataflow-path">${
                  (result.dataflow_path || []).length
                    ? result.dataflow_path.map((step, index, array) =>
                      `<span>${escapeHTML(step)}</span>${index < array.length - 1 ? '<span class="arrow">→</span>' : ''}`
                    ).join('')
                    : '<span>direct</span>'
                }</div>
              </td>
            </tr>
          </tbody>
        </table>
      </td>
    </tr>`;
}

function costChip(label, value) {
  return `<span class="cost-chip"><span class="cost-chip-label">${label}</span><span>${value}</span></span>`;
}

window.toggleExpand = function(taskID) {
  if (expandedRows.has(taskID)) expandedRows.delete(taskID);
  else expandedRows.add(taskID);
  renderResultsTab();
};

function escapeHTML(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function formatDuration(ms) {
  const numeric = Number(ms) || 0;
  if (numeric >= 60000) return `${(numeric / 60000).toFixed(1)}m`;
  if (numeric >= 1000) return `${(numeric / 1000).toFixed(1)}s`;
  return `${numeric}ms`;
}

function fmtTime(iso) {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return '—';
  return date.toLocaleTimeString();
}

// ── Filter listeners ──────────────────────────────────────────────────────────
document.getElementById('status-filter').addEventListener('change', renderResultsTab);
document.getElementById('tier-filter').addEventListener('change', renderResultsTab);

// ── Boot ──────────────────────────────────────────────────────────────────────
connectWS();
