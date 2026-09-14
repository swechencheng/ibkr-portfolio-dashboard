/**
 * portfolio.js
 *
 * Client-side logic for the IBKR Portfolio page.
 * - Connects to WebSocket at /ws for real-time updates
 * - Polls REST endpoints for data refresh
 * - Renders account summary, positions, orders, executions
 * - Supports table column sorting with click-to-sort headers
 * - Animates P&L value changes with flash effects
 */

// ── State ──────────────────────────────────────────────────────
const state = {
  summary: {},
  positions: [],
  orders: [],
  executions: [],
  pnl: {},
  wsConnected: false,
  ibkrConnected: null,
  lastUpdate: null,
  sortConfig: {
    positions: { key: 'symbol', dir: 'asc' },
    orders: { key: 'placedTime', dir: 'desc' },
    executions: { key: 'time', dir: 'desc' },
  },
  previousPnL: {}, // Track previous P&L values for flash animation
  expandedCombos: new Set(),
  expandedExecutions: new Set(),
};

const pathname = window.location.pathname; // e.g. "/paper" or "/real"
const currentEnv = (pathname === '/' || pathname === '') ? 'paper' : pathname.replace('/', '');

const API_BASE = '/api/' + currentEnv;
const POLL_INTERVAL = 10000; // 10 seconds fallback polling
let pollTimer = null;
let ws = null;
let wsReconnectTimer = null;

// ── Formatters ─────────────────────────────────────────────────

function formatNumber(val, decimals = 2) {
  if (val == null || isNaN(val)) return '—';
  return Number(val).toLocaleString('en-US', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

function formatCurrency(val, decimals = 2) {
  if (val == null || isNaN(val)) return '—';
  const num = Number(val);
  const sign = num >= 0 ? '' : '';
  return sign + formatNumber(val, decimals);
}

function formatPnL(val, decimals = 2) {
  if (val == null || isNaN(val)) return '—';
  const num = Number(val);
  const prefix = num > 0 ? '+' : '';
  return prefix + formatNumber(val, decimals);
}

function formatPercent(val) {
  if (val == null || isNaN(val)) return '—';
  const num = Number(val);
  const prefix = num > 0 ? '+' : '';
  return prefix + num.toFixed(2) + '%';
}

function formatTime(isoStr) {
  if (!isoStr) return '—';
  const d = new Date(isoStr);
  return d.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function formatDateTime(isoStr) {
  if (!isoStr) return '—';
  const d = new Date(isoStr);
  return d.toLocaleDateString('en-GB', { month: 'short', day: 'numeric' }) + ' ' +
    d.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
}

function pnlClass(val) {
  if (val == null || isNaN(val)) return 'neutral';
  const num = Number(val);
  if (num > 0) return 'positive';
  if (num < 0) return 'negative';
  return 'neutral';
}

function actionTag(action) {
  if (!action) return '';
  const upper = action.toUpperCase();
  if (upper === 'BUY' || upper === 'BOT') {
    return `<span class="buy-tag">${upper}</span>`;
  }
  if (upper === 'SELL' || upper === 'SLD') {
    return `<span class="sell-tag">${upper}</span>`;
  }
  return `<span>${action}</span>`;
}

function orderTypeTag(type) {
  if (!type) return '';
  return `<span class="order-type-tag">${type}</span>`;
}

function statusTag(status) {
  if (!status) return '';
  let cls = 'status-active';
  const s = status.toLowerCase();
  if (s.includes('submit') || s.includes('presubmit')) cls = 'status-submitted';
  else if (s.includes('fill')) cls = 'status-filled';
  else if (s.includes('cancel') || s.includes('inactive')) cls = 'status-cancelled';
  return `<span class="status-tag ${cls}">${status}</span>`;
}

function pnlBar(pnlPct) {
  if (pnlPct == null || isNaN(pnlPct) || pnlPct === 0) return '';
  const width = Math.min(Math.abs(pnlPct) * 3, 60);
  const cls = pnlPct > 0 ? 'positive' : 'negative';
  return `<span class="pnl-bar ${cls}" style="width:${width}px"></span>`;
}

// ── API Calls ──────────────────────────────────────────────────

async function fetchJSON(url) {
  try {
    const res = await fetch(API_BASE + url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (err) {
    console.warn(`Failed to fetch ${url}:`, err.message);
    return null;
  }
}

async function fetchSummary() {
  const data = await fetchJSON('/portfolio/summary');
  if (data) {
    state.summary = data;
    renderSummary();
  }
}

async function fetchPositions() {
  const data = await fetchJSON('/portfolio/positions');
  if (data && data.positions) {
    state.positions = data.positions;
    state.pnl = data.pnl || {};
    renderPositions();
    renderStatRow();
  }
}

async function fetchOrders() {
  const data = await fetchJSON('/portfolio/orders');
  if (data && data.orders) {
    state.orders = data.orders;
    renderOrders();
  }
}

async function fetchExecutions() {
  const data = await fetchJSON('/portfolio/executions');
  if (data && data.executions) {
    state.executions = data.executions;
    renderExecutions();
  }
}

async function refreshAll() {
  const btn = document.getElementById('refreshBtn');
  btn.classList.add('loading');

  await Promise.all([
    fetchSummary(),
    fetchPositions(),
    fetchOrders(),
    fetchExecutions(),
  ]);

  state.lastUpdate = new Date();
  updateLastUpdated();
  btn.classList.remove('loading');
}

// ── WebSocket ──────────────────────────────────────────────────

function connectWebSocket() {
  if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;

  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const url = `${protocol}//${window.location.host}/ws/${currentEnv}`;

  try {
    ws = new WebSocket(url);
  } catch (err) {
    console.warn('WebSocket creation failed:', err);
    scheduleReconnect();
    return;
  }

  ws.onopen = () => {
    console.log('WebSocket connected');
    state.wsConnected = true;
    updateConnectionBadge();
    // Cancel reconnect timer if any
    if (wsReconnectTimer) {
      clearTimeout(wsReconnectTimer);
      wsReconnectTimer = null;
    }
  };

  ws.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      handleWsMessage(msg);
    } catch (err) {
      // Ignore parse errors
    }
  };

  ws.onclose = () => {
    console.log('WebSocket disconnected');
    state.wsConnected = false;
    updateConnectionBadge();
    scheduleReconnect();
  };

  ws.onerror = () => {
    state.wsConnected = false;
    updateConnectionBadge();
  };
}

function scheduleReconnect() {
  if (wsReconnectTimer) return;
  wsReconnectTimer = setTimeout(() => {
    wsReconnectTimer = null;
    connectWebSocket();
  }, 3000);
}

function handleWsMessage(msg) {
  if (!msg || !msg.type) return;

  if (msg.type === 'order_update') {
    // Refresh orders and positions when order state changes
    if (msg.orders) {
      state.orders = msg.orders;
      renderOrders();
    } else {
      fetchOrders();
    }
    fetchPositions();
    fetchSummary();
    state.lastUpdate = new Date();
    updateLastUpdated();
  } else if (msg.type === 'execution_update' || msg.type === 'fill') {
    // A fill happened — refresh everything
    if (msg.executions) {
      state.executions = msg.executions;
      renderExecutions();
    } else {
      fetchExecutions();
    }
    fetchPositions();
    fetchSummary();
    state.lastUpdate = new Date();
    updateLastUpdated();
  } else if (msg.type === 'portfolio_update') {
    // Direct portfolio data update
    if (msg.summary) {
      state.summary = msg.summary;
      renderSummary();
    }
    if (msg.positions) {
      state.positions = msg.positions;
      renderPositions();
      renderStatRow();
    }
    state.lastUpdate = new Date();
    updateLastUpdated();
  } else if (msg.type === 'ibkr_status') {
    state.ibkrConnected = msg.connected;
    updateConnectionBadge();
  }
}

function updateConnectionBadge() {
  const badge = document.getElementById('connectionBadge');
  const text = document.getElementById('connectionText');

  if (!state.wsConnected) {
    badge.className = 'connection-badge disconnected';
    text.textContent = 'Backend Disconnected';
  } else if (state.ibkrConnected === false) {
    badge.className = 'connection-badge disconnected';
    text.textContent = 'IBKR Disconnected';
  } else {
    badge.className = 'connection-badge connected';
    text.textContent = 'Connected';
  }
}

// ── Renderers ──────────────────────────────────────────────────

function renderSummary() {
  const s = state.summary;
  const grid = document.getElementById('summaryGrid');
  if (!grid) return;

  const cards = [
    { label: 'Net Liquidation', key: 'netLiquidation', accent: 'blue', decimals: 0 },
    { label: 'Total Cash', key: 'totalCash', accent: 'cyan', decimals: 0 },
    { label: 'Unrealized P&L', key: 'unrealizedPnL', accent: 'green', pnl: true, decimals: 0 },
    { label: 'Realized P&L', key: 'realizedPnL', accent: 'green', pnl: true, decimals: 0 },
    { label: 'Buying Power', key: 'buyingPower', accent: 'purple', decimals: 0 },
    { label: 'Maint. Margin', key: 'maintMargin', accent: 'amber', decimals: 0 },
    { label: 'Available Funds', key: 'availableFunds', accent: 'cyan', decimals: 0 },
    { label: 'Cushion', key: 'cushion', accent: 'blue', percent: true },
  ];

  if (grid.children.length === cards.length) {
    cards.forEach((c, i) => {
      const raw = s[c.key];
      let displayVal = '—';
      let valClass = '';

      if (raw != null && !isNaN(raw)) {
        if (c.percent) {
          displayVal = Number(raw).toFixed(1) + '%';
        } else if (c.pnl) {
          displayVal = formatPnL(raw, c.decimals);
          valClass = pnlClass(raw);
        } else {
          displayVal = formatCurrency(raw, c.decimals);
        }
      }

      const cardEl = grid.children[i];
      const valEl = cardEl.querySelector('.value');
      if (valEl) {
        if (valEl.textContent !== displayVal) valEl.textContent = displayVal;
        const targetCls = `value ${valClass}`.trim();
        if (valEl.className !== targetCls) valEl.className = targetCls;
      }
    });
    return;
  }

  grid.innerHTML = cards.map(c => {
    const raw = s[c.key];
    let displayVal = '—';
    let valClass = '';

    if (raw != null && !isNaN(raw)) {
      if (c.percent) {
        displayVal = Number(raw).toFixed(1) + '%';
      } else if (c.pnl) {
        displayVal = formatPnL(raw, c.decimals);
        valClass = pnlClass(raw);
      } else {
        displayVal = formatCurrency(raw, c.decimals);
      }
    }

    return `
      <div class="summary-card accent-${c.accent}">
        <div class="label">${c.label}</div>
        <div class="value ${valClass}">${displayVal}</div>
        ${s.baseCurrency ? `<div class="sub">${s.baseCurrency}</div>` : (s.accountId ? `<div class="sub">${s.accountId}</div>` : '')}
      </div>
    `;
  }).join('');
}

function renderStatRow() {
  const pnl = state.pnl;
  const pos = state.positions;
  const row = document.getElementById('statRow');
  if (!row) return;

  const totalUnrealized = pos.reduce((sum, p) => sum + (p.unrealizedPnL || 0), 0);
  const totalRealized = pos.reduce((sum, p) => sum + (p.realizedPnL || 0), 0);
  const totalMarketValue = pos.reduce((sum, p) => sum + (p.marketValue || 0), 0);
  const winning = pos.filter(p => p.unrealizedPnL > 0).length;
  const losing = pos.filter(p => p.unrealizedPnL < 0).length;

  const pills = [
    { label: 'Total Mkt Value', value: formatCurrency(totalMarketValue, 0), cls: '' },
    { label: 'Total Unrealized', value: formatPnL(totalUnrealized, 0), cls: pnlClass(totalUnrealized) },
    { label: 'Total Realized', value: formatPnL(totalRealized, 0), cls: pnlClass(totalRealized) },
    { label: 'Winning', value: String(winning), cls: winning > 0 ? 'positive' : '' },
    { label: 'Losing', value: String(losing), cls: losing > 0 ? 'negative' : '' },
    { label: 'Positions', value: String(pos.length), cls: '' },
  ];

  if (row.children.length === pills.length) {
    pills.forEach((p, i) => {
      const valEl = row.children[i].querySelector('.stat-value');
      if (valEl) {
        if (valEl.textContent !== p.value) valEl.textContent = p.value;
        const targetCls = `stat-value ${p.cls}`.trim();
        if (valEl.className !== targetCls) valEl.className = targetCls;
      }
    });
    return;
  }

  row.innerHTML = pills.map(p =>
    `<div class="stat-pill">
      <span class="stat-label">${p.label}</span>
      <span class="stat-value ${p.cls}">${p.value}</span>
    </div>`
  ).join('');
}

function getComboId(symbol) {
  return 'combo-' + symbol.replace(/[^a-zA-Z0-9]/g, '-');
}

function getPosRowId(p) {
  if (p.secType === 'COMBO') {
    return getComboId(p.localSymbol || p.symbol);
  }
  if (p.secType === 'CASH') {
    return 'cash-' + (p.symbol || 'curr');
  }
  return 'pos-' + (p.conId || (p.localSymbol || p.symbol).replace(/[^a-zA-Z0-9]/g, '-'));
}

function toggleCombo(comboId) {
  if (state.expandedCombos.has(comboId)) {
    state.expandedCombos.delete(comboId);
  } else {
    state.expandedCombos.add(comboId);
  }

  const isExpanded = state.expandedCombos.has(comboId);
  const parentRow = document.getElementById(comboId);
  if (parentRow) parentRow.classList.toggle('expanded', isExpanded);

  const childRows = document.querySelectorAll(`.leg-${comboId}`);
  childRows.forEach(row => row.classList.toggle('expanded', isExpanded));
}

window.toggleCombo = toggleCombo;

function setupPositionsTableEvents() {
  const tbody = document.getElementById('positionsBody');
  if (!tbody || tbody.dataset.hasComboListener) return;
  tbody.dataset.hasComboListener = 'true';

  tbody.addEventListener('click', (e) => {
    const comboRow = e.target.closest('.combo-row');
    if (comboRow && comboRow.dataset.comboId) {
      toggleCombo(comboRow.dataset.comboId);
    }
  });
}

function updateCell(cell, text, className) {
  if (cell.textContent !== text) cell.textContent = text;
  if (className && cell.className !== className) cell.className = className;
}

function updateCellHTML(cell, html, className) {
  if (cell.innerHTML !== html) cell.innerHTML = html;
  if (className && cell.className !== className) cell.className = className;
}

function updatePositionRowDOM(row, p, isLeg = false) {
  const symbol = p.localSymbol || p.symbol;
  const posClass = p.position > 0 ? 'positive' : p.position < 0 ? 'negative' : '';

  // Flash animation logic specifically on the unrealized P&L cell
  const prevPnL = state.previousPnL[symbol];
  let flashClass = '';
  if (prevPnL !== undefined && prevPnL !== p.unrealizedPnL) {
    flashClass = p.unrealizedPnL > prevPnL ? 'flash-positive' : 'flash-negative';
  }
  state.previousPnL[symbol] = p.unrealizedPnL;

  const cells = row.children;
  if (cells.length < 10) return;

  // Change %
  updateCell(cells[1], formatPercent(p.changePercent), `num cell-change ${pnlClass(p.changePercent)}`);
  // P&L %
  updateCell(cells[2], formatPercent(p.pnlPercent), `num cell-pnl-pct ${pnlClass(p.pnlPercent)}`);
  // Mkt Price
  updateCell(cells[3], p.marketPrice ? formatNumber(p.marketPrice, 2) : '—', 'num cell-price');
  // Avg Price
  updateCell(cells[4], formatNumber(p.avgPrice, 2), 'num cell-avg-price');
  // Delta
  updateCell(cells[5], p.delta !== null && p.delta !== undefined ? formatNumber(p.delta, 3) : '—', 'num cell-delta');
  // Position
  updateCell(cells[6], formatNumber(p.position, 0), `num cell-pos ${posClass}`);
  // Mkt Value
  updateCell(cells[7], formatCurrency(p.marketValue, 0), 'num cell-mkt-val');
  // Unrealized P&L (with sparkline bar)
  const pnlHtml = isLeg
    ? formatPnL(p.unrealizedPnL, 0)
    : `${formatPnL(p.unrealizedPnL, 0)}${pnlBar(p.pnlPercent)}`;
  const pnlCls = `num cell-unrealized ${pnlClass(p.unrealizedPnL)}${flashClass ? ' ' + flashClass : ''}`;
  updateCellHTML(cells[8], pnlHtml, pnlCls);
  if (flashClass) {
    setTimeout(() => {
      cells[8].classList.remove('flash-positive', 'flash-negative');
    }, 800);
  }
  // Realized P&L
  updateCell(cells[9], formatPnL(p.realizedPnL, 0), `num cell-realized ${pnlClass(p.realizedPnL)}`);
}

function renderPositionRowHTML(p) {
  const symbol = p.localSymbol || p.symbol;
  const isCombo = p.secType === 'COMBO';
  const hasLegs = isCombo && p.legs && p.legs.length > 0;
  const rowId = getPosRowId(p);
  const isExpanded = isCombo && state.expandedCombos.has(rowId);
  const expandedClass = isExpanded ? 'expanded' : '';
  const posClass = p.position > 0 ? 'positive' : p.position < 0 ? 'negative' : '';

  let trHtml = `<tr id="${rowId}" class="${isCombo ? 'combo-row' : ''} ${expandedClass}" ${isCombo ? `data-combo-id="${rowId}"` : ''}>
    <td class="cell-symbol">
      <strong>
        ${isCombo ? '<span class="combo-icon">▶</span> ' : ''}${symbol}
      </strong>
    </td>
    <td class="num cell-change ${pnlClass(p.changePercent)}">${formatPercent(p.changePercent)}</td>
    <td class="num cell-pnl-pct ${pnlClass(p.pnlPercent)}">${formatPercent(p.pnlPercent)}</td>
    <td class="num cell-price">${p.marketPrice ? formatNumber(p.marketPrice, 2) : '—'}</td>
    <td class="num cell-avg-price">${formatNumber(p.avgPrice, 2)}</td>
    <td class="num cell-delta">${p.delta !== null && p.delta !== undefined ? formatNumber(p.delta, 3) : '—'}</td>
    <td class="num cell-pos ${posClass}">${formatNumber(p.position, 0)}</td>
    <td class="num cell-mkt-val">${formatCurrency(p.marketValue, 0)}</td>
    <td class="num cell-unrealized ${pnlClass(p.unrealizedPnL)}">${formatPnL(p.unrealizedPnL, 0)}${pnlBar(p.pnlPercent)}</td>
    <td class="num cell-realized ${pnlClass(p.realizedPnL)}">${formatPnL(p.realizedPnL, 0)}</td>
    <td class="mono cell-sectype" style="color:var(--text-dim)">${p.secType}</td>
  </tr>`;

  if (hasLegs) {
    const legsHtml = p.legs.map((leg, idx) => {
      const legSymbol = leg.localSymbol || leg.symbol;
      const legPosClass = leg.position > 0 ? 'positive' : leg.position < 0 ? 'negative' : '';
      return `<tr id="${rowId}-leg-${idx}" class="leg-row leg-${rowId} ${expandedClass}">
        <td class="cell-symbol">${legSymbol}</td>
        <td class="num cell-change ${pnlClass(leg.changePercent)}">${formatPercent(leg.changePercent)}</td>
        <td class="num cell-pnl-pct ${pnlClass(leg.pnlPercent)}">${formatPercent(leg.pnlPercent)}</td>
        <td class="num cell-price">${formatNumber(leg.marketPrice, 2)}</td>
        <td class="num cell-avgPrice">${formatNumber(leg.avgPrice, 2)}</td>
        <td class="num cell-delta">${leg.delta !== null && leg.delta !== undefined ? formatNumber(leg.delta, 3) : '—'}</td>
        <td class="num cell-pos ${legPosClass}">${formatNumber(leg.position, 0)}</td>
        <td class="num cell-mkt-val">${formatCurrency(leg.marketValue, 0)}</td>
        <td class="num cell-unrealized ${pnlClass(leg.unrealizedPnL)}">${formatPnL(leg.unrealizedPnL, 0)}</td>
        <td class="num cell-realized ${pnlClass(leg.realizedPnL)}">${formatPnL(leg.realizedPnL, 0)}</td>
        <td class="mono cell-sectype" style="color:var(--text-dim)"></td>
      </tr>`;
    }).join('');
    trHtml += legsHtml;
  }

  return trHtml;
}

function renderPositions() {
  const tbody = document.getElementById('positionsBody');
  const countBadge = document.getElementById('positionCount');
  const sorted = sortData(state.positions, state.sortConfig.positions);

  countBadge.textContent = sorted.length;

  if (sorted.length === 0) {
    tbody.innerHTML = '<tr><td colspan="11" class="empty-state"><div class="empty-icon">📭</div>No positions</td></tr>';
    return;
  }

  setupPositionsTableEvents();

  // Determine expected row IDs
  const expectedRowIds = [];
  for (const p of sorted) {
    const rowId = getPosRowId(p);
    expectedRowIds.push(rowId);
    if (p.secType === 'COMBO' && p.legs && p.legs.length > 0) {
      p.legs.forEach((_, idx) => expectedRowIds.push(`${rowId}-leg-${idx}`));
    }
  }

  // Check if current DOM matches expectedRowIds exactly in sequence
  const existingRows = tbody.querySelectorAll('tr[id]');
  const matchesSequence =
    existingRows.length === expectedRowIds.length &&
    Array.from(existingRows).every((r, i) => r.id === expectedRowIds[i]);

  if (matchesSequence) {
    // Fast path: In-place DOM update without destroying ANY <tr> elements
    for (const p of sorted) {
      const rowId = getPosRowId(p);
      const row = document.getElementById(rowId);
      if (row) {
        updatePositionRowDOM(row, p, false);
      }
      if (p.secType === 'COMBO' && p.legs && p.legs.length > 0) {
        p.legs.forEach((leg, idx) => {
          const legRow = document.getElementById(`${rowId}-leg-${idx}`);
          if (legRow) {
            updatePositionRowDOM(legRow, leg, true);
          }
        });
      }
    }
  } else {
    // Full render path: rebuild HTML (on sort change, positions added/removed, or initial load)
    tbody.innerHTML = sorted.map(p => renderPositionRowHTML(p)).join('');
  }
}

function renderOrders() {
  const tbody = document.getElementById('ordersBody');
  const countBadge = document.getElementById('orderCount');
  const sorted = sortData(state.orders, state.sortConfig.orders);

  countBadge.textContent = sorted.length;

  if (sorted.length === 0) {
    tbody.innerHTML = '<tr><td colspan="7" class="empty-state"><div class="empty-icon">📭</div>No open orders</td></tr>';
    return;
  }

  tbody.innerHTML = sorted.map(o => {
    const symbol = o.localSymbol || o.symbol;
    const priceStr = o.price != null ? formatNumber(o.price, 2) : '—';
    const canCancel = o.status && !o.status.toLowerCase().includes('cancel') && !o.status.toLowerCase().includes('fill');

    return `<tr>
      <td><strong>${symbol}</strong></td>
      <td class="num">${priceStr}</td>
      <td class="num">${o.totalQuantity}</td>
      <td>${actionTag(o.action)}</td>
      <td>${orderTypeTag(o.orderType)}</td>
      <td>${statusTag(o.status)}</td>
      <td>${canCancel ? `<button class="cancel-btn" onclick="cancelOrder(${o.orderId})">Cancel</button>` : ''}</td>
    </tr>`;
  }).join('');
}

function toggleExecGroup(execId) {
  if (state.expandedExecutions.has(execId)) {
    state.expandedExecutions.delete(execId);
  } else {
    state.expandedExecutions.add(execId);
  }

  const isExpanded = state.expandedExecutions.has(execId);
  const parentRow = document.getElementById(`exec-row-${execId}`);
  if (parentRow) parentRow.classList.toggle('expanded', isExpanded);

  const childRows = document.querySelectorAll(`.sub-exec-${execId}`);
  childRows.forEach(row => row.classList.toggle('expanded', isExpanded));
}

window.toggleExecGroup = toggleExecGroup;

function renderExecutions() {
  const tbody = document.getElementById('executionsBody');
  const countBadge = document.getElementById('execCount');
  const sorted = sortData(state.executions, state.sortConfig.executions);

  countBadge.textContent = sorted.length;

  if (sorted.length === 0) {
    tbody.innerHTML = '<tr><td colspan="7" class="empty-state"><div class="empty-icon">📭</div>No executions</td></tr>';
    return;
  }

  tbody.innerHTML = sorted.map(e => {
    const execId = String(e.permId || e.orderId || e.execId || Math.random());
    const hasSubs = e.subExecutions && e.subExecutions.length > 1;
    const isExpanded = hasSubs && state.expandedExecutions.has(execId);
    const expandedClass = isExpanded ? 'expanded' : '';

    const qtyStr = e.quantityDisplay || (e.quantity != null ? String(e.quantity) : '—');
    const isPartial = e.isPartial || (e.totalQuantity && e.quantity < e.totalQuantity);
    const qtyHtml = isPartial
      ? `<span class="qty-partial">${qtyStr}</span>`
      : qtyStr;

    const rowOnClick = hasSubs ? `onclick="toggleExecGroup('${execId}')"` : '';
    const groupClass = hasSubs ? 'exec-group-row' : '';

    let html = `<tr id="exec-row-${execId}" class="${groupClass} ${expandedClass}" ${rowOnClick}>
      <td class="mono" style="color:var(--text-dim)">
        ${hasSubs ? `<span class="combo-icon">▶</span>` : ''}
        ${formatTime(e.time)}
      </td>
      <td><strong>${e.localSymbol || e.symbol}</strong></td>
      <td class="num ${pnlClass(e.realizedPnL)}">${e.realizedPnL != null ? formatPnL(e.realizedPnL, 2) : '—'}</td>
      <td class="num" style="color:var(--text-dim)">${e.commission ? formatNumber(e.commission, 2) : '—'}</td>
      <td class="num">${formatNumber(e.price, 2)}</td>
      <td class="num">${qtyHtml}</td>
      <td>${actionTag(e.side)}</td>
    </tr>`;

    if (hasSubs) {
      e.subExecutions.forEach(sub => {
        html += `<tr class="sub-exec-row sub-exec-${execId} ${expandedClass}">
          <td class="mono" style="color:var(--text-dim)">${formatTime(sub.time)}</td>
          <td style="color:var(--text-secondary)">${sub.localSymbol || sub.symbol}</td>
          <td class="num ${pnlClass(sub.realizedPnL)}">${sub.realizedPnL != null ? formatPnL(sub.realizedPnL, 2) : '—'}</td>
          <td class="num" style="color:var(--text-dim)">${sub.commission ? formatNumber(sub.commission, 2) : '—'}</td>
          <td class="num">${formatNumber(sub.price, 2)}</td>
          <td class="num">${sub.quantity}</td>
          <td>${actionTag(sub.side)}</td>
        </tr>`;
      });
    }

    return html;
  }).join('');
}

// ── Sorting ────────────────────────────────────────────────────

function sortData(data, config) {
  if (!config || !config.key) return data;
  const arr = [...data];
  const { key, dir } = config;
  const mult = dir === 'asc' ? 1 : -1;

  arr.sort((a, b) => {
    let va = a[key];
    let vb = b[key];
    if (va == null) va = '';
    if (vb == null) vb = '';
    if (typeof va === 'number' && typeof vb === 'number') {
      return (va - vb) * mult;
    }
    return String(va).localeCompare(String(vb)) * mult;
  });

  return arr;
}

function setupSortHeaders() {
  const tables = {
    positionsTable: 'positions',
    ordersTable: 'orders',
    executionsTable: 'executions',
  };

  for (const [tableId, stateKey] of Object.entries(tables)) {
    const table = document.getElementById(tableId);
    if (!table) continue;

    const headers = table.querySelectorAll('thead th[data-sort]');
    headers.forEach(th => {
      th.addEventListener('click', () => {
        const key = th.dataset.sort;
        const current = state.sortConfig[stateKey];

        // Toggle direction
        if (current.key === key) {
          current.dir = current.dir === 'asc' ? 'desc' : 'asc';
        } else {
          current.key = key;
          current.dir = 'desc';
        }

        // Update visual indicators
        table.querySelectorAll('thead th').forEach(h => {
          h.classList.remove('sorted-asc', 'sorted-desc');
        });
        th.classList.add(current.dir === 'asc' ? 'sorted-asc' : 'sorted-desc');

        // Re-render
        if (stateKey === 'positions') renderPositions();
        else if (stateKey === 'orders') renderOrders();
        else if (stateKey === 'executions') renderExecutions();
      });
    });
  }
}

// ── Actions ────────────────────────────────────────────────────

async function cancelOrder(orderId) {
  try {
    const res = await fetch(API_BASE + '/cancel_order', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ orderId }),
    });
    if (!res.ok) {
      const err = await res.json();
      alert('Failed to cancel order: ' + (err.detail || res.statusText));
      return;
    }
    // Refresh orders after cancel
    setTimeout(() => fetchOrders(), 500);
  } catch (err) {
    alert('Failed to cancel order: ' + err.message);
  }
}

// ── Lifecycle ──────────────────────────────────────────────────

function updateLastUpdated() {
  const el = document.getElementById('lastUpdated');
  if (state.lastUpdate) {
    el.textContent = 'Updated ' + formatTime(state.lastUpdate.toISOString());
  }
}

function startPolling() {
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(() => {
    refreshAll();
  }, POLL_INTERVAL);
}

// ── Init ───────────────────────────────────────────────────────

async function init() {
  setupSortHeaders();
  connectWebSocket();

  const envSelect = document.getElementById('envSelect');
  if (envSelect) {
    envSelect.value = '/' + currentEnv;
    if (currentEnv === 'real') {
      envSelect.style.color = 'var(--amber)';
      envSelect.style.borderColor = 'var(--amber-dim)';
      envSelect.style.backgroundColor = 'rgba(245, 158, 11, 0.1)';
    } else {
      envSelect.style.color = 'var(--blue)';
      envSelect.style.borderColor = 'var(--blue-dim)';
      envSelect.style.backgroundColor = 'rgba(59, 130, 246, 0.1)';
    }
  }

  await refreshAll();
  startPolling();
}

document.addEventListener('DOMContentLoaded', init);
