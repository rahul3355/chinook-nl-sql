// ─── Config ───────────────────────────────────────────────
const LOADING_WORDS = [
  'Generating','Thinking','Pondering','Analyzing',
  'Inspecting','Exploring','Reviewing','Calculating',
  'Interpreting','Understanding','Preparing','Reasoning'
];

// ─── DOM ──────────────────────────────────────────────────
const chatArea     = document.getElementById('chat-area');
const emptyState   = document.getElementById('empty-state');
const messagesEl   = document.getElementById('messages');
const questionInput = document.getElementById('question-input');
const sendBtn      = document.getElementById('send-btn');
const historyList  = document.getElementById('history-list');

// ─── State ────────────────────────────────────────────────
let isLoading       = false;
let wordInterval    = null;
let wordIdx         = 0;
let historyItems    = [];

// ─── Helpers ──────────────────────────────────────────────
function escapeHtml(s) {
  return String(s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;')
    .replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function suggestionMarkup(suggestions = []) {
  if (!suggestions || suggestions.length === 0) return '';

  return `
    <div class="suggestion-chips-container">
      <div class="suggestion-label">Suggested next steps</div>
      ${suggestions.map(item => `
        <button class="suggestion-chip" data-question="${escapeHtml(item.question || '')}">
          <span>${escapeHtml(item.question || '')}</span>
          ${item.rationale ? `<span class="suggestion-rationale">${escapeHtml(item.rationale)}</span>` : ''}
        </button>
      `).join('')}
    </div>`;
}

function attachMessageInteractions(root) {
  if (!root) return;

  const chartBtn = root.querySelector('.chart-trigger-btn');
  if (chartBtn && !chartBtn.dataset.bound) {
    chartBtn.dataset.bound = '1';
    chartBtn.addEventListener('click', () => {
      const q = chartBtn.getAttribute('data-question');
      const s = chartBtn.getAttribute('data-sql');
      generateChart(q, s, chartBtn);
    });
  }

  root.querySelectorAll('.suggestion-chip').forEach(btn => {
    if (btn.dataset.bound) return;
    btn.dataset.bound = '1';
    btn.addEventListener('click', () => {
      const question = btn.getAttribute('data-question') || btn.textContent.trim();
      setQuestion(null, question);
      sendMessage();
    });
  });
}

function setArtifactsState(messageEl, suggestions = []) {
  if (!messageEl) return;
  const container = messageEl.querySelector('.message-artifacts');
  if (!container) return;
  container.innerHTML = suggestionMarkup(suggestions);
  attachMessageInteractions(container);
  scrollToBottom();
}

function formatTs(ts) {
  try { return new Date(ts).toLocaleString(); } catch { return ts; }
}

function scrollToBottom() {
  requestAnimationFrame(() => { chatArea.scrollTop = chatArea.scrollHeight; });
}

function resetInputHeight() {
  questionInput.style.height = 'auto';
}


// ─── History Sidebar ──────────────────────────────────────
async function fetchHistory() {
  try {
    const res      = await fetch('/history');
    const allItems = await res.json();
    const total    = allItems.length;
    historyItems   = allItems.slice().reverse().slice(0, 25).map((item, di) => ({
      ...item,
      _origIdx: total - 1 - di
    }));
    renderHistory(historyItems);
  } catch { /* silent */ }
}

function renderHistory(items) {
  historyItems = items || [];
  if (historyItems.length === 0) {
    historyList.innerHTML = '<div class="history-empty">No queries yet</div>';
    return;
  }
  historyList.innerHTML = historyItems.map((item, i) => `
    <div class="history-item"
         title="${escapeHtml(item.question)}"
         onclick="loadHistoryEntry(${i})">
      <span class="hist-label">${escapeHtml(item.question)}</span>
      <button class="delete-hist-btn" onclick="deleteHistoryEntry(${i}, event)" title="Delete">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2">
          <polyline points="3 6 5 6 21 6"/>
          <path d="M19 6l-1 14H6L5 6"/>
          <path d="M10 11v6"/><path d="M14 11v6"/>
          <path d="M9 6V4h6v2"/>
        </svg>
      </button>
    </div>`).join('');
}

async function deleteHistoryEntry(i, event) {
  event.stopPropagation();
  const item = historyItems[i];
  if (!item) return;
  await fetch(`/history/${item._origIdx}`, { method: 'DELETE' }).catch(() => {});
  fetchHistory();
}

function loadHistoryEntry(idx) {
  const item = historyItems[idx];
  if (!item || isLoading) return;
  newSession();
  hideEmptyState();
  appendUserMessage(item.question);
  appendHistoryAnswer(item.question, item.answer, item.sql, item.row_count, item.timestamp, item.suggestions || []);
}

function appendHistoryAnswer(question, answer, sql, rowCount, timestamp, suggestions = []) {
  const div = document.createElement('div');
  div.className = 'message assistant-message';
  const uid = 'dp' + Date.now();
  const avatarSvg = `<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2">
    <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/>
  </svg>`;
  div.innerHTML = `
    <div class="assistant-avatar">${avatarSvg}</div>
    <div class="assistant-body">
      <div class="bubble">${escapeHtml(answer)}</div>
      <div class="message-details">
        <button class="details-toggle" id="tb-${uid}" onclick="toggleDetails('${uid}')">
          <svg class="chevron" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
            <polyline points="6 9 12 15 18 9"/>
          </svg>
          <span>Show details</span>
        </button>
        <div class="details-panel" id="${uid}">
          <div class="detail-row">
            <span class="detail-label">SQL Query</span>
            <code class="detail-code">${escapeHtml(sql)}</code>
          </div>
          <div class="detail-row">
            <span class="detail-label">Rows Returned</span>
            <span class="detail-value">${rowCount || 'N/A'}</span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Timestamp</span>
            <span class="detail-value">${formatTs(timestamp)}</span>
          </div>
        </div>
      </div>
      ${(rowCount > 1 || !rowCount) ? `
      <div class="suggested-actions">
        <button class="action-btn chart-trigger-btn" data-question="${escapeHtml(question)}" data-sql="${escapeHtml(sql)}">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
            <polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/>
          </svg>
          Generate Chart
        </button>
      </div>` : ''}
      <div class="message-artifacts">${suggestionMarkup(suggestions)}</div>
    </div>`;
  messagesEl.appendChild(div);
  attachMessageInteractions(div);
  scrollToBottom();
}

// ─── Chip / History click ─────────────────────────────────
function setQuestion(el, text) {
  const q = text || (el && el.textContent.trim());
  if (!q) return;
  questionInput.value = q;
  questionInput.style.height = 'auto';
  questionInput.style.height = questionInput.scrollHeight + 'px';
  questionInput.focus();
}

// ─── Empty State ──────────────────────────────────────────
function hideEmptyState() {
  if (emptyState.style.display !== 'none') {
    emptyState.style.display = 'none';
  }
}

// ─── New Session ──────────────────────────────────────────
function newSession() {
  if (isLoading) return;
  stopLoadingAnim();
  messagesEl.innerHTML = '';
  emptyState.style.display = '';
  questionInput.value = '';
  resetInputHeight();
  questionInput.focus();
}

// ─── Sample Questions Toggle ──────────────────────────────
function toggleSampleQuestions() {
  const panel  = document.getElementById('sample-panel');
  const btn    = document.getElementById('sample-toggle-btn');
  const open   = panel.classList.toggle('visible');
  btn.classList.toggle('open', open);
}

// ─── User Message ─────────────────────────────────────────
function appendUserMessage(question) {
  const div = document.createElement('div');
  div.className = 'message user-message';
  div.innerHTML = `<div class="bubble">${escapeHtml(question)}</div>`;
  messagesEl.appendChild(div);
  scrollToBottom();
}

// ─── Loading Bubble ───────────────────────────────────────
function appendLoadingBubble() {
  const div = document.createElement('div');
  div.className = 'message assistant-message';
  div.id = 'loading-msg';
  div.innerHTML = `
    <div class="thinking-indicator">
      <div class="thinking-spinner"></div>
      <span id="thinking-text">${LOADING_WORDS[0]}...</span>
    </div>`;
  messagesEl.appendChild(div);
  scrollToBottom();
  startLoadingAnim();
}

function startLoadingAnim() {
  wordIdx = Math.floor(Math.random() * LOADING_WORDS.length);
  const el = document.getElementById('thinking-text');
  if (el) el.textContent = LOADING_WORDS[wordIdx] + '...';
  wordInterval = setInterval(() => {
    wordIdx = (wordIdx + 1) % LOADING_WORDS.length;
    const el = document.getElementById('thinking-text');
    if (el) el.textContent = LOADING_WORDS[wordIdx] + '...';
  }, 2000);
}

function stopLoadingAnim() {
  clearInterval(wordInterval);
  wordInterval = null;
}

// ─── Assistant Answer ─────────────────────────────────────
function resolveLoadingBubble(question, answer, sql, rowCount, timestamp, suggestions = [], historyId = null) {
  stopLoadingAnim();
  const msg = document.getElementById('loading-msg');
  if (!msg) return;
  msg.id = '';
  if (historyId) msg.dataset.historyId = historyId;

  const uid = 'dp' + Date.now();
  msg.innerHTML = `
    <div class="assistant-avatar">
      <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2">
        <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/>
      </svg>
    </div>
    <div class="assistant-body">
      <div class="bubble">${escapeHtml(answer)}</div>
      <div class="message-details">
        <button class="details-toggle" id="tb-${uid}" onclick="toggleDetails('${uid}')">
          <svg class="chevron" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
            <polyline points="6 9 12 15 18 9"/>
          </svg>
          <span>Show details</span>
        </button>
        <div class="details-panel" id="${uid}">
          <div class="detail-row">
            <span class="detail-label">SQL Query</span>
            <code class="detail-code">${escapeHtml(sql)}</code>
          </div>
          <div class="detail-row">
            <span class="detail-label">Rows Returned</span>
            <span class="detail-value">${rowCount}</span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Timestamp</span>
            <span class="detail-value">${formatTs(timestamp)}</span>
          </div>
        </div>
      </div>
      ${(rowCount > 1 || !rowCount) ? `
      <div class="suggested-actions">
        <button class="action-btn chart-trigger-btn" 
                data-question="${escapeHtml(question)}" 
                data-sql="${escapeHtml(sql)}">
          <svg class="btn-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
            <polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/>
          </svg>
          Generate Chart
        </button>
      </div>` : ''}
      <div class="message-artifacts">${suggestionMarkup(suggestions)}</div>
    </div>`;

  attachMessageInteractions(msg);
  scrollToBottom();
  return msg;
}

async function generateChart(question, sql, btn) {
  if (!window.Plotly) {
    alert("Plotly library not loaded.");
    return;
  }
  
  btn.disabled = true;
  btn.innerHTML = `
    <div class="thinking-spinner" style="width:12px; height:12px; border-width:2px"></div>
    Generating...
  `;
  
  try {
    const res = await fetch('/generate_chart', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question, sql })
    });
    
    if (!res.ok) {
      const e = await res.json().catch(() => ({}));
      throw new Error(e.detail || "Chart generation failed");
    }
    
    const data = await res.json();
    const figure = JSON.parse(data.chart_json);
    
    const chartDiv = document.createElement('div');
    chartDiv.className = 'chart-container';
    const chartId = 'chart-' + Date.now();
    chartDiv.id = chartId;
    
    const body = btn.parentElement.parentElement;
    const message = body.closest('.message');
    body.classList.add('has-chart');
    if (message) {
      message.classList.add('expanded-chart');
    }
    body.appendChild(chartDiv);
    
    Plotly.newPlot(chartId, figure.data, figure.layout, {
      responsive: true,
      displayModeBar: false
    });
    
    btn.parentElement.style.display = 'none';
    setTimeout(scrollToBottom, 300);
  } catch (err) {
    console.error("Chart Error:", err);
    btn.disabled = false;
    btn.classList.add('error');
    btn.innerHTML = `
      <svg class="btn-icon" style="color:#ef4444" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
        <circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/>
      </svg>
      Failed. Try again?
    `;
  }
}

function resolveError(text) {
  stopLoadingAnim();
  const msg = document.getElementById('loading-msg');
  if (!msg) return;
  msg.id = '';
  msg.innerHTML = `
    <div class="assistant-avatar" style="background:#ef4444">
      <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2">
        <circle cx="12" cy="12" r="10"/>
        <line x1="12" y1="8" x2="12" y2="12"/>
        <line x1="12" y1="16" x2="12.01" y2="16"/>
      </svg>
    </div>
    <div class="assistant-body">
      <div class="bubble" style="color:#dc2626">${escapeHtml(text)}</div>
    </div>`;
  scrollToBottom();
}

// ─── Toggle Details ───────────────────────────────────────
function toggleDetails(uid) {
  const panel  = document.getElementById(uid);
  const toggle = document.getElementById('tb-' + uid);
  if (!panel || !toggle) return;
  const open = panel.classList.toggle('visible');
  toggle.classList.toggle('open', open);
  toggle.querySelector('span').textContent = open ? 'Hide details' : 'Show details';
}

// ─── Send ─────────────────────────────────────────────────
async function sendMessage() {
  if (isLoading) return;
  const question = questionInput.value.trim();
  if (!question) return;

  isLoading = true;
  sendBtn.disabled = true;
  questionInput.value = '';
  resetInputHeight();
  hideEmptyState();
  appendUserMessage(question);
  appendLoadingBubble();

  try {
    const res = await fetch('/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question })
    });
    if (!res.ok) {
      const e = await res.json().catch(() => ({}));
      throw new Error(e.detail || `Server error ${res.status}`);
    }
    const data = await res.json();
    const visibleSuggestions = data.suggestions || [];
    resolveLoadingBubble(question, data.answer, data.sql, data.row_count, data.timestamp, visibleSuggestions, data.history_id);
    fetchHistory();
  } catch (err) {
    resolveError(err.message || 'Something went wrong. Please try again.');
  } finally {
    isLoading = false;
    sendBtn.disabled = false;
    questionInput.focus();
  }
}

// ─── Input Events ─────────────────────────────────────────
questionInput.addEventListener('keydown', e => {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault();
    sendMessage();
  }
});

questionInput.addEventListener('input', () => {
  questionInput.style.height = 'auto';
  questionInput.style.height = Math.min(questionInput.scrollHeight, 140) + 'px';
});

// ─── Init ─────────────────────────────────────────────────
fetchHistory();

window.addEventListener('resize', () => {
    if (typeof Plotly === 'undefined') return;
    const charts = document.querySelectorAll('.chart-container');
    charts.forEach(el => {
        if (el.id) {
            Plotly.Plots.resize(el);
        }
    });
});
