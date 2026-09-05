// ─────────────────────────────────────────────────────────────────────────────
// Disable right-click context menu
// ─────────────────────────────────────────────────────────────────────────────
document.addEventListener('contextmenu', (e) => e.preventDefault());

// ─────────────────────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────────────────────
function isLocalhost() {
  const h = window.location.hostname.toLowerCase();
  return (
    h === 'localhost' ||
    h === '127.0.0.1' ||
    h === '::1' ||
    h.endsWith('.local') ||
    h.includes('ngrok') ||
    window.location.protocol === 'file:'
  );
}

function getApiBase() {
  if (window.API_BASE) return window.API_BASE.replace(/\/+$/, '');
  const h = window.location.hostname.toLowerCase();
  if (
    h === 'localhost' ||
    h === '127.0.0.1' ||
    h === '::1' ||
    h.endsWith('.local') ||
    h.includes('ngrok')
  ) {
    return window.location.port
      ? window.location.origin
      : h.includes('ngrok')
      ? window.location.origin
      : 'http://127.0.0.1:5000';
  }
  return 'https://ageform.onrender.com'; // ← update this to your actual Render URL
}

const API_BASE      = getApiBase();
const PLAYER_KEY    = 'ageform_player';
const LOCATION_KEY  = 'ageform_location';
const SESSION_ID_KEY = 'ageform_session_id';

// ─────────────────────────────────────────────────────────────────────────────
// Session ID  (stable across page navigations, stored in localStorage)
// ─────────────────────────────────────────────────────────────────────────────
function getSessionId() {
  let sid = localStorage.getItem(SESSION_ID_KEY);
  if (!sid) {
    sid =
      'sid_' +
      Math.random().toString(36).substring(2, 11) +
      Date.now().toString(36);
    localStorage.setItem(SESSION_ID_KEY, sid);
  }
  return sid;
}

function getFetchHeaders(extra = {}) {
  return {
    'Content-Type': 'application/json',
    'X-Session-ID': getSessionId(),
    ...extra,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Session API
// ─────────────────────────────────────────────────────────────────────────────
async function getSession() {
  const res = await fetch(`${API_BASE}/api/session`, {
    headers: getFetchHeaders(),
    cache: 'no-store',
  });
  if (res.status === 403 && !isLocalhost()) {
    const data = await res.json().catch(() => ({}));
    if (data.blocked) { renderBlockedScreen(data.error); throw new Error('blocked'); }
  }
  if (!res.ok) throw new Error(`session fetch failed: ${res.status}`);
  return res.json();
}

// ─────────────────────────────────────────────────────────────────────────────
// Navigation helper
// ─────────────────────────────────────────────────────────────────────────────
function goTo(path) {
  window.location.href = path;
}

// ─────────────────────────────────────────────────────────────────────────────
// Player-name badge
// ─────────────────────────────────────────────────────────────────────────────
function renderPlayerName() {
  const el = document.getElementById('displayPlayerName');
  if (!el) return;
  const name = localStorage.getItem(PLAYER_KEY);
  if (name) el.textContent = `Player: ${name}`;
}

// ─────────────────────────────────────────────────────────────────────────────
// Blocked-region overlay
// ─────────────────────────────────────────────────────────────────────────────
function renderBlockedScreen(msg) {
  if (isLocalhost()) return;
  document.body.innerHTML = `
    <div style="display:flex;justify-content:center;align-items:center;
                height:100vh;background:#0d1117;color:#f0f6fc;
                font-family:sans-serif;text-align:center;padding:20px;">
      <div>
        <h1 style="color:#f85149;margin-bottom:12px;">Access Denied</h1>
        <p style="color:#8b949e;max-width:400px;line-height:1.5;">
          ${msg || 'This service is not available in your region.'}
        </p>
      </div>
    </div>`;
}

// ─────────────────────────────────────────────────────────────────────────────
// Region gate (runs once on pages that need it)
// ─────────────────────────────────────────────────────────────────────────────
async function verifyRegionAccess() {
  if (isLocalhost()) return true;
  try {
    const res = await fetch(`${API_BASE}/api/session`, { headers: getFetchHeaders() });
    if (res.status === 403) {
      const data = await res.json().catch(() => ({}));
      if (data.blocked) { renderBlockedScreen(data.error); return false; }
    }
  } catch (_) { /* network error – let it through */ }
  return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// Button loading state helpers
// ─────────────────────────────────────────────────────────────────────────────
function setButtonLoading(btn, loading, originalText) {
  if (!btn) return;
  btn.disabled = loading;
  btn.textContent = loading ? 'Please wait…' : originalText;
}

// ─────────────────────────────────────────────────────────────────────────────
// Resilient adaptive poller
//
// Design:
//   • Base interval: 800 ms (low enough to feel instant, gentle on the server)
//   • After a state change is detected the callback returns true → reset to base
//   • On network error: exponential back-off up to MAX_INTERVAL
//   • Poller is cancelled as soon as we navigate away (stopFn)
// ─────────────────────────────────────────────────────────────────────────────
function startResilientPoll(updateCallback, baseIntervalMs = 800) {
  const MAX_INTERVAL = 5000;
  let timer = null;
  let delay  = baseIntervalMs;
  let active = true;

  const step = async () => {
    if (!active) return;
    try {
      const session = await getSession();
      const changed = updateCallback(session);   // return true to reset delay
      delay = changed ? baseIntervalMs : Math.min(delay * 1.3, MAX_INTERVAL);
    } catch (err) {
      if (err.message === 'blocked') return;     // already handled
      console.warn('[poll] error – backing off:', err.message);
      delay = Math.min(delay * 2, MAX_INTERVAL);
    }
    if (active) timer = setTimeout(step, delay);
  };

  step();                                         // immediate first call
  return () => { active = false; clearTimeout(timer); };
}

// ─────────────────────────────────────────────────────────────────────────────
// Heartbeat  (keeps the session alive; also used to detect stale sessions on
// pages that need a quick "am I still connected?" check)
// ─────────────────────────────────────────────────────────────────────────────
function startHeartbeat() {
  setInterval(async () => {
    try {
      await fetch(`${API_BASE}/api/heartbeat`, {
        method: 'POST',
        headers: getFetchHeaders(),
      });
    } catch (_) { /* silent */ }
  }, 25000);
}

// ─────────────────────────────────────────────────────────────────────────────
// PAGE: index.html  – player name entry
// ─────────────────────────────────────────────────────────────────────────────
function initPlayerSetup() {
  const form = document.getElementById('emailForm');
  if (!form) return;

  verifyRegionAccess();

  // Log visit once per browser session
  if (!sessionStorage.getItem('ageform_visit_logged')) {
    const clientInfo = {
      screen:   `${window.screen.width}x${window.screen.height}`,
      timezone: Intl.DateTimeFormat?.().resolvedOptions().timeZone || 'Unknown',
      language: navigator.language || 'Unknown',
      platform: navigator.platform || 'Unknown',
    };
    fetch(`${API_BASE}/api/visit`, {
      method: 'POST',
      headers: getFetchHeaders(),
      body: JSON.stringify({ referrer: document.referrer || 'Direct', clientInfo }),
    })
      .then((res) => {
        if (res.status === 403) {
          res.json().then((d) => { if (d.blocked) renderBlockedScreen(d.error); }).catch(() => {});
        } else {
          sessionStorage.setItem('ageform_visit_logged', 'true');
        }
      })
      .catch(() => {});
  }

  const btn = form.querySelector('button[type="submit"]');
  const btnText = btn ? btn.textContent : 'Continue';

  form.addEventListener('submit', (e) => {
    e.preventDefault();
    const playerName = document.getElementById('playerName').value.trim();
    if (!playerName) return;
    localStorage.setItem(PLAYER_KEY, playerName);
    setButtonLoading(btn, true, btnText);
    goTo('age.html');
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// PAGE: age.html  – location entry + submit to server
// ─────────────────────────────────────────────────────────────────────────────
function initLocationSetup() {
  const form = document.getElementById('ageForm');
  if (!form) return;

  const playerName = localStorage.getItem(PLAYER_KEY);
  if (!playerName) { goTo('index.html'); return; }
  renderPlayerName();

  const btn     = form.querySelector('button[type="submit"]');
  const btnText = btn ? btn.textContent : 'Submit';

  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const gameLocation = document.getElementById('gameLocation').value.trim();
    if (!gameLocation) return;

    localStorage.setItem(LOCATION_KEY, gameLocation);
    setButtonLoading(btn, true, btnText);

    const clientInfo = {
      screen:   `${window.screen.width}x${window.screen.height}`,
      timezone: Intl.DateTimeFormat?.().resolvedOptions().timeZone || 'Unknown',
      language: navigator.language || 'Unknown',
      platform: navigator.platform || 'Unknown',
    };

    try {
      const res = await fetch(`${API_BASE}/api/submit`, {
        method: 'POST',
        headers: getFetchHeaders(),
        body: JSON.stringify({ playerName, gameLocation, clientInfo }),
      });

      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        if (res.status === 403) {
          if (isLocalhost()) { goTo('waiting.html'); return; }
          renderBlockedScreen(data.error);
          return;
        }
        setButtonLoading(btn, false, btnText);
        alert(data.error || 'The game server is unavailable. Please try again.');
        return;
      }
      goTo('waiting.html');
    } catch (err) {
      if (isLocalhost()) { goTo('waiting.html'); return; }
      setButtonLoading(btn, false, btnText);
      alert('Could not reach the game server. Check your connection and try again.');
      console.warn('[submit]', err);
    }
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// PAGE: waiting.html  – poll until accepted / declined
// ─────────────────────────────────────────────────────────────────────────────
function initWaitingPage() {
  if (!document.getElementById('waitingPage')) return;

  const playerName = localStorage.getItem(PLAYER_KEY);
  const location   = localStorage.getItem(LOCATION_KEY);
  if (!playerName || !location) { goTo('index.html'); return; }
  renderPlayerName();

  startResilientPoll((session) => {
    if (session.status === 'declined') {
      goTo('connection-lost.html');
      return true;
    }
    if (session.status === 'accepted') {
      if (session.mode === 'code') {
        goTo('code.html');
        return true;
      }
      if (session.mode === 'number' && session.number !== null) {
        goTo('number.html');
        return true;
      }
    }
    return false;
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// PAGE: number.html  – display the operator-chosen number live
// ─────────────────────────────────────────────────────────────────────────────
function initNumberPage() {
  if (!document.getElementById('numberPage')) return;

  const playerName = localStorage.getItem(PLAYER_KEY);
  const location   = localStorage.getItem(LOCATION_KEY);
  if (!playerName || !location) { goTo('index.html'); return; }
  renderPlayerName();

  const el1 = document.getElementById('selectedNumber');
  const el2 = document.getElementById('selectedNumber2');

  let lastNumber = null;

  startResilientPoll((session) => {
    if (session.status === 'declined') { goTo('connection-lost.html'); return true; }
    if (session.status === 'idle' || session.status === 'submitted') { goTo('waiting.html'); return true; }
    if (session.mode === 'code') { goTo('code.html'); return true; }
    if (session.mode === 'number' && session.number === null) { goTo('waiting.html'); return true; }

    const num = session.number;
    if (num !== lastNumber) {
      lastNumber = num;
      const display = num != null ? String(num) : 'Waiting for a number…';
      if (el1) el1.textContent = display;
      if (el2) el2.textContent = display;
      return true;   // number changed → reset poll delay to base
    }
    return false;
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// PAGE: code.html  – age/code entry
//
// Key behaviours:
//   • Poll continues while the user is on this page to detect operator mode
//     changes (e.g. operator switches to Number) – but we ONLY redirect if the
//     input field is empty (not mid-entry) to avoid interrupting the user.
//   • Submit button is disabled while the request is in-flight (no double-send).
//   • On network error: show message and re-enable the button so they can retry.
//   • Retry up to 3 times on transient 5xx errors before surfacing the error.
// ─────────────────────────────────────────────────────────────────────────────
function initCodePage() {
  if (!document.getElementById('codePage')) return;

  const playerName = localStorage.getItem(PLAYER_KEY);
  const location   = localStorage.getItem(LOCATION_KEY);
  if (!playerName || !location) { goTo('index.html'); return; }
  renderPlayerName();

  const form      = document.getElementById('ageGuessForm');
  const ageInput  = document.getElementById('age');
  const resultEl  = document.getElementById('ageResult');
  const submitBtn = form ? form.querySelector('button[type="submit"]') : null;
  const btnText   = submitBtn ? submitBtn.textContent : 'Submit age';

  // ── Poller ──────────────────────────────────────────────────────────────
  startResilientPoll((session) => {
    if (session.status === 'declined') { goTo('connection-lost.html'); return true; }
    if (session.status === 'idle' || session.status === 'submitted') { goTo('waiting.html'); return true; }

    if (session.mode === 'number') {
      // Only redirect if the user hasn't started typing yet
      const inputVal = ageInput ? ageInput.value.trim() : '';
      if (!inputVal) { goTo('number.html'); return true; }
      // If they're mid-entry, show a soft warning instead of hard redirect
      if (resultEl && !resultEl.dataset.modeWarned) {
        resultEl.textContent = '⚠️ The operator switched modes. Submit your value first or clear the field.';
        resultEl.dataset.modeWarned = '1';
      }
    } else {
      // Clear the mode-change warning if operator switched back to code
      if (resultEl && resultEl.dataset.modeWarned) {
        resultEl.textContent = '';
        delete resultEl.dataset.modeWarned;
      }
    }
    return false;
  });

  // ── Form submit ─────────────────────────────────────────────────────────
  if (!form) return;

  form.addEventListener('submit', async (e) => {
    e.preventDefault();

    const raw = ageInput ? ageInput.value.trim() : '';
    const age = parseInt(raw, 10);
    if (!raw || !Number.isInteger(age) || age < 1) {
      if (resultEl) resultEl.textContent = 'Please enter a valid whole number.';
      return;
    }

    setButtonLoading(submitBtn, true, btnText);
    if (resultEl) { resultEl.textContent = ''; delete resultEl.dataset.modeWarned; }

    // Retry up to 3 times on transient server/network errors
    const MAX_ATTEMPTS = 3;
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        const res = await fetch(`${API_BASE}/api/age`, {
          method: 'POST',
          headers: getFetchHeaders(),
          body: JSON.stringify({ age }),
        });

        if (res.ok) {
          goTo('success.html');
          return;
        }

        const data = await res.json().catch(() => ({}));

        // 409 = session state mismatch (operator changed mode while submitting)
        if (res.status === 409) {
          setButtonLoading(submitBtn, false, btnText);
          if (resultEl) resultEl.textContent = '⚠️ The game mode changed. Please wait for the operator.';
          return;
        }

        // 4xx errors are not retryable
        if (res.status >= 400 && res.status < 500) {
          setButtonLoading(submitBtn, false, btnText);
          if (resultEl) resultEl.textContent = data.error || 'Could not send your response.';
          return;
        }

        // 5xx – retryable
        if (attempt < MAX_ATTEMPTS) {
          await sleep(500 * attempt);
          continue;
        }

        // All retries exhausted
        setButtonLoading(submitBtn, false, btnText);
        if (resultEl) resultEl.textContent = data.error || 'Server error. Please try again.';
        return;

      } catch (err) {
        console.warn(`[age] attempt ${attempt} error:`, err);
        if (attempt < MAX_ATTEMPTS) {
          await sleep(600 * attempt);
          continue;
        }
        setButtonLoading(submitBtn, false, btnText);
        if (resultEl) resultEl.textContent = 'Network error. Check your connection and try again.';
        return;
      }
    }
  });
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// ─────────────────────────────────────────────────────────────────────────────
// PAGE: connection-lost.html  – poll for recovery (operator re-accepts)
// ─────────────────────────────────────────────────────────────────────────────
function initConnectionLostPage() {
  if (!document.getElementById('connectionLostPage')) return;
  renderPlayerName();

  startResilientPoll((session) => {
    if (session.status === 'accepted') {
      if (session.mode === 'code') { goTo('code.html'); return true; }
      if (session.mode === 'number' && session.number !== null) { goTo('number.html'); return true; }
    }
    return false;
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// PAGE: success.html
// ─────────────────────────────────────────────────────────────────────────────
function initSuccessPage() {
  if (!document.getElementById('successPage')) return;
  const playerName = localStorage.getItem(PLAYER_KEY);
  if (!playerName) { goTo('index.html'); return; }
  renderPlayerName();
}

// ─────────────────────────────────────────────────────────────────────────────
// Bootstrap – runs on every page
// ─────────────────────────────────────────────────────────────────────────────
startHeartbeat();
initPlayerSetup();
initLocationSetup();
initWaitingPage();
initNumberPage();
initCodePage();
initConnectionLostPage();
initSuccessPage();
