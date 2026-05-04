const express = require('express');
const fetch = require('node-fetch');
const cron = require('node-cron');

const app = express();
app.use(express.json());

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
const GOOGLE_KEY = process.env.GOOGLE_API_KEY;
const SHEET_ID = '1fDdMfqzat1XGbbc9G3bbI1x9QN-gFM3Udc-nUAtT3QM';
const SHEET_NAME = 'Booked Jobs';

if (!TELEGRAM_TOKEN) {
  console.error('FATAL: TELEGRAM_TOKEN env var not set. Add it on Render and redeploy.');
  process.exit(1);
}
if (!ANTHROPIC_KEY) console.warn('WARN: ANTHROPIC_API_KEY not set');
if (!GOOGLE_KEY) {
  console.error('FATAL: GOOGLE_API_KEY env var not set.');
  process.exit(1);
}

// Writes go through an Apps Script proxy living inside the sheet.
// If either env var is missing, the bot runs in read-only mode and refuses write commands.
const APPS_SCRIPT_URL = process.env.APPS_SCRIPT_URL;
const APPS_SCRIPT_SECRET = process.env.APPS_SCRIPT_SECRET;
const writesEnabled = !!(APPS_SCRIPT_URL && APPS_SCRIPT_SECRET);
if (!writesEnabled) {
  console.warn('APPS_SCRIPT_URL or APPS_SCRIPT_SECRET not set — running in READ-ONLY mode. Write commands will be rejected.');
} else {
  console.log(`Writes enabled via Apps Script proxy: ${APPS_SCRIPT_URL}`);
}

const ALLOWED_CHAT_IDS = (process.env.ALLOWED_CHAT_IDS || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);
if (ALLOWED_CHAT_IDS.length === 0) {
  console.error('FATAL: ALLOWED_CHAT_IDS env var not set. Add a comma-separated list of authorized Telegram chat IDs.');
  process.exit(1);
}

// Morning brief configuration (cron pattern, IANA timezone, comma-separated chat IDs).
// All optional; defaults to 8AM America/New_York to every chat in ALLOWED_CHAT_IDS.
const BRIEF_CRON = process.env.BRIEF_CRON || '0 8 * * *';
const BRIEF_TIMEZONE = process.env.BRIEF_TIMEZONE || 'America/New_York';
const BRIEF_CHAT_IDS = (process.env.BRIEF_CHAT_IDS || ALLOWED_CHAT_IDS.join(','))
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

// Column indices (0-based) — matches sheet exactly
const COL = {
  JOB: 0, VENDOR_ORDER: 1, PHONE: 2, EMAIL: 3, BOOKED_DATE: 4,
  INITIAL_SERVICE: 5, STATUS: 6, FROM_STATE: 7, TO_STATE: 8, PROVIDER: 9,
  VENDOR_COST: 10, VENDOR_STORAGE: 11, AVANTI_STORAGE: 12, SELL_PRICE: 13,
  MARGIN: 14, MARGIN_PCT: 15, DEPOSIT_AMT: 16, DEPOSIT_DATE: 17,
  PAYMENT_METHOD: 18, BALANCE: 19, BALANCE_DATE: 20, BALANCE_PAYMENT: 21, NOTES: 22
};

// Statuses that exclude a job from active/billable consideration
const NON_ACTIVE_STATUSES = ['completed', 'cancelled', 'future order'];

// Diagnostic state — populated on every getJobs() call, surfaced by /debug
const diag = {
  lastSheetPull: null,
  lastSheetStatus: 'never pulled',
  lastSheetError: null,
  lastHttpStatus: null,
  headerIdx: null,
  totalRows: null,
  firstColAValues: [],
  rawHeaders: [],
  jobsLoaded: null,
  multiContainerGroups: [],
  cacheHits: 0,
  cacheMisses: 0,
  bootedAt: new Date()
};

// ─────────────────────────────────────────────────────────────────────────────
// Vendor + state lookup tables
// ─────────────────────────────────────────────────────────────────────────────

// Each entry: { match: substring or regex (lowercased), label: canonical filter token }
// Filter checks if the row's PROVIDER (lowercased) includes the label.
const VENDOR_PATTERNS = [
  { match: 'pods', label: 'pods' },
  { match: '1800packrat', label: 'packrat' },
  { match: '1-800-packrat', label: 'packrat' },
  { match: '1-800-pack-rat', label: 'packrat' },
  { match: 'packrat', label: 'packrat' },
  { match: 'pack rat', label: 'packrat' },
  { match: 'zippy', label: 'zippy' },
  { match: 'units', label: 'units' },
  { match: 'estes', label: 'estes' },
  { match: 'suremove', label: 'estes' },
  { match: 'old dominion', label: 'old dominion' },
  { match: 'odhh', label: 'old dominion' }
];

// US state lookup. Keys are lowercased name + 2-letter code; value is the canonical 2-letter code.
const STATES = {
  'al': 'AL', 'alabama': 'AL', 'ak': 'AK', 'alaska': 'AK',
  'az': 'AZ', 'arizona': 'AZ', 'ar': 'AR', 'arkansas': 'AR',
  'ca': 'CA', 'california': 'CA', 'co': 'CO', 'colorado': 'CO',
  'ct': 'CT', 'connecticut': 'CT', 'de': 'DE', 'delaware': 'DE',
  'fl': 'FL', 'florida': 'FL', 'ga': 'GA', 'georgia': 'GA',
  'hi': 'HI', 'hawaii': 'HI', 'id': 'ID', 'idaho': 'ID',
  'il': 'IL', 'illinois': 'IL', 'in': 'IN', 'indiana': 'IN',
  'ia': 'IA', 'iowa': 'IA', 'ks': 'KS', 'kansas': 'KS',
  'ky': 'KY', 'kentucky': 'KY', 'la': 'LA', 'louisiana': 'LA',
  'me': 'ME', 'maine': 'ME', 'md': 'MD', 'maryland': 'MD',
  'ma': 'MA', 'massachusetts': 'MA', 'mi': 'MI', 'michigan': 'MI',
  'mn': 'MN', 'minnesota': 'MN', 'ms': 'MS', 'mississippi': 'MS',
  'mo': 'MO', 'missouri': 'MO', 'mt': 'MT', 'montana': 'MT',
  'ne': 'NE', 'nebraska': 'NE', 'nv': 'NV', 'nevada': 'NV',
  'nh': 'NH', 'new hampshire': 'NH', 'nj': 'NJ', 'new jersey': 'NJ',
  'nm': 'NM', 'new mexico': 'NM', 'ny': 'NY', 'new york': 'NY',
  'nc': 'NC', 'north carolina': 'NC', 'nd': 'ND', 'north dakota': 'ND',
  'oh': 'OH', 'ohio': 'OH', 'ok': 'OK', 'oklahoma': 'OK',
  'or': 'OR', 'oregon': 'OR', 'pa': 'PA', 'pennsylvania': 'PA',
  'ri': 'RI', 'rhode island': 'RI', 'sc': 'SC', 'south carolina': 'SC',
  'sd': 'SD', 'south dakota': 'SD', 'tn': 'TN', 'tennessee': 'TN',
  'tx': 'TX', 'texas': 'TX', 'ut': 'UT', 'utah': 'UT',
  'vt': 'VT', 'vermont': 'VT', 'va': 'VA', 'virginia': 'VA',
  'wa': 'WA', 'washington': 'WA', 'wv': 'WV', 'west virginia': 'WV',
  'wi': 'WI', 'wisconsin': 'WI', 'wy': 'WY', 'wyoming': 'WY',
  'dc': 'DC', 'district of columbia': 'DC'
};

// 2-letter codes that collide with common English words — require uppercase context to match
const AMBIGUOUS_STATE_CODES = new Set(['or', 'in', 'me', 'hi', 'al', 'la', 'ok', 'pa', 'co', 'de']);

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

function parseDate(str) {
  if (!str || str.trim() === '') return null;
  str = str.trim();
  const md = str.match(/^(\d{1,2})\/(\d{1,2})(?:\/(\d{2,4}))?$/);
  if (md) {
    const month = parseInt(md[1]);
    const day = parseInt(md[2]);
    if (md[3]) {
      let year = parseInt(md[3]);
      if (year < 100) year += 2000;
      return new Date(year, month - 1, day);
    }
    const today = new Date();
    const thisYear = today.getFullYear();
    const candidate = new Date(thisYear, month - 1, day);
    const sixMonthsAhead = new Date(today);
    sixMonthsAhead.setMonth(sixMonthsAhead.getMonth() + 6);
    if (candidate > sixMonthsAhead) {
      return new Date(thisYear - 1, month - 1, day);
    }
    return candidate;
  }
  const full = new Date(str);
  if (!isNaN(full)) return full;
  return null;
}

function fmtDate(d) {
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

function fmtTimestamp(d) {
  if (!d) return 'never';
  return d.toISOString().replace('T', ' ').slice(0, 19) + ' UTC';
}

function fmtMoney(n) {
  return '$' + (n || 0).toLocaleString(undefined, { maximumFractionDigits: 0 });
}

function stripDollar(str) {
  if (!str) return 0;
  return parseFloat(str.toString().replace(/[$,\s]/g, '')) || 0;
}

function get(row, col) {
  return (row[col] || '').toString().trim();
}

function isActive(row) {
  const status = get(row, COL.STATUS).toLowerCase();
  if (!status) return false;
  return !NON_ACTIVE_STATUSES.some(s => status.includes(s));
}

// ─────────────────────────────────────────────────────────────────────────────
// Sheet loading
// ─────────────────────────────────────────────────────────────────────────────

let jobCache = null;
let cacheTime = 0;
const CACHE_TTL = 60000;

function invalidateCache() {
  jobCache = null;
  cacheTime = 0;
}

async function getJobs(forceRefresh = false) {
  if (!forceRefresh && jobCache && (Date.now() - cacheTime) < CACHE_TTL) {
    diag.cacheHits++;
    console.log('Using cached data');
    return jobCache;
  }
  diag.cacheMisses++;
  const range = encodeURIComponent(SHEET_NAME + '!A:Z');
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${SHEET_ID}/values/${range}?key=${GOOGLE_KEY}`;
  let res, data;
  try {
    res = await fetch(url);
    diag.lastHttpStatus = res.status;
    data = await res.json();
  } catch (e) {
    diag.lastSheetStatus = 'fetch failed';
    diag.lastSheetError = e.message;
    diag.lastSheetPull = new Date();
    throw e;
  }
  if (!data.values) {
    diag.lastSheetStatus = `Sheets API error (HTTP ${res.status})`;
    diag.lastSheetError = JSON.stringify(data).slice(0, 500);
    diag.lastSheetPull = new Date();
    console.error('Google Sheets API response:', JSON.stringify(data));
    throw new Error('No sheet data');
  }
  const rows = data.values;
  diag.totalRows = rows.length;
  diag.firstColAValues = rows.slice(0, 12).map((r, i) =>
    `[row ${i}] "${(r[0] || '').toString().replace(/\n/g, '\\n').trim()}"`
  );

  let headerIdx = -1;
  for (let i = 0; i < rows.length; i++) {
    if ((rows[i][0] || '').toString().trim() === 'Job #') { headerIdx = i; break; }
  }
  diag.headerIdx = headerIdx;
  if (headerIdx === -1) {
    diag.lastSheetStatus = 'header row not found';
    diag.lastSheetPull = new Date();
    throw new Error('Header row not found');
  }
  const rawHeaders = rows[headerIdx];
  diag.rawHeaders = rawHeaders.map((h, i) =>
    `${String.fromCharCode(65 + i)}: "${(h || '').toString().replace(/\n/g, '\\n').trim()}"`
  );

  // Each row = one container. Same Job # can appear on multiple rows (multi-container customers).
  const jobs = [];
  const jobIdRows = {};
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    const jobId = get(r, COL.JOB);
    if (!jobId || !jobId.startsWith('A')) continue;
    r._sheetRow = i + 1; // 1-based row number as it appears in the sheet UI
    jobs.push(r);
    (jobIdRows[jobId] = jobIdRows[jobId] || []).push(i + 1);
  }
  const multiContainerGroups = Object.entries(jobIdRows)
    .filter(([, sheetRows]) => sheetRows.length > 1)
    .map(([jobId, sheetRows]) => `${jobId} × ${sheetRows.length} containers (sheet rows ${sheetRows.join(', ')})`);
  console.log(`Loaded ${jobs.length} container rows (${multiContainerGroups.length} multi-container job(s))`);
  diag.jobsLoaded = jobs.length;
  diag.multiContainerGroups = multiContainerGroups;
  diag.lastSheetStatus = 'OK';
  diag.lastSheetError = null;
  diag.lastSheetPull = new Date();
  jobCache = jobs;
  cacheTime = Date.now();
  return jobs;
}

// ─────────────────────────────────────────────────────────────────────────────
// Sheet writes — go through Apps Script proxy living inside the sheet
// ─────────────────────────────────────────────────────────────────────────────

function colLetter(idx) {
  return String.fromCharCode(65 + idx);
}

async function callAppsScript(action, params) {
  if (!writesEnabled) {
    throw new Error('Write commands disabled. Set APPS_SCRIPT_URL and APPS_SCRIPT_SECRET on Render.');
  }
  const res = await fetch(APPS_SCRIPT_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ secret: APPS_SCRIPT_SECRET, action, ...params })
  });
  const text = await res.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch (e) {
    throw new Error(`Apps Script returned non-JSON (HTTP ${res.status}): ${text.slice(0, 200)}`);
  }
  if (!data.ok) throw new Error(`Apps Script error: ${data.error || 'unknown'}`);
  return data;
}

async function updateCell(rowNumber, colIndex, value) {
  await callAppsScript('updateCell', { row: rowNumber, col: colIndex, value });
  invalidateCache();
}

async function updateCells(rowNumber, updates /* { colIndex: value, ... } */) {
  const updateList = Object.entries(updates).map(([col, value]) => ({ col: parseInt(col), value }));
  await callAppsScript('updateCells', { row: rowNumber, updates: updateList });
  invalidateCache();
}

// Locate a single row in the loaded jobs by Job #, optionally disambiguated by Vendor Order #.
function findJobRow(jobs, jobId, vendorOrder) {
  const upperJob = jobId.toUpperCase();
  const matches = jobs.filter(r => get(r, COL.JOB).toUpperCase() === upperJob);
  if (matches.length === 0) {
    return { error: `Job ${jobId} not found.` };
  }
  if (matches.length === 1) {
    return { row: matches[0] };
  }
  if (!vendorOrder) {
    const list = matches.map(r => `vo:${get(r, COL.VENDOR_ORDER) || '(blank)'} (sheet row ${r._sheetRow}, status ${get(r, COL.STATUS)})`).join('\n  ');
    return { error: `Job ${jobId} has ${matches.length} containers. Disambiguate with vo:VENDOR_ORDER#:\n  ${list}` };
  }
  const matched = matches.find(r => get(r, COL.VENDOR_ORDER) === vendorOrder);
  if (!matched) return { error: `No container with vo:${vendorOrder} for job ${jobId}.` };
  return { row: matched };
}

// Parse a write command's args into { jobId, vendorOrder, rest }.
// Format: <JOB#> [vo:VENDOR_ORDER#] <rest of args>
function parseWriteArgs(argString) {
  const tokens = argString.trim().split(/\s+/);
  if (tokens.length === 0 || !tokens[0]) return { error: 'Missing job number.' };
  const jobId = tokens[0];
  let vendorOrder = null;
  let restStart = 1;
  if (tokens[1] && tokens[1].toLowerCase().startsWith('vo:')) {
    vendorOrder = tokens[1].slice(3);
    restStart = 2;
  }
  const rest = tokens.slice(restStart).join(' ').trim();
  return { jobId, vendorOrder, rest };
}

function formatRowFull(r) {
  return [
    `Job #:               ${get(r, COL.JOB)}`,
    `Vendor Order #:      ${get(r, COL.VENDOR_ORDER)}`,
    `Sheet row:           ${r._sheetRow}`,
    `Phone:               ${get(r, COL.PHONE)}`,
    `Email:               ${get(r, COL.EMAIL)}`,
    `Booked Date:         ${get(r, COL.BOOKED_DATE)}`,
    `Initial Service:     ${get(r, COL.INITIAL_SERVICE)}`,
    `Status:              ${get(r, COL.STATUS)}`,
    `From → To:           ${get(r, COL.FROM_STATE)} → ${get(r, COL.TO_STATE)}`,
    `Service Provider:    ${get(r, COL.PROVIDER)}`,
    `Vendor Cost:         ${fmtMoney(stripDollar(get(r, COL.VENDOR_COST)))}`,
    `Vendor Storage:      ${fmtMoney(stripDollar(get(r, COL.VENDOR_STORAGE)))}`,
    `Avanti Storage:      ${fmtMoney(stripDollar(get(r, COL.AVANTI_STORAGE)))}`,
    `Sold Price (Avanti): ${fmtMoney(stripDollar(get(r, COL.SELL_PRICE)))}`,
    `Margin:              ${fmtMoney(stripDollar(get(r, COL.MARGIN)))} (${get(r, COL.MARGIN_PCT)})`,
    `Deposit:             ${fmtMoney(stripDollar(get(r, COL.DEPOSIT_AMT)))} on ${get(r, COL.DEPOSIT_DATE)} (${get(r, COL.PAYMENT_METHOD)})`,
    `Remaining Balance:   ${fmtMoney(stripDollar(get(r, COL.BALANCE)))} due ${get(r, COL.BALANCE_DATE)} (${get(r, COL.BALANCE_PAYMENT)})`,
    `Notes:               ${get(r, COL.NOTES) || '(none)'}`
  ].join('\n');
}

// ─────────────────────────────────────────────────────────────────────────────
// Query functions
// ─────────────────────────────────────────────────────────────────────────────

function nextBillingDate(svcDateStr) {
  const svcDate = parseDate(svcDateStr);
  if (!svcDate) return null;
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  let billing = new Date(svcDate);
  billing.setDate(billing.getDate() + 30);
  while (billing < today) {
    billing.setDate(billing.getDate() + 30);
  }
  return billing;
}

function getUpcomingJobs(jobs, days) {
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const end = new Date(today);
  end.setDate(end.getDate() + days);
  return jobs.filter(r => {
    const d = parseDate(get(r, COL.INITIAL_SERVICE));
    if (!d) return false;
    return d >= today && d <= end;
  }).sort((a, b) =>
    parseDate(get(a, COL.INITIAL_SERVICE)) - parseDate(get(b, COL.INITIAL_SERVICE))
  );
}

function getBalances(jobs) {
  return jobs.filter(r => stripDollar(get(r, COL.BALANCE)) > 0);
}

function getActiveJobs(jobs) {
  return jobs.filter(isActive);
}

// ─────────────────────────────────────────────────────────────────────────────
// Anomaly detection — daily revenue protection
// ─────────────────────────────────────────────────────────────────────────────

const NO_STORAGE_NOTE_PATTERNS = ['no storage', 'no extra storage', 'without storage', 'direct move', 'pickup directly'];

function notesIndicateNoStorage(row) {
  const notes = get(row, COL.NOTES).toLowerCase();
  if (!notes) return false;
  return NO_STORAGE_NOTE_PATTERNS.some(p => notes.includes(p));
}

function getFlags(jobs) {
  const flags = {
    cancelledWithBalance: [],
    completedWithBalance: [],
    statusNotesContradiction: [],
    activeOldNoStorage: [],
    negativeMargin: [],
    skinnyMargin: [],
    duplicateVendorOrder: [],
    missingInitialService: []
  };
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const t30 = new Date(today); t30.setDate(t30.getDate() - 30);

  // 1 & 2: cancelled / completed jobs with open balance
  for (const r of jobs) {
    const status = get(r, COL.STATUS).toLowerCase();
    const bal = stripDollar(get(r, COL.BALANCE));
    if (bal <= 0) continue;
    if (status.includes('cancelled')) flags.cancelledWithBalance.push({ row: r, balance: bal });
    else if (status.includes('completed')) flags.completedWithBalance.push({ row: r, balance: bal });
  }

  // 3: status / notes contradictions
  for (const r of jobs) {
    const status = get(r, COL.STATUS).toLowerCase();
    if (status.includes('storage') && notesIndicateNoStorage(r)) {
      flags.statusNotesContradiction.push({ row: r, reason: 'Status says storage, notes say no storage / direct move' });
    }
    if (status.includes('future order')) {
      const svc = parseDate(get(r, COL.INITIAL_SERVICE));
      if (svc && svc < today) {
        flags.statusNotesContradiction.push({ row: r, reason: `Status "Future Order" but service date ${get(r, COL.INITIAL_SERVICE)} has passed` });
      }
    }
    if (status.includes('completed')) {
      const svc = parseDate(get(r, COL.INITIAL_SERVICE));
      if (svc && svc > today) {
        flags.statusNotesContradiction.push({ row: r, reason: `Status "Completed" but service date ${get(r, COL.INITIAL_SERVICE)} is in the future` });
      }
    }
  }

  // 4: active 30+ days out, Avanti Storage charge is empty/zero, notes don't say no-storage
  for (const r of jobs) {
    if (!isActive(r)) continue;
    const svc = parseDate(get(r, COL.INITIAL_SERVICE));
    if (!svc || svc > t30) continue;
    if (stripDollar(get(r, COL.AVANTI_STORAGE)) > 0) continue;
    if (notesIndicateNoStorage(r)) continue;
    const daysOut = Math.floor((today - svc) / 86400000);
    flags.activeOldNoStorage.push({ row: r, daysOut });
  }

  // 5: margin issues (skip cancelled — irrelevant)
  for (const r of jobs) {
    if (get(r, COL.STATUS).toLowerCase().includes('cancelled')) continue;
    const margin = stripDollar(get(r, COL.MARGIN));
    const sold = stripDollar(get(r, COL.SELL_PRICE));
    if (sold === 0) continue;
    if (margin < 0) {
      flags.negativeMargin.push({ row: r, margin });
    } else {
      const pct = (margin / sold) * 100;
      if (pct < 5) flags.skinnyMargin.push({ row: r, margin, pct });
    }
  }

  // 6: duplicate Vendor Order # across different Job #s (multi-container with same Job # is OK)
  const byVO = {};
  for (const r of jobs) {
    const vo = get(r, COL.VENDOR_ORDER);
    if (!vo) continue;
    (byVO[vo] = byVO[vo] || []).push(r);
  }
  for (const [vo, rows] of Object.entries(byVO)) {
    if (rows.length < 2) continue;
    const uniqueJobs = new Set(rows.map(r => get(r, COL.JOB)));
    if (uniqueJobs.size > 1) flags.duplicateVendorOrder.push({ vendorOrder: vo, rows });
  }

  // 7: active jobs with no Initial Service Date (bot can't bill or schedule them)
  for (const r of jobs) {
    if (!isActive(r)) continue;
    if (!get(r, COL.INITIAL_SERVICE)) flags.missingInitialService.push({ row: r });
  }

  return flags;
}

function flagsTotal(flags) {
  return Object.values(flags).reduce((s, arr) => s + arr.length, 0);
}

function formatFlags(flags) {
  const total = flagsTotal(flags);
  if (total === 0) return 'No flags. Sheet looks clean.';
  const lines = [];
  lines.push(`AVANTI FLAGS — ${total} item(s) need attention`);
  lines.push('='.repeat(54));
  lines.push('');

  if (flags.cancelledWithBalance.length) {
    lines.push(`CANCELLED JOBS WITH OPEN BALANCE (${flags.cancelledWithBalance.length})`);
    lines.push('  Either refund owed OR balance should be zeroed out.');
    flags.cancelledWithBalance.forEach(({ row: r, balance }) =>
      lines.push(`  ${get(r, COL.JOB)} | ${get(r, COL.PHONE)} | ${get(r, COL.PROVIDER)} | $${balance.toLocaleString()} sitting on Cancelled job`)
    );
    lines.push('');
  }

  if (flags.completedWithBalance.length) {
    lines.push(`COMPLETED JOBS WITH OPEN BALANCE (${flags.completedWithBalance.length})`);
    lines.push('  Uncollected revenue OR balance should be zeroed.');
    flags.completedWithBalance.forEach(({ row: r, balance }) =>
      lines.push(`  ${get(r, COL.JOB)} | ${get(r, COL.PHONE)} | ${get(r, COL.PROVIDER)} | $${balance.toLocaleString()} still owed`)
    );
    lines.push('');
  }

  if (flags.statusNotesContradiction.length) {
    lines.push(`STATUS / NOTES CONTRADICTIONS (${flags.statusNotesContradiction.length})`);
    flags.statusNotesContradiction.forEach(({ row: r, reason }) => {
      lines.push(`  ${get(r, COL.JOB)} | ${reason}`);
      const notes = get(r, COL.NOTES);
      if (notes) lines.push(`    Notes: "${notes}"`);
    });
    lines.push('');
  }

  if (flags.activeOldNoStorage.length) {
    lines.push(`ACTIVE 30+ DAYS, NO STORAGE CHARGE SET (${flags.activeOldNoStorage.length})`);
    lines.push('  Container has been out a month+ but Avanti Storage cell is $0.');
    lines.push('  Either missing recurring revenue or the cell needs to be filled.');
    flags.activeOldNoStorage.forEach(({ row: r, daysOut }) =>
      lines.push(`  ${get(r, COL.JOB)} | ${get(r, COL.PHONE)} | ${get(r, COL.PROVIDER)} | ${daysOut} days out | Status: ${get(r, COL.STATUS)}`)
    );
    lines.push('');
  }

  if (flags.negativeMargin.length) {
    lines.push(`NEGATIVE MARGIN — VENDOR COST EXCEEDED SELL PRICE (${flags.negativeMargin.length})`);
    flags.negativeMargin.forEach(({ row: r, margin }) =>
      lines.push(`  ${get(r, COL.JOB)} | ${get(r, COL.PROVIDER)} | Sold ${fmtMoney(stripDollar(get(r, COL.SELL_PRICE)))} | Vendor cost ${fmtMoney(stripDollar(get(r, COL.VENDOR_COST)))} | Margin ${fmtMoney(margin)} | Status: ${get(r, COL.STATUS)}`)
    );
    lines.push('');
  }

  if (flags.skinnyMargin.length) {
    lines.push(`SKINNY MARGIN — UNDER 5% (${flags.skinnyMargin.length})`);
    flags.skinnyMargin.forEach(({ row: r, margin, pct }) =>
      lines.push(`  ${get(r, COL.JOB)} | ${get(r, COL.PROVIDER)} | Sold ${fmtMoney(stripDollar(get(r, COL.SELL_PRICE)))} | Margin ${fmtMoney(margin)} (${pct.toFixed(1)}%) | Status: ${get(r, COL.STATUS)}`)
    );
    lines.push('');
  }

  if (flags.duplicateVendorOrder.length) {
    lines.push(`DUPLICATE VENDOR ORDER # ACROSS DIFFERENT JOBS (${flags.duplicateVendorOrder.length})`);
    lines.push('  Same vendor order # appears on multiple Job #s — usually a data entry error.');
    flags.duplicateVendorOrder.forEach(({ vendorOrder, rows }) => {
      const jobIds = [...new Set(rows.map(r => get(r, COL.JOB)))].join(', ');
      lines.push(`  Vendor Order ${vendorOrder} → Jobs: ${jobIds}`);
    });
    lines.push('');
  }

  if (flags.missingInitialService.length) {
    lines.push(`ACTIVE JOBS MISSING INITIAL SERVICE DATE (${flags.missingInitialService.length})`);
    lines.push("  Bot can't compute storage billing without this. Fill it in.");
    flags.missingInitialService.forEach(({ row: r }) =>
      lines.push(`  ${get(r, COL.JOB)} | ${get(r, COL.PHONE)} | ${get(r, COL.PROVIDER)} | Status: ${get(r, COL.STATUS)}`)
    );
    lines.push('');
  }

  return lines.join('\n');
}

function summarizeFlagsForBrief(flags) {
  const total = flagsTotal(flags);
  if (total === 0) return null;
  const lines = [];
  lines.push(`NEEDS YOUR ATTENTION (${total} flag${total === 1 ? '' : 's'})`);
  if (flags.cancelledWithBalance.length) lines.push(`  ${flags.cancelledWithBalance.length} cancelled job(s) with open balance`);
  if (flags.completedWithBalance.length) lines.push(`  ${flags.completedWithBalance.length} completed job(s) with open balance`);
  if (flags.statusNotesContradiction.length) lines.push(`  ${flags.statusNotesContradiction.length} status/notes contradiction(s)`);
  if (flags.activeOldNoStorage.length) lines.push(`  ${flags.activeOldNoStorage.length} active job(s) 30+ days, no storage charge`);
  if (flags.negativeMargin.length) lines.push(`  ${flags.negativeMargin.length} negative-margin job(s)`);
  if (flags.skinnyMargin.length) lines.push(`  ${flags.skinnyMargin.length} skinny-margin job(s) (<5%)`);
  if (flags.duplicateVendorOrder.length) lines.push(`  ${flags.duplicateVendorOrder.length} duplicate vendor order #(s)`);
  if (flags.missingInitialService.length) lines.push(`  ${flags.missingInitialService.length} active job(s) missing service date`);
  lines.push('  Run /flags for detail.');
  return lines.join('\n');
}

function getStorageBillingDue(jobs, days) {
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const end = new Date(today);
  end.setDate(end.getDate() + days);
  const activeJobs = jobs.filter(isActive);
  const results = [];
  for (const r of activeJobs) {
    const svcDateStr = get(r, COL.INITIAL_SERVICE);
    const nextBilling = nextBillingDate(svcDateStr);
    if (!nextBilling) continue;
    if (nextBilling >= today && nextBilling <= end) {
      results.push({ row: r, nextBilling });
    }
  }
  results.sort((a, b) => a.nextBilling - b.nextBilling);
  return results;
}

function getHowMany(jobs) {
  const total = jobs.length;
  const active = jobs.filter(isActive);
  const completed = jobs.filter(r => get(r, COL.STATUS).toLowerCase().includes('completed'));
  const cancelled = jobs.filter(r => get(r, COL.STATUS).toLowerCase().includes('cancelled'));
  const future = jobs.filter(r => get(r, COL.STATUS).toLowerCase().includes('future'));
  const byStatus = {};
  jobs.forEach(r => {
    const s = get(r, COL.STATUS) || '(blank)';
    byStatus[s] = (byStatus[s] || 0) + 1;
  });
  const sum = (rows, col) => rows.reduce((s, r) => s + stripDollar(get(r, col)), 0);
  return {
    total,
    activeCount: active.length,
    completedCount: completed.length,
    cancelledCount: cancelled.length,
    futureCount: future.length,
    byStatus,
    soldAll: sum(jobs, COL.SELL_PRICE),
    marginAll: sum(jobs, COL.MARGIN),
    soldActive: sum(active, COL.SELL_PRICE),
    marginActive: sum(active, COL.MARGIN),
    soldCompleted: sum(completed, COL.SELL_PRICE),
    marginCompleted: sum(completed, COL.MARGIN),
    outstandingBalance: sum(jobs, COL.BALANCE)
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Filters (vendor, state) — composable on top of any base query
// ─────────────────────────────────────────────────────────────────────────────

function extractVendor(text) {
  const t = text.toLowerCase();
  for (const v of VENDOR_PATTERNS) {
    if (t.includes(v.match)) return v.label;
  }
  return null;
}

function extractState(text) {
  // Try multi-word state names first (e.g., "new york")
  const lower = text.toLowerCase();
  for (const [name, code] of Object.entries(STATES)) {
    if (name.includes(' ') && lower.includes(name)) return code;
  }
  // Then single-word names and unambiguous 2-letter codes
  const tokens = text.split(/[^A-Za-z]+/).filter(Boolean);
  for (const tok of tokens) {
    const lc = tok.toLowerCase();
    if (!STATES[lc]) continue;
    // Ambiguous 2-letter codes (in/or/me/hi/al/la/ok/pa/co/de) only count if the user wrote them in uppercase
    if (lc.length === 2 && AMBIGUOUS_STATE_CODES.has(lc) && tok !== tok.toUpperCase()) continue;
    return STATES[lc];
  }
  return null;
}

function applyVendorFilter(jobs, vendor) {
  if (!vendor) return jobs;
  return jobs.filter(r => get(r, COL.PROVIDER).toLowerCase().includes(vendor));
}

function applyStateFilter(jobs, state) {
  if (!state) return jobs;
  const upper = state.toUpperCase();
  return jobs.filter(r => {
    const from = get(r, COL.FROM_STATE).toUpperCase();
    const to = get(r, COL.TO_STATE).toUpperCase();
    return from === upper || to === upper;
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Formatting
// ─────────────────────────────────────────────────────────────────────────────

function formatJob(r) {
  const bal = stripDollar(get(r, COL.BALANCE));
  const balStr = bal > 0
    ? `$${bal.toLocaleString()} due ${get(r, COL.BALANCE_DATE)}`
    : 'Paid in full';
  return [
    get(r, COL.JOB), get(r, COL.PHONE),
    `${get(r, COL.FROM_STATE)}→${get(r, COL.TO_STATE)}`,
    get(r, COL.PROVIDER), `Svc: ${get(r, COL.INITIAL_SERVICE)}`,
    `Sell: $${stripDollar(get(r, COL.SELL_PRICE)).toLocaleString()}`,
    balStr, get(r, COL.STATUS)
  ].join(' | ');
}

function formatStorageBilling(entry) {
  const r = entry.row;
  const avantiStorage = stripDollar(get(r, COL.AVANTI_STORAGE));
  const vendorStorage = stripDollar(get(r, COL.VENDOR_STORAGE));
  const margin = avantiStorage - vendorStorage;
  return [
    get(r, COL.JOB), get(r, COL.PHONE),
    `${get(r, COL.FROM_STATE)}→${get(r, COL.TO_STATE)}`,
    get(r, COL.PROVIDER), `Svc started: ${get(r, COL.INITIAL_SERVICE)}`,
    `Next bill: ${fmtDate(entry.nextBilling)}`,
    `Charge: $${avantiStorage.toLocaleString()} | Vendor cost: $${vendorStorage.toLocaleString()} | Margin: $${margin.toFixed(2)}`,
    get(r, COL.STATUS),
    get(r, COL.NOTES) ? `Notes: ${get(r, COL.NOTES)}` : ''
  ].filter(Boolean).join(' | ');
}

function formatHowMany(stats, jobs) {
  const lines = [];
  lines.push(`COUNTS`);
  lines.push(`Total container rows: ${stats.total}`);
  lines.push(`Active (billable):    ${stats.activeCount}`);
  lines.push(`Completed:            ${stats.completedCount}`);
  lines.push(`Cancelled:            ${stats.cancelledCount}`);
  lines.push(`Future Order:         ${stats.futureCount}`);
  lines.push('');
  lines.push(`BY STATUS`);
  Object.entries(stats.byStatus)
    .sort((a, b) => b[1] - a[1])
    .forEach(([s, n]) => lines.push(`  ${s.padEnd(22)} ${n}`));
  lines.push('');
  lines.push(`REVENUE (sum of Sold Price column)`);
  lines.push(`All jobs:       ${fmtMoney(stats.soldAll)}  (margin ${fmtMoney(stats.marginAll)})`);
  lines.push(`Active jobs:    ${fmtMoney(stats.soldActive)}  (margin ${fmtMoney(stats.marginActive)})`);
  lines.push(`Completed jobs: ${fmtMoney(stats.soldCompleted)}  (margin ${fmtMoney(stats.marginCompleted)})`);
  lines.push('');
  lines.push(`OUTSTANDING`);
  lines.push(`Total customer balances owed: ${fmtMoney(stats.outstandingBalance)}`);
  return lines.join('\n');
}

// ─────────────────────────────────────────────────────────────────────────────
// Morning brief — composed in pure JS, no LLM call (deterministic, never fails on AI)
// ─────────────────────────────────────────────────────────────────────────────

async function buildMorningBrief() {
  const jobs = await getJobs(true);
  const upcoming = getUpcomingJobs(jobs, 7);
  const billing3day = getStorageBillingDue(jobs, 3);
  const allBalances = getBalances(jobs);
  const balances2day = allBalances.filter(r => {
    const d = parseDate(get(r, COL.BALANCE_DATE));
    if (!d) return false;
    const today = new Date();
    const t0 = new Date(today.getFullYear(), today.getMonth(), today.getDate());
    const t2 = new Date(t0); t2.setDate(t2.getDate() + 2);
    return d >= t0 && d <= t2;
  });
  const flags = getFlags(jobs);

  const dateStr = new Date().toLocaleDateString('en-US', {
    weekday: 'long', month: 'long', day: 'numeric', year: 'numeric',
    timeZone: BRIEF_TIMEZONE
  });

  const lines = [];
  lines.push(`AVANTI MORNING BRIEF — ${dateStr}`);
  lines.push('='.repeat(54));
  lines.push('');

  const flagSummary = summarizeFlagsForBrief(flags);
  if (flagSummary) {
    lines.push(flagSummary);
    lines.push('');
  }

  if (billing3day.length > 0) {
    lines.push(`STORAGE BILLING — DUE IN NEXT 3 DAYS (${billing3day.length})`);
    billing3day.forEach(e => lines.push('  ' + formatStorageBilling(e)));
    lines.push('');
  }

  if (balances2day.length > 0) {
    lines.push(`CUSTOMER BALANCES — DUE IN NEXT 2 DAYS (${balances2day.length})`);
    balances2day.forEach(r => lines.push('  ' + formatJob(r)));
    lines.push('');
  }

  lines.push(`SERVICE DROPS — NEXT 7 DAYS (${upcoming.length})`);
  if (upcoming.length === 0) {
    lines.push('  None scheduled');
  } else {
    upcoming.forEach(r => lines.push('  ' + formatJob(r)));
  }
  lines.push('');

  lines.push(`OPEN BALANCES — TOTAL ${allBalances.length}`);
  if (allBalances.length === 0) {
    lines.push('  None');
  } else {
    allBalances.forEach(r => lines.push('  ' + formatJob(r)));
  }

  if (billing3day.length === 0 && balances2day.length === 0 && upcoming.length === 0 && allBalances.length === 0) {
    lines.push('');
    lines.push('Nothing on deck. Quiet day.');
  }

  return lines.join('\n');
}

async function fireMorningBrief(reason) {
  console.log(`Firing morning brief (${reason}) to ${BRIEF_CHAT_IDS.length} chat(s)`);
  let brief;
  try {
    brief = await buildMorningBrief();
  } catch (e) {
    console.error('buildMorningBrief failed:', e.message);
    brief = `MORNING BRIEF FAILED: ${e.message}\n\nCheck /debug.`;
  }
  for (const chatId of BRIEF_CHAT_IDS) {
    try {
      await sendTelegram(chatId, brief);
    } catch (e) {
      console.error(`Failed to send brief to ${chatId}:`, e.message);
    }
  }
}

function describeFilters(vendor, state) {
  const parts = [];
  if (vendor) parts.push(`vendor=${vendor}`);
  if (state) parts.push(`state=${state}`);
  return parts.length ? ` [filtered: ${parts.join(', ')}]` : '';
}

// ─────────────────────────────────────────────────────────────────────────────
// Claude
// ─────────────────────────────────────────────────────────────────────────────

async function askClaude(question, context) {
  const today = new Date().toLocaleDateString('en-US', {
    weekday: 'long', year: 'numeric', month: 'long', day: 'numeric'
  });
  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': ANTHROPIC_KEY,
      'anthropic-version': '2023-06-01'
    },
    body: JSON.stringify({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 4000,
      system: `You are Avanti Ops, internal ops assistant for Avanti Freight Solutions. Today: ${today}.

The Data block below has been pre-filtered and pre-formatted by code. Your job is to present it back cleanly. RULES:
- NEVER drop, omit, summarize-away, or "..." any rows. If the Data block lists 22 jobs, your reply lists 22 jobs.
- Reformat for readability (group by date, vendor, status, etc.) when it helps, but include every row.
- Be direct. No preamble, no closing summary. The user already knows the question.
- Use simple plain text — Telegram will render it as-is.

CRITICAL STORAGE BILLING RULE: "Paid in full" only covers the initial 30-day rental period and transportation. It does NOT cover subsequent 30-day cycles. Every active job is subject to recurring monthly storage charges regardless of payment status.`,
      messages: [{ role: 'user', content: `Question: ${question}\n\nData:\n${context}` }]
    })
  });
  const data = await res.json();
  if (data.error) {
    console.error('Claude API error:', JSON.stringify(data.error));
    return `AI error: ${data.error.message}`;
  }
  return data.content?.[0]?.text || 'No response from AI.';
}

// ─────────────────────────────────────────────────────────────────────────────
// Routing
// ─────────────────────────────────────────────────────────────────────────────

function detectIntent(text) {
  const t = text.toLowerCase();
  if (t.match(/storage.*(bill|due|charge|payment|billing)|bill.*storage|who.*storage.*bill|storage.*this week|monthly rental/)) return 'storage_billing';
  if (t.match(/servic|drop.?off|pickup|this week|next \d+ days?|upcoming|getting a container|getting a trailer|being service|initial deliver/)) return 'upcoming';
  if (t.match(/balanc|owed|outstanding|due|unpaid/)) return 'balances';
  if (t.match(/how many|count|total jobs|revenue|stats|scoreboard/)) return 'howmany';
  if (t.match(/flag|anomal|issue|problem|wrong|stale|leak/)) return 'flags';
  if (t.match(/storag|active|open job|all open/)) return 'active';
  if (t.match(/summar|daily|today/)) return 'summary';
  return 'general';
}

function parseQuery(text) {
  const t = text.toLowerCase().trim();
  let command = null;

  // Slash commands take priority
  if (/^\/summary\b/.test(t)) command = 'summary';
  else if (/^\/billing\b/.test(t)) command = 'storage_billing';
  else if (/^\/upcoming\b/.test(t)) command = 'upcoming';
  else if (/^\/balances\b/.test(t)) command = 'balances';
  else if (/^\/active\b/.test(t)) command = 'active';
  else if (/^\/howmany\b/.test(t)) command = 'howmany';
  else if (/^\/flags\b/.test(t)) command = 'flags';
  else command = detectIntent(text);

  const daysMatch = t.match(/(\d+)\s*days?/) || t.match(/\/(?:billing|upcoming|summary)\s+(\d+)\b/);
  const days = daysMatch ? Math.min(365, parseInt(daysMatch[1])) : 7;

  const vendor = extractVendor(text);
  const state = extractState(text);

  return { command, days, vendor, state };
}

// ─────────────────────────────────────────────────────────────────────────────
// Telegram I/O
// ─────────────────────────────────────────────────────────────────────────────

async function sendTelegram(chatId, text) {
  while (text.length > 0) {
    const chunk = text.slice(0, 4000);
    text = text.slice(4000);
    await fetch(`https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId, text: chunk })
    });
  }
}

async function buildDebugReport() {
  let pullError = null;
  try {
    await getJobs(true);
  } catch (e) {
    pullError = e.message;
  }
  const lines = [];
  lines.push('=== AVANTI OPS BOT — DEBUG ===');
  lines.push('');
  lines.push(`Booted at:        ${fmtTimestamp(diag.bootedAt)}`);
  lines.push(`Now:              ${fmtTimestamp(new Date())}`);
  lines.push('');
  lines.push('--- ENV ---');
  lines.push(`TELEGRAM_TOKEN:   ${TELEGRAM_TOKEN ? 'set (env)' : 'MISSING'}`);
  lines.push(`ANTHROPIC_API_KEY:${ANTHROPIC_KEY ? ' set' : ' MISSING'}`);
  lines.push(`GOOGLE_API_KEY:   ${GOOGLE_KEY ? 'set' : 'MISSING'}`);
  lines.push(`APPS_SCRIPT_URL:  ${APPS_SCRIPT_URL ? 'set' : 'MISSING'}`);
  lines.push(`APPS_SCRIPT_SECRET:${APPS_SCRIPT_SECRET ? ' set' : ' MISSING'}`);
  lines.push(`Writes enabled:   ${writesEnabled ? 'YES' : 'no (read-only)'}`);
  lines.push(`Allowed chat IDs: ${ALLOWED_CHAT_IDS.length} (${ALLOWED_CHAT_IDS.join(', ')})`);
  lines.push('');
  lines.push('--- SHEET ---');
  lines.push(`Sheet ID:         ${SHEET_ID}`);
  lines.push(`Sheet tab:        ${SHEET_NAME}`);
  lines.push(`Last pull:        ${fmtTimestamp(diag.lastSheetPull)}`);
  lines.push(`Last HTTP status: ${diag.lastHttpStatus ?? 'n/a'}`);
  lines.push(`Last status:      ${diag.lastSheetStatus}`);
  if (diag.lastSheetError) lines.push(`Last error:       ${diag.lastSheetError}`);
  if (pullError) lines.push(`This pull error:  ${pullError}`);
  lines.push('');
  lines.push('--- LOAD ---');
  lines.push(`Total rows from Sheets API: ${diag.totalRows ?? 'n/a'}`);
  lines.push(`Header row index detected:  ${diag.headerIdx ?? 'n/a'}  (0-based; sheet row 4 = index 3)`);
  lines.push(`Container rows loaded:      ${diag.jobsLoaded ?? 'n/a'}`);
  lines.push(`Multi-container jobs:       ${diag.multiContainerGroups.length}${diag.multiContainerGroups.length ? '\n  ' + diag.multiContainerGroups.join('\n  ') : ''}`);
  lines.push(`Cache hits / misses:        ${diag.cacheHits} / ${diag.cacheMisses}`);
  lines.push('');
  lines.push('--- FIRST 12 ROWS, COLUMN A ---');
  diag.firstColAValues.forEach(line => lines.push('  ' + line));
  lines.push('');
  lines.push('--- DETECTED HEADER ROW ---');
  diag.rawHeaders.slice(0, 24).forEach(line => lines.push('  ' + line));
  return lines.join('\n');
}

function isAuthorized(chatId) {
  return ALLOWED_CHAT_IDS.includes(String(chatId));
}

// ─────────────────────────────────────────────────────────────────────────────
// Webhook
// ─────────────────────────────────────────────────────────────────────────────

app.post('/webhook', async (req, res) => {
  res.sendStatus(200);
  try {
    const msg = req.body?.message;
    if (!msg?.text) return;
    const chatId = msg.chat.id;
    const text = msg.text.trim();

    // /myid is always available so future authorized users can self-identify.
    if (text === '/myid') {
      const username = msg.from?.username ? '@' + msg.from.username : 'n/a';
      const name = `${msg.from?.first_name || ''} ${msg.from?.last_name || ''}`.trim() || 'n/a';
      await sendTelegram(chatId,
        `Telegram chat ID: ${chatId}\n` +
        `Username:         ${username}\n` +
        `Name:             ${name}\n\n` +
        `Forward this chat ID to the bot administrator to request access.`
      );
      return;
    }

    if (!isAuthorized(chatId)) {
      console.warn(`Unauthorized access attempt from chat ${chatId} (${msg.from?.username || 'no username'})`);
      await sendTelegram(chatId, 'Not authorized. Send /myid and forward the result to the administrator.');
      return;
    }

    if (text === '/start' || text === '/help') {
      await sendTelegram(chatId,
        `Avanti Ops is online.\n\n` +
        `SLASH COMMANDS\n` +
        `/summary         — daily ops brief\n` +
        `/billing [N]     — storage billing due in next N days (default 7)\n` +
        `/upcoming [N]    — initial services in next N days (default 7)\n` +
        `/balances        — open customer balances\n` +
        `/active          — all active (billable) jobs\n` +
        `/howmany         — counts, revenue, margin scoreboard\n` +
        `/flags           — revenue-protection anomalies (cancelled w/ balance, status mismatches, etc.)\n` +
        `/lookup <JOB#>   — full detail on one job\n` +
        `/setstatus <JOB#> [vo:VO#] <status>  — change column G\n` +
        `/addnote <JOB#> [vo:VO#] <text>      — append timestamped note to column W\n` +
        `/zerobalance <JOB#> [vo:VO#]         — clear balance + bill date\n` +
        `/testbrief       — fire the 8AM morning brief now (test)\n` +
        `/debug           — sheet diagnostics\n` +
        `/myid            — your Telegram chat ID\n\n` +
        `Multi-container jobs: add vo:VENDOR_ORDER# to disambiguate.\n\n` +
        `AUTO BRIEF\n` +
        `Daily ${BRIEF_CRON} (${BRIEF_TIMEZONE}) → ${BRIEF_CHAT_IDS.length} chat(s)\n\n` +
        `FILTERS — add a vendor or state to any query\n` +
        `  /billing PODS        — only PODS billing this week\n` +
        `  /upcoming Florida    — only FL service drops this week\n` +
        `  who needs Estes billing in 14 days\n` +
        `  any active jobs in TX\n\n` +
        `Or just ask in plain English.`
      );
      return;
    }

    if (text === '/debug') {
      await sendTelegram(chatId, 'Running diagnostics...');
      const report = await buildDebugReport();
      await sendTelegram(chatId, report);
      return;
    }

    if (text === '/testbrief') {
      await sendTelegram(chatId, 'Building morning brief...');
      await fireMorningBrief('manual /testbrief');
      return;
    }

    // ───── Read & write commands targeting a specific job ─────
    const lookupMatch = text.match(/^\/lookup\s+(.+)$/i);
    if (lookupMatch) {
      await sendTelegram(chatId, 'Looking up...');
      const jobs = await getJobs(true);
      const upper = lookupMatch[1].trim().toUpperCase();
      const matches = jobs.filter(r => get(r, COL.JOB).toUpperCase() === upper);
      if (matches.length === 0) {
        await sendTelegram(chatId, `Job ${lookupMatch[1].trim()} not found.`);
      } else {
        const blocks = matches.map((r, i) =>
          `=== Container ${i + 1} of ${matches.length} ===\n` + formatRowFull(r)
        );
        await sendTelegram(chatId, blocks.join('\n\n'));
      }
      return;
    }

    const setStatusMatch = text.match(/^\/setstatus\s+(.+)$/i);
    if (setStatusMatch) {
      const { jobId, vendorOrder, rest, error: parseErr } = parseWriteArgs(setStatusMatch[1]);
      if (parseErr) { await sendTelegram(chatId, parseErr); return; }
      if (!rest) { await sendTelegram(chatId, `Usage: /setstatus <JOB#> [vo:VENDOR_ORDER#] <new status>`); return; }
      const jobs = await getJobs(true);
      const lookup = findJobRow(jobs, jobId, vendorOrder);
      if (lookup.error) { await sendTelegram(chatId, lookup.error); return; }
      const before = get(lookup.row, COL.STATUS);
      try {
        await updateCell(lookup.row._sheetRow, COL.STATUS, rest);
        await sendTelegram(chatId,
          `Updated ${jobId} (sheet row ${lookup.row._sheetRow})\n` +
          `Status: "${before}" → "${rest}"`
        );
      } catch (e) {
        await sendTelegram(chatId, `Update failed: ${e.message}`);
      }
      return;
    }

    const addNoteMatch = text.match(/^\/addnote\s+(.+)$/i);
    if (addNoteMatch) {
      const { jobId, vendorOrder, rest, error: parseErr } = parseWriteArgs(addNoteMatch[1]);
      if (parseErr) { await sendTelegram(chatId, parseErr); return; }
      if (!rest) { await sendTelegram(chatId, `Usage: /addnote <JOB#> [vo:VENDOR_ORDER#] <note text>`); return; }
      const jobs = await getJobs(true);
      const lookup = findJobRow(jobs, jobId, vendorOrder);
      if (lookup.error) { await sendTelegram(chatId, lookup.error); return; }
      const stamp = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: '2-digit', timeZone: BRIEF_TIMEZONE });
      const existing = get(lookup.row, COL.NOTES);
      const newNote = existing
        ? `${existing} | [${stamp}] ${rest}`
        : `[${stamp}] ${rest}`;
      try {
        await updateCell(lookup.row._sheetRow, COL.NOTES, newNote);
        await sendTelegram(chatId,
          `Note added to ${jobId} (sheet row ${lookup.row._sheetRow})\n\n` +
          `BEFORE: ${existing || '(empty)'}\n\n` +
          `AFTER:  ${newNote}`
        );
      } catch (e) {
        await sendTelegram(chatId, `Update failed: ${e.message}`);
      }
      return;
    }

    const zeroBalanceMatch = text.match(/^\/zerobalance\s+(.+)$/i);
    if (zeroBalanceMatch) {
      const { jobId, vendorOrder, error: parseErr } = parseWriteArgs(zeroBalanceMatch[1]);
      if (parseErr) { await sendTelegram(chatId, parseErr); return; }
      const jobs = await getJobs(true);
      const lookup = findJobRow(jobs, jobId, vendorOrder);
      if (lookup.error) { await sendTelegram(chatId, lookup.error); return; }
      const beforeBal = get(lookup.row, COL.BALANCE);
      const beforeDate = get(lookup.row, COL.BALANCE_DATE);
      try {
        await updateCells(lookup.row._sheetRow, { [COL.BALANCE]: '', [COL.BALANCE_DATE]: '' });
        await sendTelegram(chatId,
          `Cleared balance on ${jobId} (sheet row ${lookup.row._sheetRow})\n` +
          `Balance:  "${beforeBal}" → ""\n` +
          `Bill date: "${beforeDate}" → ""`
        );
      } catch (e) {
        await sendTelegram(chatId, `Update failed: ${e.message}`);
      }
      return;
    }

    await sendTelegram(chatId, 'Pulling sheet data...');
    const allJobs = await getJobs();
    const { command, days, vendor, state } = parseQuery(text);

    // Apply optional vendor + state filters before running the base query
    const jobs = applyStateFilter(applyVendorFilter(allJobs, vendor), state);
    const filterTag = describeFilters(vendor, state);

    let context = '';
    let bypassClaude = false;
    let rawAnswer = null;

    if (command === 'upcoming') {
      const upcoming = getUpcomingJobs(jobs, days);
      const todayStr = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
      const endDate = new Date(); endDate.setDate(endDate.getDate() + days);
      const endStr = endDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
      context = upcoming.length > 0
        ? `${upcoming.length} jobs with Initial Service Date from ${todayStr} to ${endStr}${filterTag}:\n\n${upcoming.map(formatJob).join('\n')}`
        : `No jobs found with Initial Service Date between ${todayStr} and ${endStr}${filterTag}.`;

    } else if (command === 'storage_billing') {
      const due = getStorageBillingDue(jobs, days);
      const todayStr = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
      const endDate = new Date(); endDate.setDate(endDate.getDate() + days);
      const endStr = endDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
      context = due.length > 0
        ? `${due.length} jobs with storage billing due between ${todayStr} and ${endStr}${filterTag}:\n\n${due.map(formatStorageBilling).join('\n')}`
        : `No storage billing due between ${todayStr} and ${endStr}${filterTag}.`;

    } else if (command === 'balances') {
      const bal = getBalances(jobs);
      context = bal.length > 0
        ? `${bal.length} jobs with outstanding balances${filterTag}:\n\n${bal.map(formatJob).join('\n')}`
        : `No outstanding balances${filterTag}.`;

    } else if (command === 'active') {
      const active = getActiveJobs(jobs);
      context = active.length > 0
        ? `${active.length} active jobs (excludes Completed, Cancelled, Future Order)${filterTag}:\n\n${active.map(formatJob).join('\n')}`
        : `No active jobs found${filterTag}.`;

    } else if (command === 'howmany') {
      // Scoreboard is purely numeric — bypass Claude, send the raw report.
      const stats = getHowMany(jobs);
      rawAnswer = `=== AVANTI SCOREBOARD${filterTag} ===\n\n` + formatHowMany(stats, jobs);
      bypassClaude = true;

    } else if (command === 'flags') {
      // Anomaly detection — purely deterministic, bypass Claude.
      const flags = getFlags(jobs);
      rawAnswer = formatFlags(flags);
      bypassClaude = true;

    } else if (command === 'summary') {
      const upcoming = getUpcomingJobs(jobs, 7);
      const balances = getBalances(jobs);
      const storageBilling = getStorageBillingDue(jobs, 7);
      const active = getActiveJobs(jobs);
      context = `DAILY SUMMARY${filterTag}\n\nUpcoming service (next 7 days): ${upcoming.length}\n${upcoming.map(formatJob).join('\n')}\n\nStorage billing due (next 7 days): ${storageBilling.length}\n${storageBilling.map(formatStorageBilling).join('\n')}\n\nOutstanding balances: ${balances.length}\n${balances.map(formatJob).join('\n')}\n\nAll active jobs (excludes Completed/Cancelled/Future Order): ${active.length}\n${active.map(formatJob).join('\n')}`;

    } else {
      context = `All ${jobs.length} container rows${filterTag}:\n\n${jobs.map(formatJob).join('\n')}`;
    }

    if (bypassClaude) {
      await sendTelegram(chatId, rawAnswer);
    } else {
      const answer = await askClaude(text, context);
      await sendTelegram(chatId, answer);
    }

  } catch (e) {
    console.error('Webhook error:', e.message);
    try {
      const chatId = req.body?.message?.chat?.id;
      if (chatId && isAuthorized(chatId)) await sendTelegram(chatId, `Error: ${e.message}`);
    } catch {}
  }
});

app.get('/', (req, res) => res.send('Avanti Ops bot is running.'));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  // Schedule the morning brief. Validate the cron pattern up front so a typo fails loud.
  if (!cron.validate(BRIEF_CRON)) {
    console.error(`FATAL: BRIEF_CRON "${BRIEF_CRON}" is not a valid cron expression. Bot is up but morning brief is disabled.`);
  } else {
    cron.schedule(BRIEF_CRON, () => fireMorningBrief(`scheduled cron ${BRIEF_CRON} ${BRIEF_TIMEZONE}`), { timezone: BRIEF_TIMEZONE });
    console.log(`Morning brief scheduled: cron="${BRIEF_CRON}" tz="${BRIEF_TIMEZONE}" → chats=[${BRIEF_CHAT_IDS.join(', ')}]`);
  }
  // Verify Apps Script proxy is reachable so any auth/URL issues fail loud at boot, not on first write.
  if (writesEnabled) {
    callAppsScript('ping', {})
      .then(d => console.log(`Apps Script ping OK: sheet=${d.sheet} time=${d.time}`))
      .catch(e => console.error(`Apps Script ping FAILED: ${e.message}`));
  }
});
