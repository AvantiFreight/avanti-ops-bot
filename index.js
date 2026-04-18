const express = require('express');
const fetch = require('node-fetch');

const app = express();
app.use(express.json());

const TELEGRAM_TOKEN = '8738052440:AAFTYZmBLYQa6k8AB7dKrZ1bOscheghlrdI';
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
const GOOGLE_KEY = process.env.GOOGLE_API_KEY;
const SHEET_ID = '1fDdMfqzat1XGbbc9G3bbI1x9QN-gFM3Udc-nUAtT3QM';
const SHEET_NAME = 'Booked Jobs';

// Column indices (0-based) — matches your sheet exactly
// A=0: Job #, B=1: Vendor Order #, C=2: Phone, D=3: Email
// E=4: Booked Date, F=5: Initial Service, G=6: Order Status
// H=7: From State, I=8: To State, J=9: Service Provider
// K=10: Total Vendor Cost, L=11: Vendor Storage, M=12: Avanti Storage
// N=13: Sold Price, O=14: Avanti Margin, P=15: Avanti Margin %
// Q=16: Deposit Amount, R=17: Deposit Collected, S=18: Payment Method
// T=19: Remaining Balance, U=20: Remaining Balance Bill Date
// V=21: Remaining Balance Payment Method, W=22: Notes
const COL = {
  JOB: 0,
  VENDOR_ORDER: 1,
  PHONE: 2,
  EMAIL: 3,
  BOOKED_DATE: 4,
  INITIAL_SERVICE: 5,
  STATUS: 6,
  FROM_STATE: 7,
  TO_STATE: 8,
  PROVIDER: 9,
  VENDOR_COST: 10,
  VENDOR_STORAGE: 11,
  AVANTI_STORAGE: 12,
  SELL_PRICE: 13,
  MARGIN: 14,
  MARGIN_PCT: 15,
  DEPOSIT_AMT: 16,
  DEPOSIT_DATE: 17,
  PAYMENT_METHOD: 18,
  BALANCE: 19,
  BALANCE_DATE: 20,
  BALANCE_PAYMENT: 21,
  NOTES: 22
};

function parseDate(str) {
  if (!str || str.trim() === '') return null;
  str = str.trim();
  const year = new Date().getFullYear();
  const md = str.match(/^(\d{1,2})\/(\d{1,2})(?:\/\d{2,4})?$/);
  if (md) return new Date(year, parseInt(md[1]) - 1, parseInt(md[2]));
  const full = new Date(str);
  if (!isNaN(full)) return full;
  return null;
}

function stripDollar(str) {
  if (!str) return 0;
  return parseFloat(str.toString().replace(/[$,\s]/g, '')) || 0;
}

function get(row, col) {
  return (row[col] || '').toString().trim();
}

async function getJobs() {
  const range = encodeURIComponent(SHEET_NAME + '!A:W');
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${SHEET_ID}/values/${range}?key=${GOOGLE_KEY}`;
  const res = await fetch(url);
  const data = await res.json();
  if (!data.values) throw new Error('No sheet data');

  const rows = data.values;

  // Find header row by looking for 'Job #' in col A
  let headerIdx = -1;
  for (let i = 0; i < rows.length; i++) {
    const cell = (rows[i][0] || '').toString().trim();
    if (cell === 'Job #') { headerIdx = i; break; }
  }
  if (headerIdx === -1) throw new Error('Header row not found');

  // Log actual headers so we can debug column mismatches
  const rawHeaders = rows[headerIdx];
  console.log('Header row index:', headerIdx);
  rawHeaders.forEach((h, i) => {
    const clean = (h || '').toString().replace(/\n/g, '\\n').trim();
    console.log(`  Col ${i} (${String.fromCharCode(65+i)}): "${clean}"`);
  });

  const jobs = [];
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    const jobId = get(r, COL.JOB);
    if (!jobId || !jobId.startsWith('A')) continue;
    jobs.push(r);
  }

  console.log(`Loaded ${jobs.length} jobs`);
  // Log first few initial service dates for debugging
  jobs.slice(0, 5).forEach(j => {
    console.log(`  ${get(j, COL.JOB)} | svc: "${get(j, COL.INITIAL_SERVICE)}" | status: "${get(j, COL.STATUS)}"`);
  });

  return jobs;
}

function getUpcomingJobs(jobs, days) {
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const end = new Date(today);
  end.setDate(end.getDate() + days);

  const result = jobs.filter(r => {
    const raw = get(r, COL.INITIAL_SERVICE);
    const d = parseDate(raw);
    if (!d) return false;
    return d >= today && d <= end;
  });

  result.sort((a, b) => {
    return parseDate(get(a, COL.INITIAL_SERVICE)) - parseDate(get(b, COL.INITIAL_SERVICE));
  });

  return result;
}

function getBalances(jobs) {
  return jobs.filter(r => stripDollar(get(r, COL.BALANCE)) > 0);
}

function getStorageJobs(jobs) {
  return jobs.filter(r => get(r, COL.STATUS).toLowerCase().includes('storage'));
}

function formatJob(r) {
  const bal = stripDollar(get(r, COL.BALANCE));
  const balStr = bal > 0
    ? `$${bal.toLocaleString()} due ${get(r, COL.BALANCE_DATE)}`
    : 'Paid in full';
  return [
    get(r, COL.JOB),
    get(r, COL.PHONE),
    `${get(r, COL.FROM_STATE)}→${get(r, COL.TO_STATE)}`,
    get(r, COL.PROVIDER),
    `Svc: ${get(r, COL.INITIAL_SERVICE)}`,
    `Sell: $${stripDollar(get(r, COL.SELL_PRICE)).toLocaleString()}`,
    balStr,
    get(r, COL.STATUS)
  ].join(' | ');
}

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
      max_tokens: 800,
      system: `You are Avanti Ops, internal ops assistant for Avanti Freight Solutions. Today: ${today}. Be concise, direct, no fluff. Format as clean lists. Under 250 words.`,
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

function detectIntent(text) {
  const t = text.toLowerCase();
  if (t.match(/servic|drop.?off|pickup|this week|next \d+ days?|upcoming|getting a container|getting a trailer|being service/)) return 'upcoming';
  if (t.match(/balanc|owed|outstanding|due|unpaid/)) return 'balances';
  if (t.match(/storag/)) return 'storage';
  if (t.match(/summar|daily|today/)) return 'summary';
  return 'general';
}

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

app.post('/webhook', async (req, res) => {
  res.sendStatus(200);
  try {
    const msg = req.body?.message;
    if (!msg?.text) return;
    const chatId = msg.chat.id;
    const text = msg.text.trim();

    if (text === '/start') {
      await sendTelegram(chatId, `Avanti Ops is online.\n\nTry:\n- Who is getting serviced this week?\n- Who has outstanding balances?\n- Who is in storage?\n- Daily summary`);
      return;
    }

    await sendTelegram(chatId, 'Pulling sheet data...');
    const jobs = await getJobs();
    const intent = detectIntent(text);
    let context = '';

    if (intent === 'upcoming') {
      const daysMatch = text.match(/(\d+)\s*days?/);
      const days = daysMatch ? parseInt(daysMatch[1]) : 7;
      const upcoming = getUpcomingJobs(jobs, days);
      const todayStr = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
      const endDate = new Date(); endDate.setDate(endDate.getDate() + days);
      const endStr = endDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
      context = upcoming.length > 0
        ? `${upcoming.length} jobs with Initial Service Date from ${todayStr} to ${endStr}:\n\n${upcoming.map(formatJob).join('\n')}`
        : `No jobs found with Initial Service Date between ${todayStr} and ${endStr}.`;
    } else if (intent === 'balances') {
      const bal = getBalances(jobs);
      context = bal.length > 0
        ? `${bal.length} jobs with outstanding balances:\n\n${bal.map(formatJob).join('\n')}`
        : 'No outstanding balances.';
    } else if (intent === 'storage') {
      const storage = getStorageJobs(jobs);
      context = storage.length > 0
        ? `${storage.length} storage jobs:\n\n${storage.map(formatJob).join('\n')}`
        : 'No storage jobs found.';
    } else if (intent === 'summary') {
      const upcoming = getUpcomingJobs(jobs, 7);
      const balances = getBalances(jobs);
      const storage = getStorageJobs(jobs);
      context = `SUMMARY\n\nUpcoming (7 days): ${upcoming.length}\n${upcoming.map(formatJob).join('\n')}\n\nOutstanding balances: ${balances.length}\n${balances.map(formatJob).join('\n')}\n\nIn storage: ${storage.length}\n${storage.map(formatJob).join('\n')}`;
    } else {
      context = `All ${jobs.length} jobs:\n\n${jobs.map(formatJob).join('\n')}`;
    }

    const answer = await askClaude(text, context);
    await sendTelegram(chatId, answer);

  } catch (e) {
    console.error('Webhook error:', e.message);
    try {
      const chatId = req.body?.message?.chat?.id;
      if (chatId) await sendTelegram(chatId, `Error: ${e.message}`);
    } catch {}
  }
});

app.get('/', (req, res) => res.send('Avanti Ops bot is running.'));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
