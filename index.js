const express = require('express');
const fetch = require('node-fetch');

const app = express();
app.use(express.json());

const TELEGRAM_TOKEN = '8738052440:AAFTYZmBLYQa6k8AB7dKrZ1bOscheghlrdI';
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
const GOOGLE_KEY = process.env.GOOGLE_API_KEY;
const SHEET_ID = '1fDdMfqzat1XGbbc9G3bbI1x9QN-gFM3Udc-nUAtT3QM';
const SHEET_NAME = 'Booked Jobs';

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
  return parseFloat(str.replace(/[$,\s]/g, '')) || 0;
}

async function getJobs() {
  const range = encodeURIComponent(SHEET_NAME + '!A:W');
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${SHEET_ID}/values/${range}?key=${GOOGLE_KEY}`;
  const res = await fetch(url);
  const data = await res.json();
  if (!data.values) throw new Error('No sheet data');

  const rows = data.values;

  // Find header row — look for any row where col A is 'Job #'
  let headerIdx = -1;
  for (let i = 0; i < rows.length; i++) {
    if (rows[i][0] && rows[i][0].toString().trim() === 'Job #') {
      headerIdx = i;
      break;
    }
  }
  if (headerIdx === -1) throw new Error('Could not find header row with "Job #"');

  const headers = rows[headerIdx].map(h => (h || '').trim());
  console.log('Headers found:', headers.join(' | '));

  const jobs = [];
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    if (!r[0] || !r[0].toString().trim().startsWith('A')) continue;
    const obj = {};
    headers.forEach((h, idx) => { obj[h] = (r[idx] || '').trim(); });
    jobs.push(obj);
  }

  console.log(`Loaded ${jobs.length} jobs`);
  return { jobs, headers };
}

function getSvcDate(job, headers) {
  // Try exact match first, then partial match for "Initial Service"
  const candidates = ['Initial Service', 'Initial Service Date', 'Service Date'];
  for (const c of candidates) {
    if (job[c] !== undefined) return job[c];
  }
  // fallback: find any header containing "service" (case-insensitive)
  const key = headers.find(h => h.toLowerCase().includes('service') && h.toLowerCase().includes('initial'));
  return key ? job[key] : '';
}

function getUpcomingJobs(jobs, headers, days) {
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const end = new Date(today);
  end.setDate(end.getDate() + days);

  return jobs.filter(j => {
    const raw = getSvcDate(j, headers);
    const d = parseDate(raw);
    if (!d) return false;
    return d >= today && d <= end;
  }).sort((a, b) => {
    return parseDate(getSvcDate(a, headers)) - parseDate(getSvcDate(b, headers));
  });
}

function getBalances(jobs) {
  return jobs.filter(j => stripDollar(j['Remaining Balance']) > 0);
}

function getStorageJobs(jobs) {
  return jobs.filter(j => (j['Order Status'] || '').toLowerCase().includes('storage'));
}

function formatJob(j, headers) {
  const bal = stripDollar(j['Remaining Balance']);
  const balStr = bal > 0 ? ` | $${bal.toLocaleString()} due ${j['Remaining Balance Bill Date']}` : ' | Paid in full';
  const svcDate = getSvcDate(j, headers) || 'TBD';
  const phone = j[''] || '';
  const col3 = headers[2] ? j[headers[2]] : '';
  return `${j['Job #']} | ${col3} | ${j['From State']}→${j['To State']} | ${j['Service Provider']} | Svc: ${svcDate}${balStr} | ${j['Order Status']}`;
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
      system: `You are Avanti Ops, internal assistant for Avanti Freight Solutions. Today: ${today}. Be concise, direct, no fluff. Format as clean lists. Under 250 words.`,
      messages: [{
        role: 'user',
        content: `Question: ${question}\n\nData:\n${context}`
      }]
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
    const { jobs, headers } = await getJobs();
    const intent = detectIntent(text);
    let context = '';

    if (intent === 'upcoming') {
      const daysMatch = text.match(/(\d+)\s*days?/);
      const days = daysMatch ? parseInt(daysMatch[1]) : 7;
      const upcoming = getUpcomingJobs(jobs, headers, days);
      const todayStr = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
      const endDate = new Date(); endDate.setDate(endDate.getDate() + days);
      const endStr = endDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
      context = upcoming.length > 0
        ? `${upcoming.length} jobs with Initial Service Date from ${todayStr} to ${endStr}:\n${upcoming.map(j => formatJob(j, headers)).join('\n')}`
        : `No jobs found with Initial Service Date between ${todayStr} and ${endStr}.`;
    } else if (intent === 'balances') {
      const bal = getBalances(jobs);
      context = bal.length > 0
        ? `${bal.length} jobs with outstanding balances:\n${bal.map(j => formatJob(j, headers)).join('\n')}`
        : 'No outstanding balances.';
    } else if (intent === 'storage') {
      const storage = getStorageJobs(jobs);
      context = storage.length > 0
        ? `${storage.length} storage jobs:\n${storage.map(j => formatJob(j, headers)).join('\n')}`
        : 'No storage jobs found.';
    } else if (intent === 'summary') {
      const upcoming = getUpcomingJobs(jobs, headers, 7);
      const balances = getBalances(jobs);
      const storage = getStorageJobs(jobs);
      context = `SUMMARY\nUpcoming (7 days): ${upcoming.length} jobs\n${upcoming.map(j => formatJob(j, headers)).join('\n')}\n\nOutstanding balances: ${balances.length}\n${balances.map(j => formatJob(j, headers)).join('\n')}\n\nIn storage: ${storage.length}\n${storage.map(j => formatJob(j, headers)).join('\n')}`;
    } else {
      context = `All jobs (${jobs.length} total):\n${jobs.map(j => formatJob(j, headers)).join('\n')}`;
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
