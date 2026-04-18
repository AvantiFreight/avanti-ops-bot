const express = require('express');
const fetch = require('node-fetch');

const app = express();
app.use(express.json());

const TELEGRAM_TOKEN = '8738052440:AAFTYZmBLYQa6k8AB7dKrZ1bOscheghlrdI';
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
const GOOGLE_KEY = process.env.GOOGLE_API_KEY;
const SHEET_ID = '1fDdMfqzat1XGbbc9G3bbI1x9QN-gFM3Udc-nUAtT3QM';
const SHEET_NAME = 'Booked Jobs';

async function getSheetData() {
  const range = encodeURIComponent(SHEET_NAME + '!A:W');
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${SHEET_ID}/values/${range}?key=${GOOGLE_KEY}`;
  const res = await fetch(url);
  const data = await res.json();
  if (!data.values) throw new Error('No sheet data');
  return data.values.map(r => r.join('\t')).join('\n');
}

async function askClaude(question, sheetData) {
  const today = new Date().toLocaleDateString('en-US', {
    weekday: 'long', year: 'numeric', month: 'long', day: 'numeric'
  });

  const systemPrompt = `You are Avanti Ops, an internal operations assistant for Avanti Freight Solutions — a self-service moving brokerage. Answer questions about active jobs based on live Google Sheet data provided to you.

Today's date: ${today}

Business model: Sources vendor capacity (PODS, Estes, 1800PackRat, etc.), marks up pricing, sells retail. Revenue = margin between vendor cost and customer sell price. Payment: 100% upfront OR 50/50 deposit/balance.

Services:
- Container moves: PODS (primary), Pack-Rat/Zippy Shell, UNITS
- Drop-trailer/self-load: Estes SureMove (primary), Old Dominion

Sheet columns (tab-separated): Job #, Vendor Order #, Phone, Email, Booked Date, Initial Service Date, Order Status, From State, To State, Service Provider, Total Vendor Cost, Vendor Storage, Avanti Storage, Sold Price (Avanti), Avanti Margin, Avanti Margin (%), Deposit Amount, Deposit Collected Date, Payment Method, Remaining Balance, Remaining Balance Bill Date, Remaining Balance Payment Method, Notes

CRITICAL QUERY RULES — always follow these exactly:

UPCOMING SERVICE / DROP-OFFS / WHO IS GETTING SERVICED:
When asked who is getting serviced, who has a drop-off, upcoming jobs, next 7 days, this week, or any variation:
1. Scan EVERY row in the data from top to bottom — do not stop early
2. Filter ONLY by Initial Service Date — use no other column as a filter
3. Default window: today through today + 7 days unless user specifies otherwise
4. EXCLUDE any job where Initial Service Date is before today, regardless of Order Status
5. INCLUDE all job types — moves AND onsite storage both count
6. Return: Job #, Phone, Origin to Destination, Vendor, Initial Service Date, Sell Price, Balance Due, Order Status
7. Sort ascending by Initial Service Date

REMAINING BALANCES:
Return all jobs where Remaining Balance > $0. Include bill date and job #.

STORAGE JOBS:
Return all jobs where Order Status contains Storage. Include vendor storage cost and Avanti storage charge.

DAILY SUMMARY:
1. Jobs with Initial Service Date = today
2. Jobs with Initial Service Date within next 7 days
3. Outstanding balances (Remaining Balance > $0)
4. Future orders not yet serviced

GENERAL RULES:
- Be concise and direct. No fluff. Internal tool only.
- Format as clean lists for multiple jobs.
- Keep responses under 300 words — this is Telegram.
- Never filter by Order Status unless explicitly asked to.
- Always scan every single row — never stop early.`;

  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': ANTHROPIC_KEY,
      'anthropic-version': '2023-06-01'
    },
    body: JSON.stringify({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 1000,
      system: systemPrompt,
      messages: [
        {
          role: 'user',
          content: `Here is the current job data from the Google Sheet:\n\n${sheetData}\n\n---\n\nQuestion: ${question}`
        }
      ]
    })
  });

  const data = await res.json();
  if (data.error) {
    console.error('Claude API error:', JSON.stringify(data.error));
    return `AI error: ${data.error.message}`;
  }
  return data.content?.[0]?.text || 'No response from AI.';
}

async function sendTelegram(chatId, text) {
  await fetch(`https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ chat_id: chatId, text, parse_mode: 'Markdown' })
  });
}

app.post('/webhook', async (req, res) => {
  res.sendStatus(200);
  try {
    const msg = req.body?.message;
    if (!msg?.text) return;

    const chatId = msg.chat.id;
    const text = msg.text.trim();

    if (text === '/start') {
      await sendTelegram(chatId, `*Avanti Ops* is online.\n\nAsk me anything about your jobs:\n- Who is getting serviced this week?\n- Who has outstanding balances?\n- Who needs storage billing?\n- Give me a daily summary`);
      return;
    }

    await sendTelegram(chatId, 'Pulling sheet data...');
    const sheetData = await getSheetData();
    const answer = await askClaude(text, sheetData);
    await sendTelegram(chatId, answer);

  } catch (e) {
    console.error(e);
  }
});

app.get('/', (req, res) => res.send('Avanti Ops bot is running.'));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
