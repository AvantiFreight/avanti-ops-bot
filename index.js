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

  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': ANTHROPIC_KEY,
      'anthropic-version': '2023-06-01'
    },
    body: JSON.stringify({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1000,
      system: `You are Avanti Ops, an internal operations assistant for Avanti Freight Solutions — a self-service moving brokerage. Answer questions about active jobs based on live Google Sheet data.

Today's date: ${today}

Business model: Sources vendor capacity (PODS, Estes, 1800PackRat, etc.), marks up pricing, sells retail. Revenue = margin between vendor cost and customer sell price. Payment: 100% upfront OR 50/50 deposit/balance.

Services:
- Container moves: PODS (primary), Pack-Rat/Zippy Shell, UNITS
- Drop-trailer/self-load: Estes SureMove (primary), Old Dominion

Columns: Job #, Vendor Order #, Booked Date, Service Date, Status, From State, To State, Service Provider, Total Vendor Cost, Vendor Storage, Avanti Storage, Sold Price (Avanti), Avanti Margin, Avanti Margin (%), Deposit Amount, Deposit Collected Date, Payment Method, Remaining Balance, Remaining Balance Bill Date, Remaining Balance Payment Method, Notes

Rules:
- Be concise and direct. No fluff. This is an internal tool.
- Format as clean lists when summarizing multiple jobs.
- For balances due: flag any job where Remaining Balance > $0 and include the bill date.
- For storage: flag jobs with In Storage or Onsite Storage status.
- For daily summary: (1) moves today/this week, (2) storage billing due, (3) outstanding balances, (4) upcoming future orders.
- Keep responses under 300 words — this is Telegram, not a report.`,
      messages: [
        {
          role: 'user',
          content: `Here is the current job data from the Google Sheet:\n\n${sheetData}\n\n---\n\nQuestion: ${question}`
        }
      ]
    })
  });

  const data = await res.json();
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
      await sendTelegram(chatId, `*Avanti Ops* is online.\n\nAsk me anything about your jobs:\n• Who has outstanding balances?\n• Who needs storage billing?\n• Give me a daily summary\n• What moves are coming up?`);
      return;
    }

    await sendTelegram(chatId, '⏳ Pulling sheet data...');
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
