/**
 * Agencia Paco AI — Agente Telefónico
 * Servidor Node.js con Twilio ConversationRelay + Claude API
 *
 * Env vars en Railway:
 *   TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER
 *   ANTHROPIC_API_KEY, SERVER_URL, PORT
 */

'use strict';

const express    = require('express');
const http       = require('http');
const WebSocket  = require('ws');
const twilio     = require('twilio');
const Anthropic  = require('@anthropic-ai/sdk');
const cors       = require('cors');

const TWILIO_ACCOUNT_SID  = process.env.TWILIO_ACCOUNT_SID  || '';
const TWILIO_AUTH_TOKEN   = process.env.TWILIO_AUTH_TOKEN   || '';
const TWILIO_PHONE_NUMBER = process.env.TWILIO_PHONE_NUMBER || '';
const ANTHROPIC_API_KEY   = process.env.ANTHROPIC_API_KEY   || '';
const SERVER_URL          = (process.env.SERVER_URL || 'http://localhost:3000').replace(/\/$/, '');
const PORT                = parseInt(process.env.PORT || '3000');

const FB_API_KEY    = 'AIzaSyDnn5s6kfDRVxVBDviLIvI7itI5reFbklk';
const FB_PROJECT_ID = 'infografia-porter';

// ── Twilio — wrapped so placeholder values don't crash the server ────────────
let twilioClient = null;
try {
  if (TWILIO_ACCOUNT_SID.startsWith('AC') && TWILIO_ACCOUNT_SID.length === 34 && TWILIO_AUTH_TOKEN.length >= 20) {
    twilioClient = twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);
    console.log('✅ Twilio client inicializado');
  } else {
    console.warn('⚠️  Twilio no configurado — configura TWILIO_ACCOUNT_SID y TWILIO_AUTH_TOKEN reales en Railway');
  }
} catch (e) {
  console.warn('⚠️  Twilio error al iniciar:', e.message);
}

// ── Anthropic — wrapped so placeholder values don't crash ────────────────────
let anthropic = null;
try {
  if (ANTHROPIC_API_KEY.startsWith('sk-ant')) {
    anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });
    console.log('✅ Anthropic client inicializado');
  } else {
    console.warn('⚠️  ANTHROPIC_API_KEY no configurada — IA deshabilitada');
  }
} catch (e) {
  console.warn('⚠️  Anthropic error al iniciar:', e.message);
}

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/conversation' });

async function loadCerebro(bizId) {
  try {
    const url = `https://firestore.googleapis.com/v1/projects/${FB_PROJECT_ID}/databases/(default)/documents:runQuery?key=${FB_API_KEY}`;
    const body = { structuredQuery: { from: [{ collectionId: 'cerebro' }], where: { fieldFilter: { field: { fieldPath: 'biz' }, op: 'EQUAL', value: { stringValue: bizId } } }, orderBy: [{ field: { fieldPath: 'ts' }, direction: 'DESCENDING' }], limit: 40 } };
    const res = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    const docs = await res.json();
    if (!Array.isArray(docs)) return '';
    return docs.filter(d => d.document?.fields).map(d => {
      const f = d.document.fields;
      const conf = f.confidence?.integerValue || f.confidence?.doubleValue || 3;
      const stars = conf >= 5 ? '⭐⭐⭐⭐⭐' : conf >= 3 ? '⭐⭐⭐' : '⭐';
      return `[${(f.type?.stringValue||'dato').toUpperCase()} ${stars}] ${f.title?.stringValue||''}: ${f.content?.stringValue||''}`;
    }).join('\n');
  } catch (e) { console.error('Error Cerebro:', e.message); return ''; }
}

function buildSystemPrompt(bizName, bizId, knowledge, mode) {
  return `Eres el asistente de IA de ${bizName}. ${mode==='outbound'?'Estás llamando en su nombre.':'Estás atendiendo una llamada entrante.'}

REGLAS: Habla natural, conciso (2-3 oraciones). Español mexicano. No menciones que eres IA salvo que pregunten. Si quieren comprar/agendar pide nombre y número. Si no sabes algo di que el equipo les marcará. Despídete con "¡Que tenga un excelente día!"

${knowledge ? 'CONOCIMIENTO:\n' + knowledge : '(Sin conocimiento cargado aún.)'}`;
}

wss.on('connection', (ws, req) => {
  const params = new URLSearchParams(req.url.split('?')[1] || '');
  const bizId = params.get('biz') || 'default';
  const bizName = decodeURIComponent(params.get('name') || 'la empresa');
  const mode = params.get('mode') || 'inbound';
  const history = [];
  let knowledge = '';
  let aborted = false;
  loadCerebro(bizId).then(k => { knowledge = k; });
  console.log(`📞 Conexión | biz=${bizId} mode=${mode}`);

  ws.on('message', async (raw) => {
    let msg; try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (msg.type === 'interrupt') { aborted = true; return; }
    if (msg.type !== 'prompt' || !msg.voicePrompt?.trim()) return;
    history.push({ role: 'user', content: msg.voicePrompt });
    aborted = false;
    if (!anthropic) { ws.send(JSON.stringify({ type: 'text', token: 'El servicio de IA no está configurado aún. Por favor comunícate más tarde.', last: true })); return; }
    try {
      const stream = await anthropic.messages.stream({ model: 'claude-haiku-4-5-20251001', max_tokens: 200, system: buildSystemPrompt(bizName, bizId, knowledge, mode), messages: history });
      let full = '';
      for await (const chunk of stream) {
        if (aborted) break;
        if (chunk.type === 'content_block_delta' && chunk.delta?.type === 'text_delta') {
          full += chunk.delta.text;
          ws.send(JSON.stringify({ type: 'text', token: chunk.delta.text, last: false }));
        }
      }
      if (!aborted) { ws.send(JSON.stringify({ type: 'text', token: '', last: true })); history.push({ role: 'assistant', content: full }); }
    } catch (err) { console.error('Claude error:', err.message); ws.send(JSON.stringify({ type: 'text', token: 'Disculpa el problema técnico. ¿Puedes repetir?', last: true })); }
  });
  ws.on('close', () => console.log(`📵 Fin llamada biz=${bizId}`));
});

app.post('/incoming-call', (req, res) => {
  const bizId = req.query.biz || req.body.biz || 'default';
  const bizName = req.query.name || req.body.name || 'la Agencia';
  const wsUrl = `wss://${new URL(SERVER_URL).host}/conversation?biz=${bizId}&name=${encodeURIComponent(bizName)}&mode=inbound`;
  const twiml = new twilio.twiml.VoiceResponse();
  twiml.connect({ action: `${SERVER_URL}/call-ended` }).conversationRelay({ url: wsUrl, welcomeGreeting: `¡Hola! Gracias por llamar a ${bizName}. ¿En qué te puedo ayudar?`, language: 'es-MX', ttsProvider: 'google', voice: 'es-MX-Standard-B', interruptible: 'any', transcriptionProvider: 'google' });
  res.type('text/xml').send(twiml.toString());
});

app.post('/outbound-twiml', (req, res) => {
  const bizId = req.query.biz || 'default';
  const bizName = req.query.name || 'la Agencia';
  const greeting = req.query.greeting || `Hola, soy el asistente de ${bizName}.`;
  const wsUrl = `wss://${new URL(SERVER_URL).host}/conversation?biz=${bizId}&name=${encodeURIComponent(bizName)}&mode=outbound`;
  const twiml = new twilio.twiml.VoiceResponse();
  twiml.connect({ action: `${SERVER_URL}/call-ended` }).conversationRelay({ url: wsUrl, welcomeGreeting: greeting, language: 'es-MX', ttsProvider: 'google', voice: 'es-MX-Standard-B', interruptible: 'any' });
  res.type('text/xml').send(twiml.toString());
});

app.post('/make-call', async (req, res) => {
  if (!twilioClient) return res.status(500).json({ error: 'Twilio no configurado — agrega credenciales reales en Railway Variables' });
  const { to, bizId, bizName, greeting } = req.body;
  if (!to) return res.status(400).json({ error: 'Número destino requerido' });
  try {
    const call = await twilioClient.calls.create({ to, from: TWILIO_PHONE_NUMBER, url: `${SERVER_URL}/outbound-twiml?biz=${encodeURIComponent(bizId||'default')}&name=${encodeURIComponent(bizName||'la Agencia')}&greeting=${encodeURIComponent(greeting||'')}`, statusCallback: `${SERVER_URL}/call-status`, statusCallbackMethod: 'POST' });
    res.json({ success: true, callSid: call.sid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/call-ended', (req, res) => res.type('text/xml').send('<Response></Response>'));
app.post('/call-status', (req, res) => res.sendStatus(200));

app.get('/health', (req, res) => res.json({ status: 'ok', twilio: !!twilioClient, claude: !!anthropic }));
app.get('/', (req, res) => res.json({ status: 'ok', app: 'Agencia Paco AI — Agente Telefónico', twilio: !!twilioClient, claude: !!anthropic, url: SERVER_URL }));

server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🚀 Agente Telefónico en puerto ${PORT}`);
  console.log(`   Twilio: ${twilioClient ? '✅' : '❌ (falta SID/Token)'} | Claude: ${anthropic ? '✅' : '❌ (falta API key)'}`);
});
