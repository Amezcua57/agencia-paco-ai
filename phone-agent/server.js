/**
 * Agencia Paco AI — Agente Telefónico
 * Servidor Node.js con Twilio ConversationRelay + Claude API
 * Lee el Cerebro Vivo de Firebase para responder con conocimiento real
 *
 * Env vars necesarias (configura en Railway):
 *   TWILIO_ACCOUNT_SID   — de console.twilio.com
 *   TWILIO_AUTH_TOKEN    — de console.twilio.com
 *   TWILIO_PHONE_NUMBER  — número Twilio +521234567890
 *   ANTHROPIC_API_KEY    — sk-ant-...
 *   SERVER_URL           — https://tu-app.railway.app (sin / al final)
 *   PORT                 — Railway lo pone automático (default 3000)
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

const twilioClient = (TWILIO_ACCOUNT_SID && TWILIO_AUTH_TOKEN)
  ? twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN) : null;

const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/conversation' });

async function loadCerebro(bizId) {
  try {
    const url = `https://firestore.googleapis.com/v1/projects/${FB_PROJECT_ID}/databases/(default)/documents:runQuery?key=${FB_API_KEY}`;
    const body = {
      structuredQuery: {
        from: [{ collectionId: 'cerebro' }],
        where: { fieldFilter: { field: { fieldPath: 'biz' }, op: 'EQUAL', value: { stringValue: bizId } } },
        orderBy: [{ field: { fieldPath: 'ts' }, direction: 'DESCENDING' }],
        limit: 40
      }
    };
    const res = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    const docs = await res.json();
    if (!Array.isArray(docs)) return '';
    return docs.filter(d => d.document?.fields).map(d => {
      const f = d.document.fields;
      const type = f.type?.stringValue || 'dato';
      const title = f.title?.stringValue || '';
      const content2 = f.content?.stringValue || '';
      const conf = f.confidence?.integerValue || f.confidence?.doubleValue || 3;
      const stars = conf >= 5 ? '⭐⭐⭐⭐⭐' : conf >= 3 ? '⭐⭐⭐' : '⭐';
      return `[${type.toUpperCase()} ${stars}] ${title ? title + ': ' : ''}${content2}`;
    }).join('\n');
  } catch (e) { console.error('Error loading Cerebro Vivo:', e.message); return ''; }
}

function buildSystemPrompt(bizName, bizId, knowledge, mode = 'inbound') {
  const intro = mode === 'outbound'
    ? `Estás haciendo una llamada en nombre de ${bizName}.`
    : `Estás contestando una llamada entrante para ${bizName}.`;
  return `Eres el asistente de inteligencia artificial de ${bizName}.
${intro}

REGLAS DE CONVERSACIÓN:
• Habla de forma NATURAL y CONCISA — máximo 2-3 oraciones por respuesta.
• Usa español mexicano informal pero profesional.
• Nunca digas que eres una IA a menos que te lo pregunten directamente.
• Si te preguntan si eres humano, di: "Soy el asistente virtual de ${bizName}."
• Si el cliente quiere comprar o agendar, pide su nombre completo y número para que el equipo le llame.
• Si no sabes algo, di: "Déjame consultar con el equipo y te marcamos en breve."
• Nunca inventes precios, fechas ni datos que no tengas en el conocimiento.
• Al despedirte di siempre: "¡Que tenga un excelente día!"

${knowledge ? `CONOCIMIENTO DE ${bizName.toUpperCase()}:\n${knowledge}` : `(Conocimiento aún no cargado. Ofrece poner en contacto con el equipo.)`}`;
}

wss.on('connection', (ws, req) => {
  const params = new URLSearchParams(req.url.replace('/conversation?', ''));
  const bizId = params.get('biz') || 'default';
  const bizName = params.get('name') || 'la empresa';
  const mode = params.get('mode') || 'inbound';
  const history = [];
  let knowledge = '';
  let aborted = false;
  loadCerebro(bizId).then(k => { knowledge = k; });
  console.log(`📞 Nueva conexión | biz=${bizId} | mode=${mode}`);

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (msg.type === 'interrupt') { aborted = true; return; }
    if (msg.type === 'prompt') {
      const userText = msg.voicePrompt || '';
      if (!userText.trim()) return;
      history.push({ role: 'user', content: userText });
      aborted = false;
      const systemPrompt = buildSystemPrompt(bizName, bizId, knowledge, mode);
      try {
        const stream = await anthropic.messages.stream({
          model: 'claude-haiku-4-5-20251001', max_tokens: 200,
          system: systemPrompt, messages: history
        });
        let fullResponse = '';
        for await (const chunk of stream) {
          if (aborted) break;
          if (chunk.type === 'content_block_delta' && chunk.delta?.type === 'text_delta') {
            const token = chunk.delta.text;
            fullResponse += token;
            ws.send(JSON.stringify({ type: 'text', token, last: false }));
          }
        }
        if (!aborted) {
          ws.send(JSON.stringify({ type: 'text', token: '', last: true }));
          history.push({ role: 'assistant', content: fullResponse });
        }
      } catch (err) {
        console.error('Error Claude:', err.message);
        ws.send(JSON.stringify({ type: 'text', token: 'Disculpa, tuve un problema técnico. ¿Puedes repetir?', last: true }));
      }
    }
  });
  ws.on('close', () => console.log(`📵 Llamada terminada | biz=${bizId}`));
});

app.post('/incoming-call', (req, res) => {
  const bizId = req.query.biz || req.body.biz || 'default';
  const bizName = req.query.name || req.body.name || 'la Agencia';
  const serverHost = new URL(SERVER_URL).host;
  const wsUrl = `wss://${serverHost}/conversation?biz=${bizId}&name=${encodeURIComponent(bizName)}&mode=inbound`;
  const VR = twilio.twiml.VoiceResponse;
  const twiml = new VR();
  const connect = twiml.connect({ action: `${SERVER_URL}/call-ended` });
  connect.conversationRelay({ url: wsUrl, welcomeGreeting: `¡Hola! Gracias por llamar a ${bizName}. ¿En qué te puedo ayudar?`, language: 'es-MX', ttsProvider: 'google', voice: 'es-MX-Standard-B', interruptible: 'any', transcriptionProvider: 'google' });
  res.type('text/xml').send(twiml.toString());
});

app.post('/outbound-twiml', (req, res) => {
  const bizId = req.query.biz || 'default';
  const bizName = req.query.name || 'la Agencia';
  const greeting = req.query.greeting || `Hola, soy el asistente de ${bizName}. ¿Tiene un momento?`;
  const serverHost = new URL(SERVER_URL).host;
  const wsUrl = `wss://${serverHost}/conversation?biz=${bizId}&name=${encodeURIComponent(bizName)}&mode=outbound`;
  const VR = twilio.twiml.VoiceResponse;
  const twiml = new VR();
  twiml.connect({ action: `${SERVER_URL}/call-ended` }).conversationRelay({ url: wsUrl, welcomeGreeting: greeting, language: 'es-MX', ttsProvider: 'google', voice: 'es-MX-Standard-B', interruptible: 'any' });
  res.type('text/xml').send(twiml.toString());
});

app.post('/make-call', async (req, res) => {
  if (!twilioClient) return res.status(500).json({ error: 'Twilio no configurado' });
  const { to, bizId, bizName, greeting } = req.body;
  if (!to) return res.status(400).json({ error: 'Número destino requerido' });
  const twimlUrl = `${SERVER_URL}/outbound-twiml?biz=${encodeURIComponent(bizId||'default')}&name=${encodeURIComponent(bizName||'la Agencia')}&greeting=${encodeURIComponent(greeting||'')}`;
  try {
    const call = await twilioClient.calls.create({ to, from: TWILIO_PHONE_NUMBER, url: twimlUrl, statusCallback: `${SERVER_URL}/call-status`, statusCallbackMethod: 'POST' });
    res.json({ success: true, callSid: call.sid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/call-ended', (req, res) => { res.type('text/xml').send('<Response></Response>'); });
app.post('/call-status', (req, res) => { res.sendStatus(200); });

app.get('/', (req, res) => {
  res.json({ status: 'ok', app: 'Agencia Paco AI — Agente Telefónico', twilio: !!twilioClient, claude: !!ANTHROPIC_API_KEY });
});

server.listen(PORT, () => {
  console.log(`\n🚀 Agente Telefónico corriendo en puerto ${PORT}`);
  console.log(`   Twilio: ${twilioClient ? '✅' : '❌'} | Claude: ${ANTHROPIC_API_KEY ? '✅' : '❌'}`);
});
