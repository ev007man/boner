/**
 * Discord -> Phone live-talk bot — v2 (Node.js)
 *
 * Mandatory feature: you talk LIVE through Discord into a real phone call.
 *
 * How it works:
 *  1. You type !call +19055551234 in Discord, while in a voice channel.
 *  2. Bot joins your voice channel.
 *  3. Bot dials the number via Twilio. Twilio answers and opens a
 *     Media Stream websocket back to this server (/media-stream).
 *  4. Phone audio (mulaw 8kHz) -> decoded -> played into Discord voice.
 *  5. Your Discord voice (Opus 48kHz) -> decoded -> re-encoded to
 *     mulaw 8kHz -> sent back down the Twilio stream.
 *  6. !hangup ends the call and leaves voice.
 *
 * This is a real two-way bridge. It needs:
 *  - ffmpeg installed on the host (Render: add it in the build, see README)
 *  - A PUBLIC wss:// URL — Twilio cannot reach localhost. On Render,
 *    PUBLIC_URL must be your https://xxx.onrender.com URL, and the
 *    stream URL is wss://xxx.onrender.com/media-stream
 *  - An always-on host for live calls. Render FREE sleeps after 15 min
 *    idle, so a call will drop / the bot will be offline. Use Render
 *    paid Background Worker ($7/mo) for live calls, or Oracle free VPS.
 *
 * Env: DISCORD_TOKEN, TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN,
 *       TWILIO_PHONE_NUMBER, PUBLIC_URL
 */
require('dotenv').config();
const { Client, GatewayIntentBits } = require('discord.js');
const { joinVoiceChannel, createAudioPlayer, createAudioResource, AudioPlayerStatus, EndBehaviorType, getVoiceConnection } = require('@discordjs/voice');
const prism = require('prism-media');
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { spawn } = require('child_process');
const twilio = require('twilio');

const DISCORD_TOKEN = process.env.DISCORD_TOKEN;
const TWILIO_SID = process.env.TWILIO_ACCOUNT_SID;
const TWILIO_TOKEN = process.env.TWILIO_AUTH_TOKEN;
const TWILIO_NUMBER = process.env.TWILIO_PHONE_NUMBER;
const PUBLIC_URL = (process.env.PUBLIC_URL || '').replace(/^https?:\/\//, '').replace(/\/$/, '');

const twilioClient = twilio(TWILIO_SID, TWILIO_TOKEN);
const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/media-stream' });

// One active bridge per guild
const bridges = new Map(); // guildId -> { twilioWs, streamSid, player, connection, callSid, to }

app.use(express.urlencoded({ extended: false }));
app.get('/', (req, res) => res.send('Discord call bot v2 is running.'));
app.post('/twiml', (req, res) => {
  // Twilio calls this when the phone answers. Tell it to stream audio.
  const streamUrl = `wss://${PUBLIC_URL}/media-stream`;
  res.type('text/xml').send(
    `<?xml version="1.0" encoding="UTF-8"?><Response><Connect><Stream url="${streamUrl}" /></Connect></Response>`
  );
});

// --- Audio helpers ---
// mulaw 8kHz (Twilio) -> PCM 48kHz stereo, via ffmpeg, as a readable stream
function mulawToPcm48k() {
  return spawn('ffmpeg', ['-f', 'mulaw', '-ar', '8000', '-ac', '1', '-i', 'pipe:0',
    '-f', 's16le', '-ar', '48000', '-ac', '2', 'pipe:1'], { stdio: ['pipe', 'pipe', 'ignore'] });
}
// PCM 48kHz stereo (Discord) -> mulaw 8kHz mono, via ffmpeg
function pcm48kToMulaw() {
  return spawn('ffmpeg', ['-f', 's16le', '-ar', '48000', '-ac', '2', '-i', 'pipe:0',
    '-f', 'mulaw', '-ar', '8000', '-ac', '1', 'pipe:1'], { stdio: ['pipe', 'pipe', 'ignore'] });
}

wss.on('connection', (ws) => {
  let bridge = null;
  let toDiscord = null; // ffmpeg mulaw->pcm
  ws.on('message', (data) => {
    const msg = JSON.parse(data.toString());
    if (msg.event === 'start') {
      // Find the bridge waiting for this stream
      const streamSid = msg.start.streamSid;
      for (const b of bridges.values()) {
        if (b.pendingStreamSid === streamSid || !b.twilioWs) { bridge = b; break; }
      }
      if (!bridge) return;
      bridge.twilioWs = ws;
      bridge.streamSid = streamSid;
      // Start phone -> Discord audio
      toDiscord = mulawToPcm48k();
      const resource = createAudioResource(toDiscord.stdout, { inputType: 'arbitrary' });
      bridge.player.play(resource);
      console.log('Media stream started', streamSid);
    }
    if (msg.event === 'media' && bridge && toDiscord) {
      // Twilio sends base64 mulaw
      toDiscord.stdin.write(Buffer.from(msg.media.payload, 'base64'));
    }
    if (msg.event === 'stop' && bridge) {
      console.log('Media stream stopped');
      endBridge(bridge.guildId);
    }
  });
  ws.on('close', () => { if (bridge) endBridge(bridge.guildId); });
});

function endBridge(guildId) {
  const b = bridges.get(guildId);
  if (!b) return;
  bridges.delete(guildId);
  try { if (b.callSid) twilioClient.calls(b.callSid).update({ status: 'completed' }); } catch {}
  try { b.player.stop(); } catch {}
  try { b.connection.destroy(); } catch {}
  try { b.twilioWs?.close(); } catch {}
}

const client = new Client({ intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMessages, GatewayIntentBits.MessageContent, GatewayIntentBits.GuildVoiceStates] });

client.on('ready', () => console.log(`Logged in as ${client.user.tag} — live-talk v2 ready`));

client.on('messageCreate', async (msg) => {
  if (msg.author.bot || !msg.content.startsWith('!')) return;
  const [cmd, ...rest] = msg.content.slice(1).split(/\s+/);
  const guildId = msg.guildId;

  if (cmd === 'call') {
    const number = rest[0];
    if (!number) return msg.reply('Usage: `!call +19055551234` — join a voice channel first.');
    if (!msg.member.voice.channel) return msg.reply('Join a voice channel first, then use !call.');
    if (!PUBLIC_URL) return msg.reply('PUBLIC_URL is not set, Twilio cannot stream back to this bot.');

    const connection = joinVoiceChannel({
      channelId: msg.member.voice.channelId,
      guildId,
      adapterCreator: msg.guild.voiceAdapterCreator,
      selfDeaf: false,
      selfMute: false,
    });
    const player = createAudioPlayer();
    connection.subscribe(player);

    const bridge = { guildId, connection, player, twilioWs: null, streamSid: null, callSid: null, to: number };
    bridges.set(guildId, bridge);

    // Discord -> Phone: capture your voice, transcode to mulaw, send to Twilio
    const receiver = connection.receiver;
    receiver.speaking.on('start', (userId) => {
      if (userId !== msg.author.id) return; // only the caller talks through
      const opus = receiver.subscribe(userId, { end: { behavior: EndBehaviorType.AfterSilence, duration: 500 } });
      const decoder = new prism.opus.Decoder({ rate: 48000, channels: 2, frameSize: 960 });
      const encoder = pcm48kToMulaw();
      opus.pipe(decoder).pipe(encoder.stdin);
      encoder.stdout.on('data', (chunk) => {
        if (bridge.twilioWs && bridge.twilioWs.readyState === WebSocket.OPEN) {
          bridge.twilioWs.send(JSON.stringify({ event: 'media', streamSid: bridge.streamSid, media: { payload: chunk.toString('base64') } }));
        }
      });
    });

    try {
      const call = await twilioClient.calls.create({
        to: number, from: TWILIO_NUMBER,
        url: `https://${PUBLIC_URL}/twiml`,
      });
      bridge.callSid = call.sid;
      await msg.reply(`Calling ${number} — talk in ${msg.member.voice.channel.name}. Use \`!hangup\` to end.`);
    } catch (e) {
      endBridge(guildId);
      await msg.reply(`Couldn't place that call: ${e.message}`);
    }
  }

  if (cmd === 'hangup') {
    if (!bridges.has(guildId)) return msg.reply('No active call.');
    endBridge(guildId);
    await msg.reply('Call ended.');
  }

  if (cmd === 'leave') {
    endBridge(guildId);
    getVoiceConnection(guildId)?.destroy();
    await msg.reply('Left voice.');
  }
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => console.log(`Listening on ${PORT}`));
client.login(DISCORD_TOKEN);
