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
const { joinVoiceChannel, createAudioPlayer, createAudioResource, AudioPlayerStatus, EndBehaviorType, getVoiceConnection, StreamType } = require('@discordjs/voice');
const prism = require('prism-media');
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { spawn } = require('child_process');
const twilio = require('twilio');
const ffmpegPath = require('ffmpeg-static');
console.log('ffmpeg path:', ffmpegPath);

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
  return spawn(ffmpegPath || 'ffmpeg', ['-f', 'mulaw', '-ar', '8000', '-ac', '1', '-i', 'pipe:0',
    '-f', 's16le', '-ar', '48000', '-ac', '2', 'pipe:1'], { stdio: ['pipe', 'pipe', 'ignore'] });
}
// PCM 48kHz stereo (Discord) -> mulaw 8kHz mono, via ffmpeg
function pcm48kToMulaw() {
  return spawn(ffmpegPath || 'ffmpeg', ['-f', 's16le', '-ar', '48000', '-ac', '2', '-i', 'pipe:0',
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
      const resource = createAudioResource(toDiscord.stdout, { inputType: StreamType.Raw });
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

  if (cmd === 'join') {
    if (!msg.member.voice.channel) return msg.reply('Join a voice channel first, then use !join.');
    if (getVoiceConnection(guildId)) return msg.reply('Already in a voice channel.');
    const connection = joinVoiceChannel({
      channelId: msg.member.voice.channelId,
      guildId,
      adapterCreator: msg.guild.voiceAdapterCreator,
      selfDeaf: false,
      selfMute: false,
    });
    const player = createAudioPlayer();
    connection.subscribe(player);
    connection._player = player;
    await msg.reply(`Joined ${msg.member.voice.channel.name}. Try \`!play <url>\` or \`!record\`.`);
  }

  if (cmd === 'leave') {
    endBridge(guildId);
    getVoiceConnection(guildId)?.destroy();
    await msg.reply('Left voice.');
  }

  // !play <audio URL> — play an audio clip from a URL in Discord voice.
  // Works with direct .mp3/.wav/.ogg links. For YouTube, use a direct
  // audio stream URL. If a call is active, the clip plays in Discord
  // voice (it will pause phone audio on the shared player).
  // !playphone <audio URL> — play the clip into the active phone call instead.
  if (cmd === 'play' || cmd === 'playphone') {
    const url = rest[0];
    if (!url || !url.startsWith('http')) return msg.reply('Usage: `!play https://example.com/clip.mp3`');
    const bridge = bridges.get(guildId);
    if (cmd === 'playphone') {
      if (!bridge || !bridge.twilioWs) return msg.reply('No active call. Use `!call` first, then `!playphone <url>`.');
      // Stream the clip to Twilio: ffmpeg URL -> mulaw 8kHz -> media stream
      const ff = spawn(ffmpegPath || 'ffmpeg', ['-i', url, '-f', 'mulaw', '-ar', '8000', '-ac', '1', 'pipe:1'], { stdio: ['ignore', 'pipe', 'ignore'] });
      ff.stdout.on('data', (chunk) => {
        if (bridge.twilioWs.readyState === WebSocket.OPEN) {
          bridge.twilioWs.send(JSON.stringify({ event: 'media', streamSid: bridge.streamSid, media: { payload: chunk.toString('base64') } }));
        }
      });
      await msg.reply(`Playing ${url} into the phone call.`);
      return;
    }
    if (!msg.member.voice.channel) return msg.reply('Join a voice channel first, then use !play.');
    let connection = getVoiceConnection(guildId);
    let player;
    if (bridge) {
      connection = bridge.connection;
      player = bridge.player;
    } else {
      connection = joinVoiceChannel({ channelId: msg.member.voice.channelId, guildId, adapterCreator: msg.guild.voiceAdapterCreator });
      player = createAudioPlayer();
      connection.subscribe(player);
      connection._player = player;
    }
    // ffmpeg fetches the URL and outputs Opus-ready PCM for Discord
    const ff = spawn(ffmpegPath || 'ffmpeg', ['-i', url, '-f', 's16le', '-ar', '48000', '-ac', '2', 'pipe:1'], { stdio: ['ignore', 'pipe', 'pipe'] });
    ff.stderr.on('data', (d) => console.log('play ffmpeg:', d.toString().slice(0, 500)));
    ff.on('error', (e) => console.log('play ffmpeg spawn error:', e.message));
    const resource = createAudioResource(ff.stdout, { inputType: StreamType.Raw });
    player.play(resource);
    await msg.reply(`Playing ${url} in ${msg.member.voice.channel.name}. Use \`!stop\` to stop it.`);
  }

  if (cmd === 'stop') {
    const bridge = bridges.get(guildId);
    const conn = getVoiceConnection(guildId);
    if (bridge) bridge.player.stop();
    else if (conn && conn._player) conn._player.stop();
    else return msg.reply('Nothing playing right now.');
    await msg.reply('Stopped. (Play a new clip with `!play <url>`.)');
  }

  // !record — record the Discord voice chat (and, if a call is active,
  // the phone side is recorded by Twilio separately — see !recordcall).
  // Only record people who know they are being recorded.
  // Records to MP3 (compressed) in ./recordings/ on the host.
  // !stoprecord — stop recording, convert to MP3 if needed, and
  // immediately upload the MP3 to this Discord channel as an attachment.
  // Note: Discord's upload limit is 25 MB per file (10 MB on some
  // servers without Nitro) — a long recording may exceed it, in which
  // case the bot will report the file size instead.
  if (cmd === 'record') {
    const conn = getVoiceConnection(guildId);
    if (!conn) return msg.reply('Join a voice channel and use !join first, then !record.');
    if (conn._recording) return msg.reply('Already recording.');
    const fs = require('fs');
    fs.mkdirSync('recordings', { recursive: true });
    const file = `recordings/${guildId}-${Date.now()}.mp3`;
    // ffmpeg: PCM 48kHz stereo in -> MP3 out (128k, compressed)
    const ff = spawn(ffmpegPath || 'ffmpeg', ['-f', 's16le', '-ar', '48000', '-ac', '2', '-i', 'pipe:0', '-codec:a', 'libmp3lame', '-b:a', '128k', '-y', file], { stdio: ['pipe', 'ignore', 'ignore'] });
    conn._recording = { ff, file, channel: msg.channel, streams: new Map() };
    // Auto-stop after 15 minutes: end ffmpeg, then upload the MP3 to chat
    conn._recording.timer = setTimeout(async () => {
      const rec = conn._recording;
      if (!rec) return;
      conn._recording = null;
      try { rec.ff.stdin.end(); } catch {}
      await new Promise((r) => rec.ff.on('close', r));
      const fs = require('fs');
      try {
        const size = fs.statSync(rec.file).size;
        if (size > 25 * 1024 * 1024) {
          await rec.channel.send(`Recording stopped automatically after 15 minutes, but the MP3 is ${(size / 1024 / 1024).toFixed(1)} MB — too big for Discord to upload (25 MB limit). File is on the host as ${rec.file}.`);
        } else {
          await rec.channel.send({ content: 'Recording stopped automatically after 15 minutes. Here is the recording:', files: [{ attachment: rec.file, name: rec.file.split('/').pop() }] });
        }
      } catch (e) {
        await rec.channel.send(`Recording stopped after 15 minutes, but I couldn't upload the MP3: ${e.message}`);
      }
    }, 15 * 60 * 1000);
    if (!conn._recordListener) {
      conn._recordListener = true;
      conn.receiver.speaking.on('start', (userId) => {
        if (!conn._recording) return;
        const opus = conn.receiver.subscribe(userId, { end: { behavior: EndBehaviorType.AfterSilence, duration: 500 } });
        const decoder = new prism.opus.Decoder({ rate: 48000, channels: 2, frameSize: 960 });
        opus.pipe(decoder).pipe(conn._recording.ff.stdin, { end: false });
      });
    }
    await msg.reply(`Recording voice chat. Tell everyone in the channel you're recording. Use \`!stoprecord\` to stop — I'll upload the MP3 here when you do.`);
  }

  if (cmd === 'stoprecord') {
    const conn = getVoiceConnection(guildId);
    if (!conn || !conn._recording) return msg.reply('Not recording.');
    const rec = conn._recording;
    conn._recording = null;
    if (rec.timer) clearTimeout(rec.timer);
    try { rec.ff.stdin.end(); } catch {}
    // Wait a moment for ffmpeg to finish writing the MP3, then upload it
    await new Promise((r) => rec.ff.on('close', r));
    const fs = require('fs');
    try {
      const size = fs.statSync(rec.file).size;
      if (size > 25 * 1024 * 1024) {
        await msg.reply(`Stopped recording, but the MP3 is ${(size / 1024 / 1024).toFixed(1)} MB — too big for Discord to upload (25 MB limit). File is on the host as ${rec.file}.`);
      } else {
        await rec.channel.send({ content: 'Here is the recording:', files: [{ attachment: rec.file, name: rec.file.split('/').pop() }] });
      }
    } catch (e) {
      await msg.reply(`Stopped recording, but I couldn't upload the MP3: ${e.message}`);
    }
  }

  // !recordcall — turn on Twilio call recording for the active call
  // ($0.0025/min, stored by Twilio). Use !stoprecordcall to stop it.
  if (cmd === 'recordcall') {
    const bridge = bridges.get(guildId);
    if (!bridge || !bridge.callSid) return msg.reply('No active call.');
    try {
      await twilioClient.calls(bridge.callSid).recordings.create({ recordingStatusCallback: `https://${PUBLIC_URL}/` });
      await msg.reply('Twilio call recording started.');
    } catch (e) { await msg.reply(`Couldn't start call recording: ${e.message}`); }
  }

  if (cmd === 'stoprecordcall') {
    const bridge = bridges.get(guildId);
    if (!bridge || !bridge.callSid) return msg.reply('No active call.');
    try {
      await twilioClient.calls(bridge.callSid).recordings('current').update({ status: 'stopped' });
      await msg.reply('Twilio call recording stopped.');
    } catch (e) { await msg.reply(`Couldn't stop call recording: ${e.message}`); }
  }
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => console.log(`Listening on ${PORT}`));
client.login(DISCORD_TOKEN);
