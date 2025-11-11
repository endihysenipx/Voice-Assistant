# app.py
"""
Conversational Email + Calendar Voice Assistant (Unified Google & Microsoft) - v6.3
- Voice-first assistant for EITHER Gmail/Google Calendar OR Outlook/Microsoft Calendar.
- User chooses which service to connect on the main page.
- Uses tool-calling to search/read/summarize/compose emails AND list/create/update/delete meetings for the selected service.
- Dynamic backend selects the correct tools and prompts based on the active authentication (Google or Microsoft).
- Audio interrupt, WebSockets for audio + JSON, TTS + Whisper transcription.

v6.2: Implemented server-side sessions to reliably store OAuth flow state (fixes MSAL state/nonce issues).
v6.3: Outlook fixes:
  - outlook_search_emails(): empty query now fetches latest messages (no $search).
  - $search now always includes 'ConsistencyLevel: eventual' header via graph_request().
  - Tool schema updated so 'query' is optional.
  - outlook_read_email() prefers text body with 'Prefer: outlook.body-content-type="text"'.

Install:
  pip install fastapi uvicorn "websockets>=12" httpx python-dotenv msal "itsdangerous>=2.0" \
              google-auth google-auth-oauthlib google-api-python-client

Run:
  uvicorn app:app --host 0.0.0.0 --port 8000 --reload

Env (.env):
  # OpenAI
  OPENAI_API_KEY=...
  OPENAI_BASE_URL=https://api.openai.com
  REALTIME_MODEL=gpt-4o-mini
  REALTIME_VOICE=breeze
  # Google
  GOOGLE_CLIENT_ID=xxxxxxxx.apps.googleusercontent.com
  GOOGLE_CLIENT_SECRET=xxxxxxxx
  GOOGLE_REDIRECT_URI=http://localhost:8000/gmail/oauth2callback
  # Microsoft
  MS_TENANT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
  MS_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
  MS_CLIENT_SECRET=xxxxxxxx
  MS_REDIRECT_URI=http://localhost:8000/outlook/callback
"""

import os, io, json, base64, re, uuid, asyncio, traceback, datetime, time
from typing import Optional, List, Dict, Any

import httpx
import msal
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Header, Response
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from dotenv import load_dotenv
from starlette.middleware.sessions import SessionMiddleware

# Google / Gmail / Calendar
from email.message import EmailMessage
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

load_dotenv()

# ---------- Configuration ----------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com")
REALTIME_MODEL = os.getenv("REALTIME_MODEL", "gpt-4o-mini")
REALTIME_VOICE = os.getenv("REALTIME_VOICE", "breeze")

# Google Configuration
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:8000/gmail/oauth2callback")
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/calendar",
]

# Microsoft Graph API Configuration
MS_TENANT_ID = os.getenv("MS_TENANT_ID")
MS_CLIENT_ID = os.getenv("MS_CLIENT_ID")
MS_CLIENT_SECRET = os.getenv("MS_CLIENT_SECRET")
MS_REDIRECT_URI = os.getenv("MS_REDIRECT_URI", "http://localhost:8000/outlook/callback")
MS_SCOPES = ["User.Read", "Mail.ReadWrite", "Mail.Send", "Calendars.ReadWrite"]
MS_AUTHORITY = f"https://login.microsoftonline.com/{MS_TENANT_ID}"
GRAPH_API_ENDPOINT = "https://graph.microsoft.com/v1.0"

if not all([OPENAI_API_KEY, GOOGLE_CLIENT_ID, MS_CLIENT_ID]):
    raise RuntimeError("Set OpenAI, Google, and Microsoft credentials in .env file")

app = FastAPI()

# Session cookie (used to store OAuth state for both providers)
app.add_middleware(SessionMiddleware, secret_key="a_very_secret_key_for_oauth_session")

# In-memory demo state (single user)
_GMAIL_CREDS: Optional[Credentials] = None
_MSAL_TOKEN: Optional[Dict[str, Any]] = None
_GENERATED_AUDIO: Dict[str, bytes] = {}

# ---------- Global HTTP client ----------
_httpx_client: Optional[httpx.AsyncClient] = None

@app.on_event("startup")
async def _startup():
    global _httpx_client
    _httpx_client = httpx.AsyncClient(timeout=httpx.Timeout(60.0), limits=httpx.Limits(max_connections=50))

@app.on_event("shutdown")
async def _shutdown():
    global _httpx_client
    if _httpx_client:
        await _httpx_client.aclose()

def _client() -> httpx.AsyncClient:
    if not _httpx_client:
        raise RuntimeError("HTTP client not initialized")
    return _httpx_client

# ======================= UI / HTML Page =======================

CONVERSATIONAL_HTML = """
<!doctype html><html><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no"/>
<title>Voice Assistant</title>
<style>
  :root { --bg: #111827; --card: #1f2937; --ink: #f9fafb; --muted: #9ca3af; --brand: #818cf8; --brand-hover: #6366f1; --red: #f87171; --border: #374151; --chip: #312e81; --chip-ink: #c7d2fe; --user-bubble-bg: #3730a3; --user-bubble-ink: #e0e7ff; }
  * { box-sizing:border-box; -webkit-tap-highlight-color:transparent; }
  html, body { height:100%; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background:var(--bg); color:var(--ink); margin:0; display:flex; flex-direction:column; }
  .app-container { display:flex; flex-direction:column; height:100%; max-width:800px; width:100%; margin:0 auto; background:var(--bg); }
  header { padding:12px 16px; border-bottom:1px solid var(--border); display:flex; justify-content:space-between; align-items:center; flex-shrink:0; background: var(--card); }
  header h1 { font-size:18px; margin:0; }
  .badge { background:var(--chip); color:var(--chip-ink); border-radius:12px; padding:3px 10px; font-size:12px; font-weight:500; }
  .chat-container { flex:1 1 auto; overflow-y:auto; padding:16px; display:flex; flex-direction:column; gap:12px; }
  .bubble { max-width:85%; padding:10px 14px; border-radius:18px; line-height:1.5; }
  .bubble.user { margin-left:auto; background:var(--user-bubble-bg); color:var(--user-bubble-ink); }
  .bubble.assistant { margin-right:auto; background:var(--card); }
  .bubble.system { font-style:italic; text-align:center; background:transparent; color:var(--muted); font-size:13px; padding:6px 10px; border-radius:12px; }
  .bubble pre { white-space:pre-wrap; font-family:inherit; margin:0; }
  .draft { border:1px dashed var(--brand); border-radius:12px; padding:12px; background:rgba(31,41,55,0.5); margin-top:8px; }
  .draft h3 { margin:0 0 8px 0; font-size:14px; color: var(--brand); }
  .draft pre { white-space:pre-wrap; font-family:inherit; background:var(--bg); border:1px solid var(--border); padding:8px; border-radius:8px; }
  .draft .actions { display:flex; gap:8px; margin-top:10px; }
  .btn { padding:10px 14px; border:0; background:var(--brand); color:#fff; border-radius:10px; cursor:pointer; font-size:15px; transition:background-color .2s; }
  .btn:hover { background:var(--brand-hover); }
  .btn.secondary { background:#4b5563; color:#fff; }
  .btn.secondary:hover { background:#6b7280; }
  .pill { display:inline-block; background:var(--chip); color:var(--chip-ink); border:1px solid var(--brand); padding:2px 8px; border-radius:999px; font-size:11px; margin-top:6px; }
  .context-display { font-size:12px; color:var(--muted); line-height:1.4; background:var(--card); padding:8px 10px; border-radius:8px; border:1px solid var(--border); }
  .controls-bar { flex-shrink:0; padding:16px; border-top:1px solid var(--border); text-align:center; }
  #mic-btn { width:72px; height:72px; border-radius:50%; border:0; background:var(--brand); color:white; cursor:pointer; display:inline-flex; align-items:center; justify-content:center; transition: all .2s; box-shadow: 0 0 0 0 rgba(129, 140, 248, 0); }
  #mic-btn:disabled { background:var(--muted); cursor:not-allowed; }
  #mic-btn.listening, #mic-btn.speaking { background:var(--red); animation: pulse 1.5s infinite; }
  @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(248, 113, 113, 0.7); } 70% { box-shadow: 0 0 0 16px rgba(248, 113, 113, 0); } 100% { box-shadow: 0 0 0 0 rgba(248, 113, 113, 0); }
  }
  #status-text { color:var(--muted); font-size:14px; margin-top:12px; min-height:20px; }
  .auth-view { padding: 24px; text-align:center; }
  .auth-view h2 { margin-top:0; }
  .auth-buttons { display: flex; justify-content: center; gap: 16px; margin-top: 16px; }
</style>
</head><body>
<div id="app-container" class="app-container">
  <header>
    <h1><span id="service-name">Email</span> Assistant</h1><span class="badge">Voice AI</span>
  </header>
  <div class="chat-container" id="chat-container">
    <div id="auth-view" class="auth-view" style="display:none;">
      <h2>Welcome!</h2>
      <p id="auth-msg">Please connect an account to begin.</p>
      <div class="auth-buttons">
        <a class="btn" href="/gmail/login">Connect Google</a>
        <a class="btn" href="/outlook/login">Connect Outlook</a>
      </div>
    </div>
    <div id="chat-log"></div>
    <div id="draft-wrap" style="display:none"></div>
    <div id="context-wrap" style="display:none"></div>
  </div>
  <div id="controls" class="controls-bar" style="display:none;">
    <button id="mic-btn" onclick="handleMicClick()" disabled>
      <span id="mic-icon-container"></span>
    </button>
    <div id="status-text">Checking connection...</div>
  </div>
</div>
<audio id="audio-player" style="display:none;"></audio>
<script>
const AppState = { IDLE: 'IDLE', LISTENING: 'LISTENING', PROCESSING: 'PROCESSING', SPEAKING: 'SPEAKING' };
let state = AppState.IDLE; let socket; let mediaRecorder; let audioChunks = [];
const chatLog = document.getElementById('chat-log'); const chatContainer = document.getElementById('chat-container'); const micBtn = document.getElementById('mic-btn'); const micIconContainer = document.getElementById('mic-icon-container'); const statusText = document.getElementById('status-text'); const audioPlayer = document.getElementById('audio-player');
const ICONS = { mic: `<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/><line x1="12" y1="19" x2="12" y2="22"/></svg>`, stop: `<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"></rect></svg>`};

function setAppState(newState) {
  state = newState; micBtn.classList.remove('listening', 'speaking');
  switch (state) {
    case AppState.IDLE: micIconContainer.innerHTML = ICONS.mic; micBtn.disabled = false; updateStatus('Tap the mic to start.'); break;
    case AppState.LISTENING: micIconContainer.innerHTML = ICONS.stop; micBtn.classList.add('listening'); micBtn.disabled = false; updateStatus('Listening... tap to stop.'); break;
    case AppState.PROCESSING: micIconContainer.innerHTML = ICONS.mic; micBtn.disabled = true; updateStatus('Thinking...'); break;
    case AppState.SPEAKING: micIconContainer.innerHTML = ICONS.stop; micBtn.classList.add('speaking'); micBtn.disabled = false; break;
  }
}
function handleMicClick() {
  switch (state) {
    case AppState.IDLE:
      if (!socket || socket.readyState !== WebSocket.OPEN) { connectWebSocket().then(startRecording); }
      else { startRecording(); }
      break;
    case AppState.LISTENING: stopRecording(); break;
    case AppState.SPEAKING: stopCurrentAudio(); setAppState(AppState.IDLE); break;
  }
}
function scrollToBottom() { chatContainer.scrollTop = chatContainer.scrollHeight; }
function appendChat(role, text) { const wrap = document.createElement('div'); wrap.className = 'bubble ' + role; if (role === 'assistant') { const pre = document.createElement('pre'); pre.textContent = text; wrap.appendChild(pre); } else { wrap.textContent = text; } chatLog.appendChild(wrap); scrollToBottom(); }
function updateContext(info) { let contextWrap = document.getElementById('context-wrap'); if (info && info.id) { contextWrap.style.display = 'block'; contextWrap.innerHTML = `<div class="context-display"><div><span class="pill">Current Context</span></div><strong>Type:</strong> ${info.type || 'Email'}<br><strong>From/Organizer:</strong> ${info.from || info.organizer || 'N/A'}<br><strong>Subject/Title:</strong> ${info.subject || info.title || 'N/A'}</div>`; } else { contextWrap.style.display = 'none'; contextWrap.innerHTML = ''; } scrollToBottom(); }
function showDraft(to, subject, body){ const draftWrap = document.getElementById('draft-wrap'); draftWrap.innerHTML = `<div class="draft"><h3>Email draft (preview)</h3><div><strong>To:</strong> <span>${to || '(none)'}</span></div><div><strong>Subject:</strong> <span>${subject || '(none)'}</span></div><div style="margin-top:6px;"><strong>Body:</strong></div><pre>${body || ''}</pre><div class="actions"><button class="btn" onclick="sendDraft()">Send</button><button class="btn secondary" onclick="cancelDraft()">Cancel</button></div></div>`; draftWrap.style.display = 'block'; scrollToBottom(); }
function hideDraft(){ document.getElementById('draft-wrap').style.display = 'none'; }
function updateStatus(text){ statusText.textContent = text; }
function stopCurrentAudio() { audioPlayer.pause(); audioPlayer.src = ''; }
async function startRecording() {
  try {
    stopCurrentAudio();
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    const mimeType = MediaRecorder.isTypeSupported('audio/webm; codecs=opus') ? 'audio/webm; codecs=opus' : 'audio/webm';
    mediaRecorder = new MediaRecorder(stream, { mimeType }); audioChunks = [];
    mediaRecorder.ondataavailable = e => { if (e.data && e.data.size > 0) audioChunks.push(e.data); };
    mediaRecorder.onstop = () => {
      stream.getTracks().forEach(t => t.stop());
      if (socket && socket.readyState === WebSocket.OPEN && audioChunks.length > 0) {
        const audioBlob = new Blob(audioChunks, { type: mimeType });
        socket.send(audioBlob);
        setAppState(AppState.PROCESSING);
      } else { setAppState(AppState.IDLE); }
    };
    mediaRecorder.start(); setAppState(AppState.LISTENING);
  } catch (e) { console.error('Mic error', e); updateStatus('Microphone access denied.'); setAppState(AppState.IDLE); }
}
function stopRecording() { if (mediaRecorder && mediaRecorder.state === 'recording') { mediaRecorder.stop(); } }
async function checkAuth(){
  const r = await fetch('/api/status'); const j = await r.json();
  const serviceNameElem = document.getElementById('service-name');
  if (j.connected_service === 'none') {
    document.getElementById('auth-view').style.display = 'block';
    document.getElementById('controls').style.display = 'none';
    serviceNameElem.textContent = 'Email';
  } else {
    document.getElementById('auth-view').style.display = 'none';
    document.getElementById('controls').style.display = 'block';
    serviceNameElem.textContent = j.connected_service === 'google' ? 'Gmail' : 'Outlook';
    setAppState(AppState.IDLE);
  }
}
function connectWebSocket(){
  return new Promise((resolve, reject) => {
    updateStatus('Connecting to assistant...');
    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    socket = new WebSocket(`${proto}//${window.location.host}/ws`);
    socket.onopen = () => { appendChat('system', 'Connection established. Tap the mic to begin.'); resolve(); };
    socket.onclose = () => { updateStatus('Session ended.'); setAppState(AppState.IDLE); };
    socket.onerror = (err) => { console.error('WebSocket Error:', err); updateStatus('Connection error. Please refresh.'); setAppState(AppState.IDLE); reject(err); };
    socket.onmessage = (event) => {
      let msg; try { msg = JSON.parse(event.data); } catch { return; }
      switch (msg.type) {
        case 'play_audio': stopCurrentAudio(); updateStatus(msg.status_text); setAppState(AppState.SPEAKING); audioPlayer.src = msg.url; audioPlayer.play().catch(e => { console.error("Audio play failed:", e); setAppState(AppState.IDLE); }); break;
        case 'update_status': updateStatus(msg.text); break;
        case 'chat_append': appendChat(msg.role, msg.text); break;
        case 'context_update': updateContext(msg.context); break;
        case 'draft_preview': showDraft(msg.to, msg.subject, msg.body); break;
        case 'draft_clear': hideDraft(); break;
      }
    };
  });
}
function sendDraft(){ if(!socket || socket.readyState !== WebSocket.OPEN) return; socket.send(JSON.stringify({ action: 'send_draft' })); }
function cancelDraft(){ if(!socket || socket.readyState !== WebSocket.OPEN) return; socket.send(JSON.stringify({ action: 'cancel_draft' })); }
audioPlayer.onended = () => { if (state === AppState.SPEAKING) { setAppState(AppState.IDLE); } };
checkAuth();
</script>
</body></html>
"""

# ======================= OpenAI & API Helpers =======================

async def tts_any(text: str) -> str:
    payload = {"model": "tts-1", "voice": REALTIME_VOICE, "input": text, "response_format": "mp3"}
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    r = await _client().post(f"{OPENAI_BASE_URL.rstrip('/')}/v1/audio/speech", json=payload, headers=headers)
    r.raise_for_status()
    audio_id = str(uuid.uuid4())
    _GENERATED_AUDIO[audio_id] = r.content
    return f"/audio/{audio_id}"

async def transcribe_bytes(audio_bytes: bytes) -> str:
    files = {"file": ("speech.webm", audio_bytes, "audio/webm")}
    data = {"model": "whisper-1"}
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    r = await _client().post(f"{OPENAI_BASE_URL.rstrip('/')}/v1/audio/transcriptions", data=data, files=files, headers=headers)
    r.raise_for_status()
    return r.json().get("text", "").strip()

def _parse_rfc3339(dt_str: str) -> str:
    try:
        if re.search(r"[+-]\d{2}:\d{2}$", dt_str) or dt_str.endswith("Z"):
            return dt_str
        if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$", dt_str):
            return datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M").isoformat()
        if re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?$", dt_str):
            return dt_str if len(dt_str) > 16 else dt_str + ":00"
        if re.match(r"^\d{4}-\d{2}-\d{2}$", dt_str):
            return datetime.datetime.strptime(dt_str, "%Y-%m-%d").isoformat()
    except Exception:
        pass
    return dt_str

# --- Google Helpers ---
def _require_google_creds() -> Credentials:
    if not _GMAIL_CREDS or not _GMAIL_CREDS.valid:
        raise RuntimeError("Google not connected.")
    return _GMAIL_CREDS

def _gmail_service() -> Any:
    return build("gmail", "v1", credentials=_require_google_creds(), cache_discovery=False)

def _calendar_service() -> Any:
    return build("calendar", "v3", credentials=_require_google_creds(), cache_discovery=False)

def _get_email_body(msg: Dict) -> str:
    body_data = ""
    if 'parts' in msg.get('payload', {}):
        for part in msg['payload']['parts']:
            if part.get('mimeType') == 'text/plain' and 'data' in part.get('body', {}):
                body_data = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')
                break
    if not body_data and 'data' in msg.get('payload', {}).get('body', {}):
        body_data = base64.urlsafe_b64decode(msg['payload']['body']['data']).decode('utf-8', errors='ignore')
    return body_data

# --- Microsoft Helpers ---
def _get_msal_app():
    return msal.ConfidentialClientApplication(
        MS_CLIENT_ID, authority=MS_AUTHORITY, client_credential=MS_CLIENT_SECRET
    )

def _require_ms_token() -> str:
    global _MSAL_TOKEN
    if not _MSAL_TOKEN:
        raise RuntimeError("Microsoft not connected.")
    # Refresh if nearly expired (simple heuristic)
    if _MSAL_TOKEN.get("expires_in", 0) < 60 and _MSAL_TOKEN.get("refresh_token"):
        new_token = _get_msal_app().acquire_token_by_refresh_token(
            _MSAL_TOKEN.get("refresh_token"), scopes=MS_SCOPES
        )
        if "error" in new_token:
            _MSAL_TOKEN = None
            raise RuntimeError("Could not refresh token.")
        _MSAL_TOKEN = new_token
    return _MSAL_TOKEN["access_token"]

async def graph_request(
    method: str,
    endpoint: str,
    headers: Optional[Dict[str, str]] = None,
    **kwargs
) -> httpx.Response:
    """
    Unified Graph request helper.
    - Always adds ConsistencyLevel: eventual (required for $search).
    - Surfaces Graph error text for better debugging.
    """
    token = _require_ms_token()
    base_headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "ConsistencyLevel": "eventual",
    }
    if headers:
        base_headers.update(headers)
    url = f"{GRAPH_API_ENDPOINT}{endpoint}"
    r = await _client().request(method, url, headers=base_headers, **kwargs)
    try:
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        detail = r.text
        raise RuntimeError(f"Graph error {r.status_code}: {detail}") from e
    return r

# ======================= Conversational Logic (Agentic) =======================

class ConversationManager:
    def __init__(self, ws: WebSocket, service_type: str):
        self.ws = ws
        self.service_type = service_type

        now = datetime.datetime.now()
        current_time_str = now.strftime("%A, %B %d, %Y, %I:%M %p %Z")

        google_prompt = (
            f"You are a helpful, conversational Google assistant for Gmail and Calendar.\n"
            f"- CRITICAL CONTEXT: The current date and time is {current_time_str}. "
            f"Use this to resolve all relative time references like 'today', 'tomorrow', etc."
        )
        microsoft_prompt = (
            f"You are a helpful, conversational Microsoft Outlook assistant for Mail and Calendar.\n"
            f"- CRITICAL CONTEXT: The current date and time is {current_time_str}. "
            f"Use this to resolve all relative time references like 'today', 'tomorrow', etc."
        )

        base_instructions = """
- Help the user manage their inbox AND calendar using your voice.
- After calling a tool, ALWAYS tell the user what you did and what you found (concise).
- When reading long emails, suggest summarizing.
- When listing events, include start time (with date) and title; include location if present.
- When creating events, confirm title, start, end, attendees, and location.
- Keep responses short and actionable. Avoid filler."""

        prompt = (google_prompt if service_type == 'google' else microsoft_prompt) + base_instructions

        self.history: List[Dict[str, Any]] = [{"role": "system", "content": prompt}]
        self.last_draft_google: Optional[Dict[str, str]] = None
        self.last_draft_microsoft_id: Optional[str] = None
        self.current_email_context: Optional[Dict[str, str]] = None
        self.current_event_context: Optional[Dict[str, str]] = None

    async def send_audio_response(self, text: str, status_text: str):
        await self.ws.send_json({"type": "chat_append", "role": "assistant", "text": text})
        audio_url = await tts_any(text)
        await self.ws.send_json({"type": "play_audio", "url": audio_url, "status_text": status_text})

    async def append_chat(self, role: str, text: str):
        await self.ws.send_json({"type": "chat_append", "role": role, "text": text})

    async def update_context_display(self):
        ctx = None
        if self.current_event_context:
            ctx = {
                "id": self.current_event_context.get("id"),
                "type": "Calendar Event",
                "organizer": self.current_event_context.get("organizer"),
                "title": self.current_event_context.get("summary"),
            }
        elif self.current_email_context:
            ctx = {
                "id": self.current_email_context.get("id"),
                "type": "Email",
                "from": self.current_email_context.get("from"),
                "subject": self.current_email_context.get("subject"),
            }
        await self.ws.send_json({"type": "context_update", "context": ctx})

    async def show_draft(self, to: str, subject: str, body: str):
        await self.ws.send_json({"type": "draft_preview", "to": to, "subject": subject, "body": body})

    async def clear_draft(self):
        self.last_draft_google = None
        self.last_draft_microsoft_id = None
        await self.ws.send_json({"type": "draft_clear"})

    @property
    def tools(self):
        google_tools = [
            {"type": "function", "function": {"name": "gmail_search_emails", "description": "Searches for emails in the user's Gmail inbox.", "parameters": {"type": "object", "properties": {"query": {"type": "string"}, "max_results": {"type": "integer", "default": 5}}, "required": ["query"]}}},
            {"type": "function", "function": {"name": "gmail_read_email", "description": "Reads a preview of a Gmail email.", "parameters": {"type": "object", "properties": {"message_id": {"type": "string"}}, "required": []}}},
            {"type": "function", "function": {"name": "gmail_summarize_email", "description": "Summarizes the Gmail email in context.", "parameters": {"type": "object", "properties": {}}}},
            {"type": "function", "function": {"name": "gmail_draft_new_email", "description": "Creates a new Gmail draft.", "parameters": {"type": "object", "properties": {"to": {"type": "string"}, "subject": {"type": "string"}, "body": {"type": "string"}}, "required": ["to", "subject", "body"]}}},
            {"type": "function", "function": {"name": "gmail_draft_reply", "description": "Creates a Gmail reply draft.", "parameters": {"type": "object", "properties": {"body": {"type": "string"}}, "required": ["body"]}}},
            {"type": "function", "function": {"name": "gmail_send_draft", "description": "Sends the last Gmail draft.", "parameters": {"type": "object", "properties": {}}}},
            {"type": "function", "function": {"name": "gmail_delete_email", "description": "Deletes a Gmail email.", "parameters": {"type": "object", "properties": {"message_id": {"type": "string"}}, "required": []}}},
            {"type": "function", "function": {"name": "gmail_archive_email", "description": "Archives a Gmail email.", "parameters": {"type": "object", "properties": {"message_id": {"type": "string"}}, "required": []}}},
            {"type": "function", "function": {"name": "gmail_mark_as_read", "description": "Marks a Gmail email as read.", "parameters": {"type": "object", "properties": {"message_id": {"type": "string"}}, "required": []}}},
            {"type": "function", "function": {"name": "gmail_mark_as_unread", "description": "Marks a Gmail email as unread.", "parameters": {"type": "object", "properties": {"message_id": {"type": "string"}}, "required": []}}},
            {"type": "function", "function": {"name": "calendar_list_events", "description": "Lists Google Calendar events.", "parameters": {"type": "object", "properties": {"time_min": {"type": "string"}, "time_max": {"type": "string"}, "max_results": {"type": "integer", "default": 10}, "query": {"type": "string"}}, "required": []}}},
            {"type": "function", "function": {"name": "calendar_quick_add", "description": "Quickly adds a Google Calendar event from text.", "parameters": {"type": "object", "properties": {"text": {"type": "string"}}, "required": ["text"]}}},
            {"type": "function", "function": {"name": "calendar_create_event", "description": "Creates a detailed Google Calendar event.", "parameters": {"type": "object", "properties": {"summary": {"type": "string"}, "start_time": {"type": "string"}, "end_time": {"type": "string"}, "timezone": {"type": "string"}, "location": {"type": "string"}, "attendees": {"type": "array", "items": {"type": "string"}}}, "required": ["summary", "start_time", "end_time"]}}},
            {"type": "function", "function": {"name": "calendar_update_event_time", "description": "Updates a Google Calendar event's time.", "parameters": {"type": "object", "properties": {"event_id": {"type": "string"}, "start_time": {"type": "string"}, "end_time": {"type": "string"}, "timezone": {"type": "string"}}, "required": ["event_id", "start_time", "end_time"]}}},
            {"type": "function", "function": {"name": "calendar_delete_event", "description": "Deletes a Google Calendar event.", "parameters": {"type": "object", "properties": {"event_id": {"type": "string"}}, "required": ["event_id"]}}},
        ]
        microsoft_tools = [
            {"type": "function", "function": {
                "name": "outlook_search_emails",
                "description": "Searches Outlook inbox. If query is empty, returns the latest emails.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Free-text search. Leave empty to fetch latest messages."},
                        "max_results": {"type": "integer", "default": 5}
                    },
                    "required": []
                }
            }},
            {"type": "function", "function": {"name": "outlook_read_email", "description": "Reads a preview of an Outlook email.", "parameters": {"type": "object", "properties": {"message_id": {"type": "string"}}, "required": []}}},
            {"type": "function", "function": {"name": "outlook_summarize_email", "description": "Summarizes the Outlook email in context.", "parameters": {"type": "object", "properties": {}}}},
            {"type": "function", "function": {"name": "outlook_draft_new_email", "description": "Creates a new Outlook draft.", "parameters": {"type": "object", "properties": {"to": {"type": "string"}, "subject": {"type": "string"}, "body": {"type": "string"}}, "required": ["to", "subject", "body"]}}},
            {"type": "function", "function": {"name": "outlook_draft_reply", "description": "Creates an Outlook reply draft.", "parameters": {"type": "object", "properties": {"body": {"type": "string"}}, "required": ["body"]}}},
            {"type": "function", "function": {"name": "outlook_send_draft", "description": "Sends the last Outlook draft.", "parameters": {"type": "object", "properties": {}}}},
            {"type": "function", "function": {"name": "outlook_delete_email", "description": "Deletes an Outlook email.", "parameters": {"type": "object", "properties": {"message_id": {"type": "string"}}, "required": []}}},
            {"type": "function", "function": {"name": "outlook_archive_email", "description": "Archives an Outlook email.", "parameters": {"type": "object", "properties": {"message_id": {"type": "string"}}, "required": []}}},
            {"type": "function", "function": {"name": "outlook_mark_as_read", "description": "Marks an Outlook email as read.", "parameters": {"type": "object", "properties": {"message_id": {"type": "string"}}, "required": []}}},
            {"type": "function", "function": {"name": "outlook_mark_as_unread", "description": "Marks an Outlook email as unread.", "parameters": {"type": "object", "properties": {"message_id": {"type": "string"}}, "required": []}}},
            {"type": "function", "function": {"name": "calendar_list_events", "description": "Lists Microsoft Calendar events.", "parameters": {"type": "object", "properties": {"time_min": {"type": "string"}, "time_max": {"type": "string"}, "max_results": {"type": "integer", "default": 10}, "query": {"type": "string"}}, "required": []}}},
            {"type": "function", "function": {"name": "calendar_create_event", "description": "Creates a detailed Microsoft Calendar event.", "parameters": {"type": "object", "properties": {"summary": {"type": "string"}, "start_time": {"type": "string"}, "end_time": {"type": "string"}, "timezone": {"type": "string"}, "location": {"type": "string"}, "attendees": {"type": "array", "items": {"type": "string"}}}, "required": ["summary", "start_time", "end_time"]}}},
            {"type": "function", "function": {"name": "calendar_update_event_time", "description": "Updates a Microsoft Calendar event's time.", "parameters": {"type": "object", "properties": {"event_id": {"type": "string"}, "start_time": {"type": "string"}, "end_time": {"type": "string"}, "timezone": {"type": "string"}}, "required": ["event_id", "start_time", "end_time"]}}},
            {"type": "function", "function": {"name": "calendar_delete_event", "description": "Deletes a Microsoft Calendar event.", "parameters": {"type": "object", "properties": {"event_id": {"type": "string"}}, "required": ["event_id"]}}},
        ]
        return google_tools if self.service_type == 'google' else microsoft_tools

    # --- GOOGLE TOOL IMPLEMENTATIONS ---
    def _parse_headers(self, headers: List[Dict]) -> Dict[str, str]:
        return {h['name'].lower(): h['value'] for h in headers}

    async def gmail_search_emails(self, query: str, max_results: int = 5) -> str:
        s = _gmail_service()
        results = s.users().messages().list(userId='me', q=query, maxResults=max_results).execute()
        messages = results.get('messages', [])
        if not messages:
            return f"No emails found for '{query}'"
        email_list = []
        for msg in messages:
            meta = s.users().messages().get(userId='me', id=msg['id'], format='metadata', metadataHeaders=['From', 'Subject']).execute()
            headers = self._parse_headers(meta.get('payload', {}).get('headers', []))
            email_list.append({"id": msg['id'], "from": headers.get('from', '...').split('<')[0].strip(), "subject": headers.get('subject', '(No Subject)')})
        return json.dumps(email_list)

    async def gmail_read_email(self, message_id: Optional[str] = None) -> str:
        target_id = message_id or (self.current_email_context and self.current_email_context.get('id'))
        if not target_id:
            return "Error: No message ID."
        msg = _gmail_service().users().messages().get(userId='me', id=target_id, format='full').execute()
        headers = self._parse_headers(msg.get('payload', {}).get('headers', []))
        self.current_email_context = {
            'id': msg['id'],
            'threadId': msg['threadId'],
            'from': headers.get('from', ''),
            'subject': headers.get('subject', ''),
            'message-id': headers.get('message-id', ''),
            'references': headers.get('references', ''),
        }
        self.current_event_context = None
        await self.update_context_display()
        return json.dumps({"from": self.current_email_context['from'], "subject": self.current_email_context['subject'], "body_preview": _get_email_body(msg)[:800]})

    async def gmail_summarize_email(self) -> str:
        if not self.current_email_context:
            return "Error: No email in context."
        msg = _gmail_service().users().messages().get(userId='me', id=self.current_email_context['id'], format='full').execute()
        prompt = "Summarize this email concisely, focusing on key points and action items:\n\n" + _get_email_body(msg)
        payload = {"model": REALTIME_MODEL, "messages": [{"role": "user", "content": prompt}], "temperature": 0.2}
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
        r = await _client().post(f"{OPENAI_BASE_URL.rstrip('/')}/v1/chat/completions", json=payload, headers=headers)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]

    async def gmail_draft_new_email(self, to: str, subject: str, body: str) -> str:
        self.current_email_context = None
        await self.update_context_display()
        self.last_draft_google = {"to": to, "subject": subject, "body": body}
        await self.show_draft(to, subject, body)
        return "Draft created. Ask user to confirm."

    async def gmail_draft_reply(self, body: str) -> str:
        if not self.current_email_context:
            return "Error: No email context to reply to."
        subject = self.current_email_context.get('subject', '')
        if not subject.lower().startswith("re:"):
            subject = f"Re: {subject}"
        self.last_draft_google = {"to": self.current_email_context['from'], "subject": subject, "body": body}
        await self.show_draft(self.current_email_context['from'], subject, body)
        return "Reply draft created."

    async def gmail_send_draft(self) -> str:
        if not self.last_draft_google:
            return "Error: No draft to send."
        s = _gmail_service()
        profile = s.users().getProfile(userId='me').execute()
        message = EmailMessage()
        message.set_content(self.last_draft_google['body'])
        message['To'] = self.last_draft_google['to']
        message['From'] = profile['emailAddress']
        message['Subject'] = self.last_draft_google['subject']
        if self.current_email_context and self.current_email_context.get('message-id'):
            message['In-Reply-To'] = self.current_email_context['message-id']
            refs = self.current_email_context.get('references', '').strip()
            message['References'] = (refs + " " if refs else "") + self.current_email_context['message-id']
            body = {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode(), 'threadId': self.current_email_context['threadId']}
        else:
            body = {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}
        s.users().messages().send(userId='me', body=body).execute()
        if self.current_email_context:
            await self.gmail_mark_as_read(self.current_email_context['id'])
        await self.clear_draft()
        self.current_email_context = None
        await self.update_context_display()
        return "Email sent."

    async def _gmail_context_action(self, message_id: Optional[str], action_func: callable, success_msg: str, clear_ctx: bool = True) -> str:
        target_id = message_id or (self.current_email_context and self.current_email_context.get('id'))
        if not target_id:
            return "Error: No message ID."
        action_func(userId='me', id=target_id).execute()
        if clear_ctx and self.current_email_context and self.current_email_context.get('id') == target_id:
            self.current_email_context = None
            await self.update_context_display()
        return success_msg

    async def gmail_delete_email(self, message_id: Optional[str] = None) -> str:
        return await self._gmail_context_action(message_id, _gmail_service().users().messages().trash, "Email deleted.")

    async def gmail_archive_email(self, message_id: Optional[str] = None) -> str:
        return await self._gmail_context_action(message_id, lambda **kwargs: _gmail_service().users().messages().modify(**kwargs, body={'removeLabelIds': ['INBOX']}), "Email archived.")

    async def gmail_mark_as_read(self, message_id: Optional[str] = None) -> str:
        return await self._gmail_context_action(message_id, lambda **kwargs: _gmail_service().users().messages().modify(**kwargs, body={'removeLabelIds': ['UNREAD']}), "Email marked as read.", clear_ctx=False)

    async def gmail_mark_as_unread(self, message_id: Optional[str] = None) -> str:
        return await self._gmail_context_action(message_id, lambda **kwargs: _gmail_service().users().messages().modify(**kwargs, body={'addLabelIds': ['UNREAD']}), "Email marked as unread.", clear_ctx=False)

    # --- MICROSOFT TOOL IMPLEMENTATIONS ---
    async def outlook_search_emails(self, query: str = "", max_results: int = 5) -> str:
        """
        If query is empty -> fetch latest messages (no $search).
        If query is provided -> use $search (ConsistencyLevel header already added in graph_request()).
        """
        if not query or not query.strip():
            params = {
                "$orderby": "receivedDateTime desc",
                "$top": max_results,
                "$select": "id,subject,from,receivedDateTime"
            }
            r = await graph_request("GET", "/me/messages", params=params)
        else:
            params = {
                "$search": f'"{query}"',
                "$top": max_results,
                "$select": "id,subject,from,receivedDateTime"
            }
            r = await graph_request("GET", "/me/messages", params=params)

        messages = r.json().get("value", [])
        if not messages:
            return "No emails found." if not query.strip() else f"No emails found for '{query}'"

        out = []
        for m in messages:
            sender = (m.get('from', {}) or {}).get('emailAddress', {}) or {}
            out.append({
                "id": m.get('id'),
                "from": sender.get('name') or sender.get('address') or "...",
                "subject": m.get('subject') or "(No Subject)",
                "received": m.get('receivedDateTime', "")
            })
        return json.dumps(out)

    async def outlook_read_email(self, message_id: Optional[str] = None) -> str:
        target_id = message_id or (self.current_email_context and self.current_email_context.get('id'))
        if not target_id:
            return "Error: No message ID."
        r = await graph_request(
            "GET",
            f"/me/messages/{target_id}?$select=id,subject,from,toRecipients,bodyPreview,body",
            headers={"Prefer": 'outlook.body-content-type="text"'}
        )
        msg = r.json()
        self.current_email_context = {
            'id': msg['id'],
            'from': (msg.get('from', {}) or {}).get('emailAddress', {}).get('address', ''),
            'subject': msg.get('subject', '')
        }
        self.current_event_context = None
        await self.update_context_display()
        body_preview = (msg.get('body', {}) or {}).get('content', '') or msg.get('bodyPreview', '')
        return json.dumps({"from": self.current_email_context['from'], "subject": self.current_email_context['subject'], "body_preview": body_preview[:800]})

    async def outlook_summarize_email(self) -> str:
        if not self.current_email_context:
            return "Error: No email in context."
        r = await graph_request("GET", f"/me/messages/{self.current_email_context['id']}?$select=body", headers={"Prefer": 'outlook.body-content-type="text"'})
        prompt = "Summarize this email concisely:\n\n" + ((r.json().get('body', {}) or {}).get('content', '') or '')
        payload = {"model": REALTIME_MODEL, "messages": [{"role": "user", "content": prompt}], "temperature": 0.2}
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
        resp = await _client().post(f"{OPENAI_BASE_URL.rstrip('/')}/v1/chat/completions", json=payload, headers=headers)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]

    async def outlook_draft_new_email(self, to: str, subject: str, body: str) -> str:
        self.current_email_context = None
        await self.update_context_display()
        message = {
            "subject": subject,
            "body": {"contentType": "Text", "content": body},
            "toRecipients": [{"emailAddress": {"address": addr.strip()}} for addr in to.split(',')]
        }
        r = await graph_request("POST", "/me/messages", json=message)
        self.last_draft_microsoft_id = r.json().get("id")
        await self.show_draft(to, subject, body)
        return "Draft created. Ask user to confirm."

    async def outlook_draft_reply(self, body: str) -> str:
        if not self.current_email_context:
            return "Error: No email context to reply to."
        reply_payload = {"comment": body}
        r = await graph_request("POST", f"/me/messages/{self.current_email_context['id']}/createReply", json=reply_payload)
        draft = r.json()
        self.last_draft_microsoft_id = draft.get('id')
        to_str = ", ".join([r.get('emailAddress',{}).get('address','') for r in draft.get('toRecipients', [])])
        await self.show_draft(to_str, draft.get('subject'), body)
        return "Reply draft created."

    async def outlook_send_draft(self) -> str:
        if not self.last_draft_microsoft_id:
            return "Error: No draft to send."
        await graph_request("POST", f"/me/messages/{self.last_draft_microsoft_id}/send")
        await self.clear_draft()
        self.current_email_context = None
        await self.update_context_display()
        return "Email sent."

    async def outlook_delete_email(self, message_id: Optional[str] = None) -> str:
        target_id = message_id or (self.current_email_context and self.current_email_context.get('id'))
        if not target_id:
            return "Error: No message ID."
        await graph_request("DELETE", f"/me/messages/{target_id}")
        if self.current_email_context and self.current_email_context.get('id') == target_id:
            self.current_email_context = None
            await self.update_context_display()
        return "Email deleted."

    async def outlook_archive_email(self, message_id: Optional[str] = None) -> str:
        target_id = message_id or (self.current_email_context and self.current_email_context.get('id'))
        if not target_id:
            return "Error: No message ID."
        r = await graph_request("GET", "/me/mailFolders?$filter=wellKnownName eq 'archive'")
        folders = r.json().get("value", [])
        if not folders:
            return "Error: Could not find Archive folder."
        await graph_request("POST", f"/me/messages/{target_id}/move", json={"destinationId": folders[0]['id']})
        if self.current_email_context and self.current_email_context.get('id') == target_id:
            self.current_email_context = None
            await self.update_context_display()
        return "Email archived."

    async def outlook_mark_as_read(self, message_id: Optional[str] = None) -> str:
        target_id = message_id or (self.current_email_context and self.current_email_context.get('id'))
        if not target_id:
            return "Error: No message ID."
        await graph_request("PATCH", f"/me/messages/{target_id}", json={"isRead": True})
        return "Email marked as read."

    async def outlook_mark_as_unread(self, message_id: Optional[str] = None) -> str:
        target_id = message_id or (self.current_email_context and self.current_email_context.get('id'))
        if not target_id:
            return "Error: No message ID."
        await graph_request("PATCH", f"/me/messages/{target_id}", json={"isRead": False})
        return "Email marked as unread."

    # --- UNIFIED CALENDAR TOOL IMPLEMENTATIONS ---
    async def calendar_list_events(self, time_min: Optional[str] = None, time_max: Optional[str] = None, max_results: int = 10, query: Optional[str] = None) -> str:
        now_utc = datetime.datetime.utcnow()
        start_dt = time_min or now_utc.isoformat()
        end_dt = time_max or (now_utc + datetime.timedelta(days=7)).isoformat()

        if self.service_type == 'google':
            s = _calendar_service()
            events_result = s.events().list(
                calendarId='primary',
                timeMin=start_dt + "Z",
                timeMax=end_dt + "Z",
                maxResults=max_results,
                q=query or None,
                singleEvents=True,
                orderBy='startTime'
            ).execute()
            items = events_result.get('items', [])
            if not items:
                return "No upcoming events found."
            out = [{
                "id": ev.get('id'),
                "summary": ev.get('summary', '(No title)'),
                "start": ev.get('start', {}).get('dateTime'),
                "end": ev.get('end', {}).get('dateTime'),
                "location": ev.get('location', '')
            } for ev in items]
            return json.dumps(out)
        else:
            params = {"startDateTime": start_dt, "endDateTime": end_dt, "$top": max_results, "$orderby": "start/dateTime"}
            if query:
                params["$filter"] = f"contains(subject,'{query}')"
            r = await graph_request("GET", "/me/calendarView", params=params)
            items = r.json().get("value", [])
            if not items:
                return "No upcoming events found."
            out = [{
                "id": ev.get('id'),
                "summary": ev.get('subject', '(No title)'),
                "start": ev.get('start', {}).get('dateTime'),
                "end": ev.get('end', {}).get('dateTime'),
                "location": ev.get('location', {}).get('displayName', '')
            } for ev in items]
            return json.dumps(out)

    async def calendar_quick_add(self, text: str) -> str:
        if self.service_type != 'google':
            return "Quick add is only available for Google Calendar."
        ev = _calendar_service().events().quickAdd(calendarId='primary', text=text).execute()
        return f"Event created: {ev.get('summary', '(No title)')}"

    async def calendar_create_event(self, summary: str, start_time: str, end_time: str, timezone: Optional[str] = None, location: Optional[str] = None, attendees: Optional[List[str]] = None) -> str:
        start_rfc, end_rfc = _parse_rfc3339(start_time), _parse_rfc3339(end_time)
        if self.service_type == 'google':
            body = {"summary": summary, "start": {"dateTime": start_rfc}, "end": {"dateTime": end_rfc}}
            if timezone:
                body["start"]["timeZone"] = timezone; body["end"]["timeZone"] = timezone
            if location:
                body["location"] = location
            if attendees:
                body["attendees"] = [{"email": e} for e in attendees]
            ev = _calendar_service().events().insert(calendarId='primary', body=body, sendUpdates="all").execute()
            return f"Event created: {ev.get('summary', summary)}."
        else:
            body = {
                "subject": summary,
                "start": {"dateTime": start_rfc, "timeZone": timezone or "UTC"},
                "end": {"dateTime": end_rfc, "timeZone": timezone or "UTC"}
            }
            if location:
                body["location"] = {"displayName": location}
            if attendees:
                body["attendees"] = [{"emailAddress": {"address": e}, "type": "required"} for e in attendees]
            ev = (await graph_request("POST", "/me/events", json=body)).json()
            return f"Event created: {ev.get('subject', summary)}."

    async def calendar_update_event_time(self, event_id: str, start_time: str, end_time: str, timezone: Optional[str] = None) -> str:
        start_rfc, end_rfc = _parse_rfc3339(start_time), _parse_rfc3339(end_time)
        if self.service_type == 'google':
            s = _calendar_service()
            ev = s.events().get(calendarId='primary', eventId=event_id).execute()
            ev['start']['dateTime'], ev['end']['dateTime'] = start_rfc, end_rfc
            if timezone:
                ev['start']['timeZone'] = timezone; ev['end']['timeZone'] = timezone
            ev_updated = s.events().update(calendarId='primary', eventId=event_id, body=ev, sendUpdates="all").execute()
            return f"Event time updated for '{ev_updated.get('summary', '')}'."
        else:
            body = {"start": {"dateTime": start_rfc, "timeZone": timezone or "UTC"}, "end": {"dateTime": end_rfc, "timeZone": timezone or "UTC"}}
            ev_updated = (await graph_request("PATCH", f"/me/events/{event_id}", json=body)).json()
            return f"Event time updated for '{ev_updated.get('subject', '')}'."

    async def calendar_delete_event(self, event_id: str) -> str:
        if self.service_type == 'google':
            _calendar_service().events().delete(calendarId='primary', eventId=event_id, sendUpdates="all").execute()
        else:
            await graph_request("DELETE", f"/me/events/{event_id}")
        if self.current_event_context and self.current_event_context.get("id") == event_id:
            self.current_event_context = None
            await self.update_context_display()
        return "Event deleted."

    # --- AGENTIC CORE ---
    async def start(self):
        service_name = "Google" if self.service_type == 'google' else "Outlook"
        initial_greeting = f"Hello! I'm your {service_name} assistant. How can I help?"
        self.history.append({"role": "assistant", "content": initial_greeting})
        await self.send_audio_response(initial_greeting, "Ready for your command...")

    async def process_user_message(self, transcript: str):
        await self.append_chat("user", transcript)
        self.history.append({"role": "user", "content": transcript})
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as openai_client:
                payload = {"model": REALTIME_MODEL, "messages": self.history, "tools": self.tools, "tool_choice": "auto"}
                headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
                r = await openai_client.post(f"{OPENAI_BASE_URL.rstrip('/')}/v1/chat/completions", json=payload, headers=headers)
                r.raise_for_status()
                response_message = r.json()["choices"][0]["message"]
                self.history.append(response_message)
                if response_message.get("tool_calls"):
                    await self.execute_tool_calls(response_message["tool_calls"])
                else:
                    await self.send_audio_response(response_message.get("content", ""), "Tap the mic to reply...")
        except Exception:
            print(f"[AGENT ERROR] {traceback.format_exc()}")
            await self.send_audio_response("I hit an error. Please try again.", "Error")

    async def execute_tool_calls(self, tool_calls: List[Dict]):
        tool_functions = {
            # Gmail
            "gmail_search_emails": self.gmail_search_emails,
            "gmail_read_email": self.gmail_read_email,
            "gmail_summarize_email": self.gmail_summarize_email,
            "gmail_draft_new_email": self.gmail_draft_new_email,
            "gmail_draft_reply": self.gmail_draft_reply,
            "gmail_send_draft": self.gmail_send_draft,
            "gmail_delete_email": self.gmail_delete_email,
            "gmail_archive_email": self.gmail_archive_email,
            "gmail_mark_as_read": self.gmail_mark_as_read,
            "gmail_mark_as_unread": self.gmail_mark_as_unread,
            # Outlook
            "outlook_search_emails": self.outlook_search_emails,
            "outlook_read_email": self.outlook_read_email,
            "outlook_summarize_email": self.outlook_summarize_email,
            "outlook_draft_new_email": self.outlook_draft_new_email,
            "outlook_draft_reply": self.outlook_draft_reply,
            "outlook_send_draft": self.outlook_send_draft,
            "outlook_delete_email": self.outlook_delete_email,
            "outlook_archive_email": self.outlook_archive_email,
            "outlook_mark_as_read": self.outlook_mark_as_read,
            "outlook_mark_as_unread": self.outlook_mark_as_unread,
            # Calendar (unified)
            "calendar_list_events": self.calendar_list_events,
            "calendar_quick_add": self.calendar_quick_add,
            "calendar_create_event": self.calendar_create_event,
            "calendar_update_event_time": self.calendar_update_event_time,
            "calendar_delete_event": self.calendar_delete_event,
        }
        for tool_call in tool_calls:
            function_name = tool_call['function']['name']
            function_args = json.loads(tool_call['function']['arguments'] or "{}")
            await self.append_chat("system", f"Tool: {function_name}({json.dumps(function_args)})")
            try:
                function_response = await tool_functions[function_name](**function_args)
            except Exception:
                function_response = f"Error executing tool: {traceback.format_exc().splitlines()[-1]}"
            self.history.append({"tool_call_id": tool_call['id'], "role": "tool", "name": function_name, "content": function_response})

        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as openai_client:
            payload = {"model": REALTIME_MODEL, "messages": self.history}
            headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
            r = await openai_client.post(f"{OPENAI_BASE_URL.rstrip('/')}/v1/chat/completions", json=payload, headers=headers)
            r.raise_for_status()
            final_response = r.json()["choices"][0]["message"]
            self.history.append(final_response)
            await self.send_audio_response(final_response.get("content", ""), "Tap the mic to reply...")

    async def handle_ws_packet(self, data: Dict[str, Any]):
        action = (data.get("action") or "").lower()
        if action == "send_draft":
            await self.process_user_message("Yes, send the draft.")
        elif action == "cancel_draft":
            await self.clear_draft()
            await self.process_user_message("Cancel the draft.")

# ======================= FastAPI Endpoints =======================

@app.get("/", response_class=HTMLResponse)
async def home():
    return HTMLResponse(CONVERSATIONAL_HTML)

@app.get("/audio/{audio_id}")
async def get_audio(audio_id: str, range: Optional[str] = Header(None)):
    audio_data = _GENERATED_AUDIO.get(audio_id)
    if not audio_data:
        return PlainTextResponse("Not Found", status_code=404)
    file_size = len(audio_data)
    headers = {"Content-Type": "audio/mpeg", "Accept-Ranges": "bytes", "Cache-Control": "no-store"}
    if range is None:
        headers["Content-Length"] = str(file_size)
        return Response(content=audio_data, headers=headers, status_code=200)
    match = re.search(r"bytes=(\d+)-(\d*)", range)
    if not match:
        return PlainTextResponse("Invalid Range header", status_code=416)
    start, end = int(match.group(1)), int(match.group(2)) if match.group(2) else file_size - 1
    if start >= file_size or start > end:
        return PlainTextResponse("Range not satisfiable", status_code=416)
    content = audio_data[start:end + 1]
    headers["Content-Length"] = str(len(content))
    headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
    return Response(content=content, headers=headers, status_code=206)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    service = 'none'
    if _GMAIL_CREDS and _GMAIL_CREDS.valid:
        service = 'google'
    elif _MSAL_TOKEN and _MSAL_TOKEN.get("access_token"):
        try:
            _require_ms_token()
            service = 'microsoft'
        except RuntimeError:
            pass

    if service == 'none':
        await websocket.close(code=1008, reason="No service connected")
        return

    manager = ConversationManager(websocket, service_type=service)
    await manager.start()
    try:
        while True:
            packet = await websocket.receive()
            if packet.get("type") == "websocket.disconnect":
                break
            if packet.get("bytes"):
                transcript = ""
                try:
                    transcript = await transcribe_bytes(packet["bytes"])
                except Exception as e:
                    print(f"[STT ERROR] {e}")
                if not transcript:
                    await manager.send_audio_response("Sorry, I didn't catch that.", "Didn't hear you...")
                    continue
                await manager.process_user_message(transcript)
            elif packet.get("text"):
                try:
                    await manager.handle_ws_packet(json.loads(packet["text"]))
                except Exception:
                    continue
    except WebSocketDisconnect:
        print("Client disconnected.")

# --- AUTH ENDPOINTS ---
@app.get("/api/status")
def get_auth_status():
    if _GMAIL_CREDS and _GMAIL_CREDS.valid:
        return {"connected_service": "google"}
    if _MSAL_TOKEN and _MSAL_TOKEN.get("access_token"):
        return {"connected_service": "microsoft"}
    return {"connected_service": "none"}

# Google OAuth
@app.get("/gmail/login")
def gmail_login(request: Request):
    cfg = {"web": {"client_id": GOOGLE_CLIENT_ID, "client_secret": GOOGLE_CLIENT_SECRET, "redirect_uris": [GOOGLE_REDIRECT_URI], "auth_uri": "https://accounts.google.com/o/oauth2/auth", "token_uri": "https://oauth2.googleapis.com/token"}}
    flow = Flow.from_client_config(cfg, scopes=GOOGLE_SCOPES, redirect_uri=GOOGLE_REDIRECT_URI)
    auth_url, state = flow.authorization_url(access_type="offline", include_granted_scopes="true", prompt="consent")
    request.session["google_oauth_state"] = state
    return RedirectResponse(auth_url)

@app.get("/gmail/oauth2callback")
async def gmail_oauth2callback(request: Request, code: str, state: str):
    if state != request.session.get("google_oauth_state"):
        return PlainTextResponse("Invalid state", 400)
    cfg = {"web": {"client_id": GOOGLE_CLIENT_ID, "client_secret": GOOGLE_CLIENT_SECRET, "redirect_uris": [GOOGLE_REDIRECT_URI]}}
    flow = Flow.from_client_config(cfg, scopes=GOOGLE_SCOPES, state=state, redirect_uri=GOOGLE_REDIRECT_URI)
    flow.fetch_token(code=code)
    global _GMAIL_CREDS, _MSAL_TOKEN
    _GMAIL_CREDS = flow.credentials
    _MSAL_TOKEN = None
    return RedirectResponse("/")

# Microsoft OAuth
@app.get("/outlook/login")
def outlook_login(request: Request):
    flow = _get_msal_app().initiate_auth_code_flow(MS_SCOPES, redirect_uri=MS_REDIRECT_URI)
    request.session["msal_flow"] = flow
    return RedirectResponse(flow["auth_uri"])

@app.get("/outlook/callback")
async def outlook_callback(request: Request):
    flow = request.session.pop("msal_flow", {})
    if not request.query_params.get("code") or not flow:
        return PlainTextResponse("Auth failed", 400)
    result = _get_msal_app().acquire_token_by_auth_code_flow(flow, dict(request.query_params))
    if "error" in result:
        return PlainTextResponse(f"Auth Error: {result.get('error_description')}", 400)
    global _MSAL_TOKEN, _GMAIL_CREDS
    _MSAL_TOKEN = result
    _GMAIL_CREDS = None
    return RedirectResponse("/")
