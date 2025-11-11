# app.py
"""
Conversational Email + Calendar Voice Assistant (Unified Google & Microsoft) - v6.5.2 (Graph API Fix + send/read tweaks)
- Voice-first assistant for EITHER Gmail/Google Calendar OR Outlook/Microsoft Calendar.
- User chooses which service to connect on the main page.
- Proactive startup: On connect, summarizes new emails (from today/yesterday) and today's events.
- Auto-starts conversation on page load if user is authenticated.
- Uses tool-calling to search/read/summarize/compose emails AND list/create/update/delete meetings.
- Dynamic backend selects the correct tools and prompts based on the active authentication.
- Audio interrupt, WebSockets for audio + JSON, TTS + Whisper transcription.

v6.5.1: Attempted to patch Microsoft Graph API filter syntax.
v6.5.2: Correctly patched Graph API date filter to use unquoted UTC ISO string with 'Z' suffix.
v6.5.2a: Outlook read now marks mail as read, sturdier drafts, recipient sanitizer, stronger OpenAI error logging.

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
from typing import Optional, List, Dict, Any, Tuple, Set
from datetime import timezone

import httpx
import msal
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Header, Response
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from dotenv import load_dotenv
from starlette.middleware.sessions import SessionMiddleware

# Google / Gmail / Calendar
from email.message import EmailMessage
from email.utils import parseaddr, getaddresses
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

SUGGESTION_BLOCK_RE = re.compile(r"<suggestions>(.*?)</suggestions>", re.DOTALL)

def _extract_suggestions(message: str) -> Tuple[str, List[Dict[str, str]]]:
    suggestions: List[Dict[str, str]] = []
    if not message:
        return "", suggestions

    for raw in SUGGESTION_BLOCK_RE.findall(message):
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for item in payload.get("items", []):
            label = (item.get("label") or item.get("title") or "").strip()
            prompt = (item.get("prompt") or item.get("text") or label).strip()
            if label and prompt:
                suggestions.append({"label": label, "prompt": prompt})

    cleaned_message = SUGGESTION_BLOCK_RE.sub("", message).strip()
    return cleaned_message, suggestions

def _identity_from_header(value: Optional[str]) -> Dict[str, str]:
    name, email = parseaddr(value or "")
    name = (name or "").strip()
    email = (email or "").strip()
    if name and email:
        display = f"{name} <{email}>"
    elif email:
        display = email
    else:
        display = name
    return {"name": name, "email": email, "display": display or (value or "").strip()}

def _identities_from_header(value: Optional[str]) -> List[Dict[str, str]]:
    if not value:
        return []
    identities: List[Dict[str, str]] = []
    for name, email in getaddresses([value]):
        name = (name or "").strip()
        email = (email or "").strip()
        if not name and not email:
            continue
        if name and email:
            display = f"{name} <{email}>"
        elif email:
            display = email
        else:
            display = name
        identities.append({"name": name, "email": email, "display": display})
    return identities

def _identity_from_graph(email_address: Optional[Dict[str, Any]]) -> Dict[str, str]:
    email_address = email_address or {}
    name = (email_address.get("name") or "").strip()
    email = (email_address.get("address") or "").strip()
    if name and email:
        display = f"{name} <{email}>"
    elif email:
        display = email
    else:
        display = name
    return {"name": name, "email": email, "display": display}

def _identities_from_graph(entries: Optional[List[Dict[str, Any]]]) -> List[Dict[str, str]]:
    identities: List[Dict[str, str]] = []
    for entry in entries or []:
        identity = _identity_from_graph(entry.get("emailAddress"))
        if identity.get("name") or identity.get("email") or identity.get("display"):
            identities.append(identity)
    return identities

def _join_identity_displays(identities: List[Dict[str, str]]) -> str:
    parts: List[str] = []
    for ident in identities:
        name = ident.get("name", "").strip()
        email = ident.get("email", "").strip()
        display = ident.get("display", "").strip()
        if name and email:
            parts.append(f"{name} <{email}>")
        elif email:
            parts.append(email)
        elif display:
            parts.append(display)
    return ", ".join(parts)

def _tool_status_message(name: str, args: Dict[str, Any], service: str) -> Optional[str]:
    name = name or ""
    query = (args.get("query") or "").strip()
    mailbox = "Gmail" if service == "google" else "Outlook"
    calendar_service = "Google Calendar" if service == "google" else "Microsoft Calendar"
    if name in {"gmail_search_emails", "outlook_search_emails"}:
        return f"Searching your {mailbox} inbox{' for ' + query if query else ''}..."
    if name in {"gmail_read_email", "outlook_read_email"}:
        return "Opening that message..."
    if name in {"gmail_summarize_email", "outlook_summarize_email"}:
        return "Summarizing that message for you..."
    if name in {"gmail_draft_new_email", "outlook_draft_new_email"}:
        return "Drafting that email..."
    if name in {"gmail_draft_reply", "outlook_draft_reply"}:
        return "Writing your reply..."
    if name in {"gmail_send_draft", "outlook_send_draft"}:
        return "Sending that email..."
    if name in {"gmail_delete_email", "outlook_delete_email"}:
        return "Deleting that email..."
    if name in {"gmail_archive_email", "outlook_archive_email"}:
        return "Archiving that email..."
    if name in {"gmail_mark_as_read", "outlook_mark_as_read"}:
        return "Marking that email as read..."
    if name in {"gmail_mark_as_unread", "outlook_mark_as_unread"}:
        return "Marking that email as unread..."
    if name == "calendar_list_events":
        return f"Reviewing your upcoming {calendar_service} schedule..."
    if name == "calendar_quick_add":
        return "Scheduling that event..."
    if name == "calendar_create_event":
        return f"Putting that event on your {calendar_service}..."
    if name == "calendar_update_event_time":
        return "Updating that event's timing..."
    if name == "calendar_delete_event":
        return f"Removing that event from your {calendar_service}..."
    return None

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
  .app-container { display:flex; flex-direction:column; height:100%; max-width:1120px; width:100%; margin:0 auto; background:var(--bg); }
  header { padding:12px 16px; border-bottom:1px solid var(--border); display:flex; justify-content:space-between; align-items:center; flex-shrink:0; background: var(--card); }
  header h1 { font-size:18px; margin:0; }
  .badge { background:var(--chip); color:var(--chip-ink); border-radius:12px; padding:3px 10px; font-size:12px; font-weight:500; }
  .main-layout { flex:1; display:flex; gap:16px; padding:16px; overflow:hidden; }
  .people-panel { width:260px; background:var(--card); border:1px solid var(--border); border-radius:16px; padding:16px; display:flex; flex-direction:column; gap:12px; transition:opacity .2s; }
  .people-panel.hidden { display:none; }
  .people-header { font-size:13px; letter-spacing:0.08em; text-transform:uppercase; color:var(--muted); font-weight:600; }
  .people-list { flex:1; overflow-y:auto; display:flex; flex-direction:column; gap:10px; padding-right:4px; }
  .person-card { border:1px solid var(--border); border-radius:12px; padding:12px; background:rgba(31,41,55,0.65); cursor:pointer; text-align:left; transition:background-color .2s, border-color .2s, transform .2s; display:flex; flex-direction:column; gap:6px; }
  .person-card:hover, .person-card:focus { border-color:var(--brand); background:rgba(99,102,241,0.12); transform:translateY(-1px); }
  .person-card:focus { outline:2px solid var(--brand); outline-offset:2px; }
  .person-name { font-size:15px; font-weight:600; color:var(--ink); }
  .person-email { font-size:12px; color:var(--muted); }
  .person-meta { font-size:12px; color:var(--muted); display:flex; flex-direction:column; gap:4px; }
  .person-details { display:none; font-size:12px; line-height:1.4; color:var(--ink); }
  .person-meta-line { font-size:12px; color:var(--muted); }
  .person-preview { margin-top:6px; font-size:12px; color:var(--ink); line-height:1.5; }
  .person-card.expanded .person-details { display:block; }
  .person-subject { font-weight:500; }
  .people-empty { font-size:12px; color:var(--muted); text-align:center; padding:12px; border:1px dashed var(--border); border-radius:10px; }
  .chat-container { flex:1 1 auto; overflow-y:auto; padding:16px; display:flex; flex-direction:column; gap:12px; background:rgba(17,24,39,0.65); border:1px solid var(--border); border-radius:16px; }
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
  .manual-input { margin-top:12px; display:flex; gap:8px; }
  .manual-input input { flex:1; background:var(--card); border:1px solid var(--border); border-radius:10px; padding:10px 12px; color:var(--ink); font-size:14px; }
  .manual-input input:disabled { opacity:0.6; cursor:not-allowed; }
  .manual-input .btn { flex-shrink:0; }
  .suggestions { display:flex; flex-wrap:wrap; gap:8px; padding:12px 16px 0 16px; }
  .suggestions.hidden { display:none; }
  .suggestion-chip { border-radius:999px; border:1px solid var(--brand); padding:8px 14px; background:var(--chip); color:var(--chip-ink); font-size:13px; cursor:pointer; transition:background-color .2s, color .2s; }
  .suggestion-chip:hover { background:var(--brand-hover); color:#fff; }
  .auth-view { padding: 24px; text-align:center; }
  .auth-view h2 { margin-top:0; }
  .auth-buttons { display: flex; justify-content: center; gap: 16px; margin-top: 16px; }
  @media (max-width: 1024px) {
    .main-layout { flex-direction:column; }
    .people-panel { width:100%; order:2; }
  }
  @media (max-width: 640px) {
    header { flex-direction:column; gap:8px; align-items:flex-start; }
    .chat-container { padding:12px; }
  }
</style>
</head><body>
<div id="app-container" class="app-container">
  <header>
    <h1><span id="service-name">Email</span> Assistant</h1><span class="badge">Voice AI</span>
  </header>
  <div class="main-layout">
    <aside id="people-panel" class="people-panel hidden">
      <div class="people-header">Latest Senders</div>
      <div id="people-list" class="people-list">
        <div class="people-empty">No recent senders yet.</div>
      </div>
    </aside>
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
      <div id="suggestions-wrap" class="suggestions hidden"></div>
    </div>
  </div>
  <div id="controls" class="controls-bar" style="display:none;">
    <button id="mic-btn" onclick="handleMicClick()" disabled>
      <span id="mic-icon-container"></span>
    </button>
    <div id="status-text">Checking connection...</div>
    <div class="manual-input">
      <input id="text-input" type="text" placeholder="Type a message..." autocomplete="off"/>
      <button id="send-btn" class="btn secondary" onclick="sendManualMessage()">Send</button>
    </div>
  </div>
</div>
<audio id="audio-player" style="display:none;"></audio>
<script>
const AppState = { IDLE: 'IDLE', LISTENING: 'LISTENING', PROCESSING: 'PROCESSING', SPEAKING: 'SPEAKING' };
let state = AppState.IDLE; let socket; let mediaRecorder; let audioChunks = [];
const chatLog = document.getElementById('chat-log'); const chatContainer = document.getElementById('chat-container'); const micBtn = document.getElementById('mic-btn'); const micIconContainer = document.getElementById('mic-icon-container'); const statusText = document.getElementById('status-text'); const audioPlayer = document.getElementById('audio-player');
const suggestionsWrap = document.getElementById('suggestions-wrap'); const textInput = document.getElementById('text-input'); const sendBtn = document.getElementById('send-btn'); const peoplePanel = document.getElementById('people-panel'); const peopleList = document.getElementById('people-list');
const ICONS = { mic: `<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/><line x1="12" y1="19" x2="12" y2="22"/></svg>`, stop: `<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"></rect></svg>`};

function renderPeopleList(items){
  if (!peoplePanel || !peopleList) return;
  peopleList.innerHTML = '';
  if (!items || !items.length) {
    peoplePanel.classList.add('hidden');
    const empty = document.createElement('div');
    empty.className = 'people-empty';
    empty.textContent = 'No recent senders yet.';
    peopleList.appendChild(empty);
    return;
  }
  peoplePanel.classList.remove('hidden');
  items.slice(0, 12).forEach((person) => {
    const name = person && (person.name || person.display || person.email) || 'Unknown Sender';
    const email = person && person.email || '';
    const subject = person && person.subject || '';
    const received = person && person.received || '';
    const preview = person && person.preview || '';
    const service = person && person.service || '';

    const card = document.createElement('button');
    card.type = 'button';
    card.className = 'person-card';
    card.setAttribute('aria-expanded', 'false');
    const nameSpan = document.createElement('span');
    nameSpan.className = 'person-name';
    nameSpan.textContent = name;
    card.appendChild(nameSpan);
    if (email) {
      const emailSpan = document.createElement('span');
      emailSpan.className = 'person-email';
      emailSpan.textContent = email;
      card.appendChild(emailSpan);
    }
    const details = document.createElement('div');
    details.className = 'person-details';
    if (subject) {
      const subjectSpan = document.createElement('div');
      subjectSpan.className = 'person-subject';
      subjectSpan.textContent = subject;
      details.appendChild(subjectSpan);
    }
    const meta = document.createElement('div');
    meta.className = 'person-meta';
    if (received) {
      const receivedRow = document.createElement('div');
      receivedRow.className = 'person-meta-line';
      const strong = document.createElement('strong');
      strong.textContent = 'Last message';
      receivedRow.appendChild(strong);
      receivedRow.appendChild(document.createTextNode(` ${received}`));
      meta.appendChild(receivedRow);
    }
    if (service) {
      const svcRow = document.createElement('div');
      svcRow.className = 'person-meta-line';
      const strong = document.createElement('strong');
      strong.textContent = 'Account';
      svcRow.appendChild(strong);
      svcRow.appendChild(document.createTextNode(` ${service === 'google' ? 'Gmail' : 'Outlook'}`));
      meta.appendChild(svcRow);
    }
    if (meta.children.length) {
      details.appendChild(meta);
    }
    if (preview) {
      const previewDiv = document.createElement('div');
      previewDiv.className = 'person-preview';
      previewDiv.textContent = preview;
      details.appendChild(previewDiv);
    }
    card.appendChild(details);
    const toggle = () => {
      const expanded = !card.classList.contains('expanded');
      card.classList.toggle('expanded', expanded);
      card.setAttribute('aria-expanded', expanded ? 'true' : 'false');
    };
    card.addEventListener('click', (ev) => { ev.preventDefault(); toggle(); });
    card.addEventListener('keydown', (ev) => {
      if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); toggle(); }
    });
    peopleList.appendChild(card);
  });
}

function renderSuggestions(items){
  if (!suggestionsWrap) return;
  suggestionsWrap.innerHTML = '';
  if (!items || !items.length) {
    suggestionsWrap.classList.add('hidden');
    return;
  }
  suggestionsWrap.classList.remove('hidden');
  items.slice(0, 3).forEach((item) => {
    const label = (item && (item.label || item.prompt)) || '';
    const prompt = (item && (item.prompt || item.label)) || '';
    if (!label || !prompt) return;
    const chip = document.createElement('button');
    chip.className = 'suggestion-chip';
    chip.textContent = label;
    chip.type = 'button';
    chip.onclick = () => sendManualMessage(prompt);
    suggestionsWrap.appendChild(chip);
  });
}

function setAppState(newState) {
  state = newState; micBtn.classList.remove('listening', 'speaking');
  switch (state) {
    case AppState.IDLE: micIconContainer.innerHTML = ICONS.mic; micBtn.disabled = false; updateStatus('Tap the mic to start.'); break;
    case AppState.LISTENING: micIconContainer.innerHTML = ICONS.stop; micBtn.classList.add('listening'); micBtn.disabled = false; updateStatus('Listening... tap to stop.'); break;
    case AppState.PROCESSING: micIconContainer.innerHTML = ICONS.mic; micBtn.disabled = true; updateStatus('Thinking...'); break;
    case AppState.SPEAKING: micIconContainer.innerHTML = ICONS.stop; micBtn.classList.add('speaking'); micBtn.disabled = false; break;
  }
  const socketReady = socket && socket.readyState === WebSocket.OPEN;
  const disableManual = !socketReady || state === AppState.LISTENING || state === AppState.PROCESSING;
  if (textInput) textInput.disabled = disableManual;
  if (sendBtn) sendBtn.disabled = disableManual;
}
function handleMicClick() {
  renderSuggestions([]);
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
function updateContext(info) {
  let contextWrap = document.getElementById('context-wrap');
  if (info && info.id) {
    const type = info.type || 'Email';
    const fromLine = info.from || info.organizer || 'N/A';
    const emailLine = info.from_email ? `<br><strong>Email:</strong> ${info.from_email}` : '';
    const subjectLine = info.subject || info.title || 'N/A';
    contextWrap.style.display = 'block';
    contextWrap.innerHTML = `<div class="context-display"><div><span class="pill">Current Context</span></div><strong>Type:</strong> ${type}<br><strong>From/Organizer:</strong> ${fromLine}${emailLine}<br><strong>Subject/Title:</strong> ${subjectLine}</div>`;
  } else {
    contextWrap.style.display = 'none';
    contextWrap.innerHTML = '';
  }
  scrollToBottom();
}
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
function sendManualMessage(textOverride){
  if (!socket || socket.readyState !== WebSocket.OPEN) return;
  const raw = textOverride !== undefined ? textOverride : (textInput ? textInput.value : '');
  const text = (raw || '').trim();
  if (!text) return;
  if (textInput && textOverride === undefined) { textInput.value = ''; }
  renderSuggestions([]);
  setAppState(AppState.PROCESSING);
  socket.send(JSON.stringify({ action: 'manual_message', text }));
}
async function checkAuth(){
  const r = await fetch('/api/status'); const j = await r.json();
  const serviceNameElem = document.getElementById('service-name');
  if (j.connected_service === 'none') {
    document.getElementById('auth-view').style.display = 'block';
    document.getElementById('controls').style.display = 'none';
    serviceNameElem.textContent = 'Email';
    renderPeopleList([]);
  } else {
    document.getElementById('auth-view').style.display = 'none';
    document.getElementById('controls').style.display = 'block';
    serviceNameElem.textContent = j.connected_service === 'google' ? 'Gmail' : 'Outlook';
    renderPeopleList([]);
    connectWebSocket();
  }
}
function connectWebSocket(){
  return new Promise((resolve, reject) => {
    updateStatus('Connecting to assistant...');
    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    socket = new WebSocket(`${proto}//${window.location.host}/ws`);
    socket.onopen = () => { appendChat('system', 'Connection established. Assistant is starting...'); renderSuggestions([]); renderPeopleList([]); setAppState(state); resolve(); };
    socket.onclose = () => { updateStatus('Session ended.'); renderSuggestions([]); renderPeopleList([]); setAppState(AppState.IDLE); };
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
        case 'suggestions': renderSuggestions(msg.items || []); break;
        case 'people_list': renderPeopleList(msg.people || []); break;
      }
    };
  });
}
function sendDraft(){ if(!socket || socket.readyState !== WebSocket.OPEN) return; socket.send(JSON.stringify({ action: 'send_draft' })); }
function cancelDraft(){ if(!socket || socket.readyState !== WebSocket.OPEN) return; socket.send(JSON.stringify({ action: 'cancel_draft' })); }
audioPlayer.onended = () => { if (state === AppState.SPEAKING) { setAppState(AppState.IDLE); } };
if (textInput) {
  textInput.addEventListener('keydown', (ev) => {
    if (ev.key === 'Enter' && !ev.shiftKey) { ev.preventDefault(); sendManualMessage(); }
  });
}
renderPeopleList([]);
renderSuggestions([]);
setAppState(AppState.IDLE);
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

# Recipient sanitizer
def _split_recipients(to: str) -> list:
    # Split by comma, trim, drop empties and dups, preserve order
    seen = set()
    result = []
    for addr in [p.strip() for p in to.split(",")]:
        if addr and addr.lower() not in seen:
            seen.add(addr.lower())
            result.append(addr)
    return result

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
            f"You are a warm, conversational Google assistant who manages Gmail and Google Calendar like a proactive chief of staff.\n"
            f"- CRITICAL CONTEXT: The current date and time is {current_time_str}. "
            f"Use this to resolve all relative time references like 'today', 'tomorrow', etc."
        )
        microsoft_prompt = (
            f"You are a warm, conversational Microsoft Outlook assistant who manages Mail and Microsoft Calendar like a proactive chief of staff.\n"
            f"- CRITICAL CONTEXT: The current date and time is {current_time_str}. "
            f"Use this to resolve all relative time references like 'today', 'tomorrow', etc."
        )

        base_instructions = """
- You are a world-class proactive voice assistant. Your goal is to help the user manage their digital life (email and calendar) through natural conversation.
- Keep your tone warm, confident, and collaborative—sound like a thoughtful teammate instead of a scripted bot.
- Briefly acknowledge what you are doing before or after running tools so the user always knows what is happening.
- After calling a tool, ALWAYS summarize the results in your own words, highlighting key takeaways and recommended next steps. Never read raw JSON or metadata aloud.
- When reading long emails, offer to summarize or capture action items so the user stays focused.
- Ask for clarifications instead of assuming details, especially before destructive actions like sending, deleting, or cancelling items.
- Confirm with the user before finalizing destructive actions and explain what will happen next.
- Keep responses focused on the user's goals and end with an invitation to continue helping when appropriate.
- Provide up to three follow-up suggestions encoded as JSON wrapped in <suggestions>{"items":[{"label":"<short label>","prompt":"<assistant-ready prompt>"}]}</suggestions>. Suggestions must be short, relevant, and should not be referenced in the spoken reply."""

        prompt = (google_prompt if service_type == 'google' else microsoft_prompt) + base_instructions

        self.history: List[Dict[str, Any]] = [{"role": "system", "content": prompt}]
        self.last_draft_google: Optional[Dict[str, str]] = None
        self.last_draft_microsoft_id: Optional[str] = None
        self.current_email_context: Optional[Dict[str, str]] = None
        self.current_event_context: Optional[Dict[str, str]] = None
        self.recent_contacts: List[Dict[str, Any]] = []
        self.account_identity: Dict[str, str] = {"email": "", "display_name": ""}
        self._outlook_inbox_id: Optional[str] = None
        self._handled_email_ids: Set[str] = set()
        self._announced_unread_ids: Set[str] = set()
        self._new_email_poll_task: Optional[asyncio.Task] = None
        self._new_email_poll_interval: int = 45
        self._active = True

    async def send_audio_response(self, text: str, status_text: str):
        display_text, suggestions = _extract_suggestions(text or "")
        if not display_text:
            display_text = "Done."

        await self.ws.send_json({"type": "chat_append", "role": "assistant", "text": display_text})
        await self.ws.send_json({"type": "suggestions", "items": suggestions})

        audio_url = await tts_any(display_text)
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
            sender_name = (self.current_email_context.get("from_name") or "").strip()
            sender_email = (self.current_email_context.get("from_email") or "").strip()
            if sender_name and sender_email:
                sender_display = f"{sender_name} <{sender_email}>"
            else:
                sender_display = self.current_email_context.get("from") or sender_email or sender_name
            ctx = {
                "id": self.current_email_context.get("id"),
                "type": "Email",
                "from": sender_display,
                "from_email": sender_email,
                "subject": self.current_email_context.get("subject"),
            }
        await self.ws.send_json({"type": "context_update", "context": ctx})

    async def show_draft(self, to: str, subject: str, body: str):
        await self.ws.send_json({"type": "draft_preview", "to": to, "subject": subject, "body": body})

    async def clear_draft(self):
        self.last_draft_google = None
        self.last_draft_microsoft_id = None
        await self.ws.send_json({"type": "draft_clear"})

    def _remember_handled_email(self, message_id: Optional[str]) -> None:
        if message_id:
            self._handled_email_ids.add(message_id)

    def _is_handled_email(self, message_id: Optional[str]) -> bool:
        return bool(message_id) and message_id in self._handled_email_ids

    def _forget_handled_email(self, message_id: Optional[str]) -> None:
        if message_id:
            self._handled_email_ids.discard(message_id)
            self._announced_unread_ids.discard(message_id)

    async def _fetch_unread_email_contacts(self, max_results: int = 5) -> List[Dict[str, Any]]:
        try:
            if self.service_type == 'google':
                resp = await self.gmail_search_emails(query="in:inbox is:unread", max_results=max_results, publish=False)
            else:
                resp = await self.outlook_search_emails(query="", max_results=max_results, publish=False)
            if not resp or "No emails found" in resp:
                return []
            data = json.loads(resp)
            if isinstance(data, list):
                return [c for c in data if isinstance(c, dict)]
            return []
        except Exception as e:
            print(f"[UNREAD FETCH WARNING] {e}")
            return []

    async def _poll_for_new_emails(self):
        try:
            await asyncio.sleep(15)
            while self._active:
                try:
                    contacts = await self._fetch_unread_email_contacts()
                    new_contacts: List[Dict[str, Any]] = []
                    for contact in contacts:
                        cid = contact.get("id")
                        if not cid or self._is_handled_email(cid) or cid in self._announced_unread_ids:
                            continue
                        self._announced_unread_ids.add(cid)
                        new_contacts.append(contact)
                    if new_contacts:
                        for contact in new_contacts:
                            self._merge_contact(contact)
                        await self._publish_people_list()
                        first = new_contacts[0]
                        count = len(new_contacts)
                        sender = first.get("from") or first.get("from_name") or first.get("from_email") or "someone"
                        subject = first.get("subject") or "(No Subject)"
                        if count > 1:
                            spoken = f"You just received {count} new emails. The most recent is from {sender} about \"{subject}\". Want me to open it?"
                        else:
                            spoken = f"You just received a new email from {sender} about \"{subject}\". Want me to open it?"
                        suggestions = {
                            "items": [
                                {"label": "Read it", "prompt": "Please read my latest email."},
                                {"label": "Summarize it", "prompt": "Summarize the newest email for me."},
                                {"label": "Reply", "prompt": "Draft a quick reply to the latest email."}
                            ]
                        }
                        message = f"{spoken} <suggestions>{json.dumps(suggestions)}</suggestions>"
                        await self.send_audio_response(message, "New email arrived.")
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    print(f"[EMAIL POLL WARNING] {e}")
                await asyncio.sleep(self._new_email_poll_interval)
        except asyncio.CancelledError:
            pass
        finally:
            self._new_email_poll_task = None

    async def stop(self):
        self._active = False
        if self._new_email_poll_task:
            self._new_email_poll_task.cancel()
            try:
                await self._new_email_poll_task
            except asyncio.CancelledError:
                pass
            self._new_email_poll_task = None

    async def _load_gmail_email_into_context(self, message_id: str, mark_read: bool = False) -> Tuple[Dict[str, Any], str]:
        msg = _gmail_service().users().messages().get(userId='me', id=message_id, format='full').execute()
        headers = self._parse_headers(msg.get('payload', {}).get('headers', []))
        sender = _identity_from_header(headers.get('from'))
        reply_to_list = _identities_from_header(headers.get('reply-to')) or ([sender] if sender.get("email") or sender.get("name") else [])
        to_recipients = _identities_from_header(headers.get('to'))
        cc_recipients = _identities_from_header(headers.get('cc'))
        reply_to_emails = [ident.get("email") for ident in reply_to_list if ident.get("email")]
        body_text = _get_email_body(msg)
        context = {
            'id': msg['id'],
            'threadId': msg['threadId'],
            'from': sender.get('display') or headers.get('from', ''),
            'from_name': sender.get('name') or "",
            'from_email': sender.get('email') or "",
            'subject': headers.get('subject', ''),
            'message-id': headers.get('message-id', ''),
            'references': headers.get('references', ''),
            'reply_to_recipients': reply_to_list,
            'reply_to_emails': reply_to_emails,
            'to_recipients': to_recipients,
            'cc_recipients': cc_recipients,
            'to': _join_identity_displays(to_recipients),
            'cc': _join_identity_displays(cc_recipients),
            'date': headers.get('date', ''),
            'reply_to': _join_identity_displays(reply_to_list),
            'body_preview': body_text[:1000],
        }
        self.current_email_context = context
        self.current_event_context = None
        await self.update_context_display()
        await self._ensure_account_identity()
        self._merge_contact({
            "id": context['id'],
            "name": context.get('from_name'),
            "email": context.get('from_email'),
            "display": context.get('from'),
            "subject": context.get('subject'),
            "received": context.get('date'),
            "preview": body_text[:200],
            "service": self.service_type,
        })
        await self._publish_people_list()
        if mark_read and not self._is_handled_email(context['id']):
            try:
                await self.gmail_mark_as_read(context['id'])
            except Exception as e:
                print(f"[Gmail mark-as-read warning] {e}")
        return context, body_text

    async def _load_outlook_email_into_context(self, message_id: str, mark_read: bool = False) -> Tuple[Dict[str, Any], str]:
        params = {
            "$select": "id,subject,from,bodyPreview,body,toRecipients,ccRecipients,replyTo,sentDateTime,receivedDateTime,internetMessageId",
        }
        r = await graph_request(
            "GET",
            f"/me/messages/{message_id}",
            params=params,
            headers={"Prefer": 'outlook.body-content-type="text"'}
        )
        msg = r.json()
        sender = _identity_from_graph((msg.get('from', {}) or {}).get('emailAddress'))
        reply_to_list = _identities_from_graph(msg.get('replyTo')) or ([sender] if sender.get("email") or sender.get("name") else [])
        to_recipients = _identities_from_graph(msg.get('toRecipients'))
        cc_recipients = _identities_from_graph(msg.get('ccRecipients'))
        reply_to_emails = [ident.get("email") for ident in reply_to_list if ident.get("email")]
        received = msg.get('receivedDateTime') or msg.get('sentDateTime') or ''
        body_text = (msg.get('body', {}) or {}).get('content', '') or msg.get('bodyPreview', '')
        context = {
            'id': msg.get('id', message_id),
            'from': sender.get('display') or sender.get('email') or sender.get('name') or "",
            'from_name': sender.get('name') or "",
            'from_email': sender.get('email') or "",
            'subject': msg.get('subject', ''),
            'reply_to_recipients': reply_to_list,
            'reply_to_emails': reply_to_emails,
            'to_recipients': to_recipients,
            'cc_recipients': cc_recipients,
            'to': _join_identity_displays(to_recipients),
            'cc': _join_identity_displays(cc_recipients),
            'reply_to': _join_identity_displays(reply_to_list),
            'date': received,
            'internet_message_id': msg.get('internetMessageId', ''),
            'body_preview': body_text[:1000],
        }
        self.current_email_context = context
        self.current_event_context = None
        await self.update_context_display()
        await self._ensure_account_identity()
        self._merge_contact({
            "id": context['id'],
            "name": context.get('from_name'),
            "email": context.get('from_email'),
            "display": context.get('from'),
            "subject": context.get('subject'),
            "received": context.get('date'),
            "preview": body_text[:200],
            "service": self.service_type,
        })
        await self._publish_people_list()
        if mark_read and not self._is_handled_email(context['id']):
            try:
                await self.outlook_mark_as_read(context['id'])
            except Exception as e:
                print(f"[Outlook mark-as-read warning] {e}")
        return context, body_text

    async def _ensure_email_context(self, message_id: Optional[str] = None, mark_read: bool = False) -> bool:
        if self.current_email_context and (not message_id or self.current_email_context.get('id') == message_id):
            if mark_read and not self._is_handled_email(self.current_email_context.get('id')):
                try:
                    if self.service_type == 'google':
                        await self.gmail_mark_as_read(self.current_email_context['id'])
                    else:
                        await self.outlook_mark_as_read(self.current_email_context['id'])
                except Exception as e:
                    print(f"[Context mark-as-read warning] {e}")
            return True

        target_id = message_id
        if not target_id:
            for contact in self.recent_contacts:
                if contact.get("service") != self.service_type:
                    continue
                cid = contact.get("id")
                if cid and not self._is_handled_email(cid):
                    target_id = cid
                    break
        if not target_id:
            for contact in self.recent_contacts:
                if contact.get("service") != self.service_type:
                    continue
                cid = contact.get("id")
                if cid:
                    target_id = cid
                    break
        if not target_id:
            return False
        try:
            if self.service_type == 'google':
                await self._load_gmail_email_into_context(target_id, mark_read=mark_read)
            else:
                await self._load_outlook_email_into_context(target_id, mark_read=mark_read)
        except Exception as e:
            print(f"[CONTEXT FETCH WARNING] Could not load email context: {e}")
            return False
        return True

    def _merge_contact(self, contact: Dict[str, Any]):
        if not contact:
            return
        name = (contact.get("name") or contact.get("from_name") or contact.get("from") or "").strip()
        email = (contact.get("email") or contact.get("from_email") or "").strip()
        if not name and email:
            name = email.split("@")[0]
        display = (contact.get("display") or contact.get("from") or "").strip()
        if not display:
            if name and email:
                display = f"{name} <{email}>"
            else:
                display = name or email or "Unknown Sender"
        normalized = {
            "id": contact.get("id") or contact.get("message_id") or "",
            "name": name or display or "Unknown Sender",
            "email": email,
            "display": display,
            "subject": contact.get("subject") or "",
            "preview": (contact.get("preview") or contact.get("body_preview") or "").strip(),
            "received": contact.get("received") or contact.get("date") or "",
            "service": contact.get("service") or self.service_type,
        }
        key_email = normalized["email"].lower() if normalized["email"] else None
        key_name = normalized["name"].lower() if normalized["name"] else None
        account_email = (self.account_identity.get("email") or "").lower()
        account_name = (self.account_identity.get("display_name") or "").lower()

        if key_email and account_email and key_email == account_email:
            return
        if not key_email and key_name and account_name and key_name == account_name:
            return

        merged = False
        for existing in self.recent_contacts:
            existing_email = (existing.get("email") or "").lower()
            existing_name = (existing.get("name") or "").lower()
            if key_email and existing_email == key_email:
                for k, v in normalized.items():
                    if v:
                        existing[k] = v
                merged = True
                break
            if not key_email and key_name and existing_name == key_name:
                for k, v in normalized.items():
                    if v:
                        existing[k] = v
                merged = True
                break
        if not merged:
            self.recent_contacts.insert(0, normalized)
        self.recent_contacts = self.recent_contacts[:15]

    async def _publish_people_list(self):
        await self.ws.send_json({"type": "people_list", "people": self.recent_contacts})

    async def _ensure_account_identity(self):
        if self.account_identity.get("email"):
            return
        try:
            if self.service_type == 'google':
                profile = _gmail_service().users().getProfile(userId='me').execute()
                email = (profile.get("emailAddress") or "").strip()
                display_name = email
            else:
                resp = await graph_request("GET", "/me", params={"$select": "displayName,mail,userPrincipalName"})
                data = resp.json()
                email = (data.get("mail") or data.get("userPrincipalName") or "").strip()
                display_name = (data.get("displayName") or email).strip()
            self.account_identity = {
                "email": email.lower(),
                "display_name": display_name,
            }
        except Exception as e:
            print(f"[IDENTITY WARNING] Unable to load account identity: {e}")
            self.account_identity = {"email": "", "display_name": ""}

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

    async def gmail_search_emails(self, query: str, max_results: int = 5, publish: bool = True) -> str:
        s = _gmail_service()
        if publish:
            await self._ensure_account_identity()
        normalized_query = (query or "").strip()
        if "in:" not in normalized_query.lower():
            normalized_query = f"in:inbox {normalized_query}".strip()
        if "is:" not in normalized_query.lower() and "label:" not in normalized_query.lower():
            normalized_query = f"{normalized_query} is:unread".strip()
        results = s.users().messages().list(
            userId='me',
            q=normalized_query,
            labelIds=['INBOX', 'UNREAD'],
            includeSpamTrash=False,
            maxResults=max_results
        ).execute()
        messages = results.get('messages', [])
        email_list = []
        for msg in messages:
            if self._is_handled_email(msg.get('id')):
                continue
            meta = s.users().messages().get(userId='me', id=msg['id'], format='full').execute()
            headers = self._parse_headers(meta.get('payload', {}).get('headers', []))
            sender = _identity_from_header(headers.get('from'))
            body_preview = (_get_email_body(meta) or meta.get('snippet', '') or '')[:200]
            contact = {
                "id": msg['id'],
                "from": sender.get("display") or headers.get('from', '...') or "...",
                "from_name": sender.get("name") or "",
                "from_email": sender.get("email") or "",
                "subject": headers.get('subject', '(No Subject)'),
                "received": headers.get('date', ''),
                "body_preview": body_preview,
                "service": self.service_type,
            }
            email_list.append(contact)
            if publish:
                self._merge_contact(contact)
            if len(email_list) >= max_results:
                break
        if not email_list:
            return f"No emails found for '{query}'"
        if publish:
            await self._publish_people_list()
        return json.dumps(email_list)

    async def gmail_read_email(self, message_id: Optional[str] = None) -> str:
        target_id = message_id or (self.current_email_context and self.current_email_context.get('id'))
        if not target_id:
            return "Error: No message ID."
        context, body_text = await self._load_gmail_email_into_context(target_id, mark_read=True)
        return json.dumps({
            "id": context['id'],
            "from": context['from'],
            "from_name": context.get('from_name', ''),
            "from_email": context.get('from_email', ''),
            "reply_to": context.get('reply_to', ''),
            "subject": context['subject'],
            "to": context.get('to', ''),
            "cc": context.get('cc', ''),
            "received": context.get('date', ''),
            "body_preview": body_text[:1000]
        })

    async def gmail_summarize_email(self) -> str:
        if not self.current_email_context:
            return "Error: No email in context."
        msg = _gmail_service().users().messages().get(userId='me', id=self.current_email_context['id'], format='full').execute()
        body_text = _get_email_body(msg)
        sender_name = self.current_email_context.get('from_name') or ""
        sender_email = self.current_email_context.get('from_email') or ""
        subject = self.current_email_context.get('subject') or ""
        to_line = self.current_email_context.get('to') or "(you)"
        cc_line = self.current_email_context.get('cc') or ""
        received = self.current_email_context.get('date') or ""
        prompt = (
            "You are preparing a spoken summary of an email for the account owner.\n"
            "Deliver a warm, professional synopsis that:\n"
            "- Opens with the sender's name and subject.\n"
            "- Highlights the main points and any explicit requests or deadlines.\n"
            "- Calls out the sender's email address if a reply may be needed.\n"
            "- Ends with a suggested next step or reply idea when appropriate.\n"
            "Keep it under 170 words.\n\n"
            f"Metadata:\nSubject: {subject}\nFrom: {sender_name} <{sender_email}>\nTo: {to_line}\nCc: {cc_line}\nReceived: {received}\n\n"
            f"Email Body:\n```\n{body_text}\n```"
        )
        payload = {"model": REALTIME_MODEL, "messages": [{"role": "user", "content": prompt}], "temperature": 0.4}
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
        r = await _client().post(f"{OPENAI_BASE_URL.rstrip('/')}/v1/chat/completions", json=payload, headers=headers)
        r.raise_for_status()
        summary = r.json()["choices"][0]["message"]["content"]
        if self.current_email_context and not self._is_handled_email(self.current_email_context.get('id')):
            try:
                await self.gmail_mark_as_read(self.current_email_context['id'])
            except Exception as e:
                print(f"[Gmail auto mark-as-read warning] {e}")
        return summary

    async def gmail_draft_new_email(self, to: str, subject: str, body: str) -> str:
        self.current_email_context = None
        await self.update_context_display()
        recipients = _split_recipients(to)
        self.last_draft_google = {"to": ", ".join(recipients), "subject": subject, "body": body}
        await self.show_draft(", ".join(recipients), subject, body)
        return "Draft created. Ask user to confirm."

    async def gmail_draft_reply(self, body: str) -> str:
        if not await self._ensure_email_context(mark_read=True):
            return "Error: No email context to reply to."
        subject = self.current_email_context.get('subject', '')
        if not subject.lower().startswith("re:"):
            subject = f"Re: {subject}"
        reply_to_recipients = self.current_email_context.get('reply_to_recipients') or []
        if not reply_to_recipients:
            fallback_display = self.current_email_context.get('from') or ""
            reply_to_recipients = [{
                "name": self.current_email_context.get('from_name', ''),
                "email": self.current_email_context.get('from_email', ''),
                "display": fallback_display,
            }]
        to_field = _join_identity_displays(reply_to_recipients) or self.current_email_context.get('from') or ""
        self.last_draft_google = {"to": to_field, "subject": subject, "body": body}
        await self.show_draft(to_field, subject, body)
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
        target_id = message_id or (self.current_email_context and self.current_email_context.get('id'))
        if not target_id:
            return "Error: No message ID."
        result = await self._gmail_context_action(target_id, lambda **kwargs: _gmail_service().users().messages().modify(**kwargs, body={'removeLabelIds': ['UNREAD']}), "Email marked as read.", clear_ctx=False)
        self._remember_handled_email(target_id)
        self._announced_unread_ids.discard(target_id)
        return result

    async def gmail_mark_as_unread(self, message_id: Optional[str] = None) -> str:
        target_id = message_id or (self.current_email_context and self.current_email_context.get('id'))
        if not target_id:
            return "Error: No message ID."
        result = await self._gmail_context_action(target_id, lambda **kwargs: _gmail_service().users().messages().modify(**kwargs, body={'addLabelIds': ['UNREAD']}), "Email marked as unread.", clear_ctx=False)
        self._forget_handled_email(target_id)
        self._announced_unread_ids.discard(target_id)
        return result

    # --- MICROSOFT TOOL IMPLEMENTATIONS ---
    async def outlook_search_emails(self, query: str = "", max_results: int = 5, publish: bool = True) -> str:
        if publish:
            await self._ensure_account_identity()
        inbox_endpoint = "/me/mailFolders('Inbox')/messages"
        if not query or not query.strip():
            params = {
                "$orderby": "receivedDateTime desc",
                "$top": max_results,
                "$select": "id,subject,from,receivedDateTime,bodyPreview,isRead",
                "$filter": "isRead eq false"
            }
            r = await graph_request("GET", inbox_endpoint, params=params)
        else:
            params = {
                "$search": f'"{query}"',
                "$top": max_results * 3,
                "$select": "id,subject,from,receivedDateTime,bodyPreview,isRead"
            }
            r = await graph_request("GET", inbox_endpoint, params=params)

        messages = r.json().get("value", [])
        email_list = []
        for m in messages:
            if m.get("isRead"):
                continue
            if self._is_handled_email(m.get('id')):
                continue
            sender = (m.get('from', {}) or {}).get('emailAddress', {}) or {}
            sender_name = (sender.get('name') or "").strip()
            sender_email = (sender.get('address') or "").strip()
            if sender_name and sender_email:
                sender_display = f"{sender_name} <{sender_email}>"
            else:
                sender_display = sender_name or sender_email or "..."
            contact = {
                "id": m.get('id'),
                "from": sender_display,
                "from_name": sender_name,
                "from_email": sender_email,
                "subject": m.get('subject') or "(No Subject)",
                "received": m.get('receivedDateTime', ""),
                "body_preview": (m.get('bodyPreview') or "")[:200],
                "service": self.service_type,
            }
            email_list.append(contact)
            if publish:
                self._merge_contact(contact)
            if len(email_list) >= max_results:
                break
        if not email_list:
            return "No emails found." if not query.strip() else f"No emails found for '{query}'"
        if publish:
            await self._publish_people_list()
        return json.dumps(email_list)

    async def outlook_read_email(self, message_id: Optional[str] = None) -> str:
        target_id = message_id or (self.current_email_context and self.current_email_context.get('id'))
        if not target_id:
            return "Error: No message ID."
        context, body_text = await self._load_outlook_email_into_context(target_id, mark_read=True)
        return json.dumps({
            "id": context['id'],
            "from": context['from'],
            "from_name": context.get('from_name', ''),
            "from_email": context.get('from_email', ''),
            "reply_to": context.get('reply_to', ''),
            "subject": context['subject'],
            "to": context.get('to', ''),
            "cc": context.get('cc', ''),
            "received": context.get('date', ''),
            "body_preview": body_text[:1000]
        })

    async def outlook_summarize_email(self) -> str:
        if not self.current_email_context:
            return "Error: No email in context."
        r = await graph_request(
            "GET",
            f"/me/messages/{self.current_email_context['id']}",
            params={"$select": "body"},
            headers={"Prefer": 'outlook.body-content-type="text"'}
        )
        body_text = ((r.json().get('body', {}) or {}).get('content', '') or '')
        sender_name = self.current_email_context.get('from_name') or ""
        sender_email = self.current_email_context.get('from_email') or ""
        subject = self.current_email_context.get('subject') or ""
        to_line = self.current_email_context.get('to') or "(you)"
        cc_line = self.current_email_context.get('cc') or ""
        received = self.current_email_context.get('date') or ""
        prompt = (
            "Provide a concise, user-friendly summary of this Outlook email.\n"
            "Mention the sender by name, include their email address, cover the main points, and note any requests, deadlines, or attachments.\n"
            "If a response is implied, suggest how the user might reply.\n"
            "Keep the summary under 170 words.\n\n"
            f"Metadata:\nSubject: {subject}\nFrom: {sender_name} <{sender_email}>\nTo: {to_line}\nCc: {cc_line}\nReceived: {received}\n\n"
            f"Email Body:\n```\n{body_text}\n```"
        )
        payload = {"model": REALTIME_MODEL, "messages": [{"role": "user", "content": prompt}], "temperature": 0.4}
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
        resp = await _client().post(f"{OPENAI_BASE_URL.rstrip('/')}/v1/chat/completions", json=payload, headers=headers)
        resp.raise_for_status()
        summary = resp.json()["choices"][0]["message"]["content"]
        if self.current_email_context and not self._is_handled_email(self.current_email_context.get('id')):
            try:
                await self.outlook_mark_as_read(self.current_email_context['id'])
            except Exception as e:
                print(f"[Outlook auto mark-as-read warning] {e}")
        return summary

    async def outlook_draft_new_email(self, to: str, subject: str, body: str) -> str:
        self.current_email_context = None
        await self.update_context_display()
        recipients = _split_recipients(to)
        message = {
            "subject": subject,
            "body": {"contentType": "Text", "content": body},
            "toRecipients": [{"emailAddress": {"address": addr}} for addr in recipients]
        }
        r = await graph_request("POST", "/me/messages", json=message)
        self.last_draft_microsoft_id = r.json().get("id")
        await self.show_draft(", ".join(recipients), subject, body)
        return "Draft created. Ask user to confirm."

    async def outlook_draft_reply(self, body: str) -> str:
        if not await self._ensure_email_context(mark_read=True):
            return "Error: No email context to reply to."
        reply_payload = {"comment": body}
        r = await graph_request("POST", f"/me/messages/{self.current_email_context['id']}/createReply", json=reply_payload)
        draft = r.json() if r.content else {}
        draft_id = draft.get('id')
        if not draft_id:
            try:
                search_r = await graph_request(
                    "GET",
                    "/me/messages?$filter=isDraft eq true&$orderby=receivedDateTime desc&$top=10&$select=id,subject,toRecipients"
                )
                for m in search_r.json().get("value", []):
                    subj = (m.get("subject") or "").lower()
                    if subj.startswith("re:") or subj.startswith("fw:"):
                        draft_id = m.get("id")
                        draft = m
                        break
            except Exception as e:
                print(f"[Outlook reply-draft lookup warning] {e}")
        if not draft_id:
            return "Error: Could not create a reply draft."
        self.last_draft_microsoft_id = draft_id
        to_str = ", ".join([rcpt.get('emailAddress', {}).get('address', '') for rcpt in draft.get('toRecipients', []) if rcpt.get('emailAddress')])
        await self.show_draft(to_str, draft.get('subject', '(No Subject)'), body)
        return "Reply draft created."
                                
    async def outlook_send_draft(self) -> str:
        if not self.last_draft_microsoft_id:
            return "Error: No draft to send."
        try:
            await graph_request("POST", f"/me/messages/{self.last_draft_microsoft_id}/send")
        except Exception as e:
            return f"Error: Could not send the draft. {e}"
        handled_id = self.current_email_context.get('id') if self.current_email_context else None
        await self.clear_draft()
        if handled_id and not self._is_handled_email(handled_id):
            try:
                await self.outlook_mark_as_read(handled_id)
            except Exception as e:
                print(f"[Outlook send mark-as-read warning] {e}")
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
        self._remember_handled_email(target_id)
        self._announced_unread_ids.discard(target_id)
        return "Email marked as read."

    async def outlook_mark_as_unread(self, message_id: Optional[str] = None) -> str:
        target_id = message_id or (self.current_email_context and self.current_email_context.get('id'))
        if not target_id:
            return "Error: No message ID."
        await graph_request("PATCH", f"/me/messages/{target_id}", json={"isRead": False})
        self._forget_handled_email(target_id)
        self._announced_unread_ids.discard(target_id)
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
    async def _get_unread_email_summary(self) -> Tuple[str, List[Dict[str, Any]]]:
        contacts: List[Dict[str, Any]] = []
        try:
            await self._ensure_account_identity()
            now_utc = datetime.datetime.now(timezone.utc)
            start_of_yesterday_utc = (now_utc - datetime.timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            start_time_iso_utc = start_of_yesterday_utc.isoformat().replace('+00:00', 'Z')

            if self.service_type == 'google':
                after_date_str = start_of_yesterday_utc.strftime('%Y/%m/%d')
                query = f"in:inbox is:unread after:{after_date_str}"
                unread_json = await self.gmail_search_emails(query=query, max_results=5, publish=False)
            else:
                params = {
                    "$filter": f"isRead eq false and receivedDateTime ge {start_time_iso_utc}",
                    "$top": 5,
                    "$select": "id,subject,from,receivedDateTime,bodyPreview"
                }
                r = await graph_request("GET", "/me/mailFolders('Inbox')/messages", params=params)
                messages = r.json().get("value", [])
                if not messages:
                    return "You have no new emails since yesterday.", contacts
                out = []
                for m in messages:
                    sender = _identity_from_graph((m.get('from', {}) or {}).get('emailAddress'))
                    out.append({
                        "id": m.get('id'),
                        "from": sender.get('display') or sender.get('email') or sender.get('name') or "...",
                        "from_name": sender.get('name') or "",
                        "from_email": sender.get('email') or "",
                        "subject": m.get('subject') or "(No Subject)",
                        "received": m.get('receivedDateTime', ""),
                        "body_preview": (m.get('bodyPreview') or "")[:200],
                        "service": self.service_type,
                    })
                unread_json = json.dumps(out)

            if "No emails found" in unread_json:
                return "You have no new emails since yesterday.", contacts

            emails = [e for e in json.loads(unread_json) if not self._is_handled_email(e.get("id"))]
            if not emails:
                return "You have no new emails since yesterday.", contacts
            for e in emails:
                contacts.append({
                    "id": e.get("id"),
                    "name": e.get("from_name") or (e.get("from") or "").split("<")[0].strip(),
                    "email": e.get("from_email") or "",
                    "display": e.get("from"),
                    "subject": e.get("subject") or "",
                    "received": e.get("received") or "",
                    "preview": (e.get("body_preview") or "")[:200],
                    "service": self.service_type,
                })
            count = len(emails)
            plural = "s" if count > 1 else ""
            senders = list({(e.get('from') or '').split('<')[0].strip() for e in emails[:3] if e.get('from')})
            senders = [s for s in senders if s]
            senders_str = ", ".join(senders)
            if senders_str:
                summary = f"You have {count} new email{plural} since yesterday, including messages from {senders_str}."
            else:
                summary = f"You have {count} new email{plural} since yesterday."
            return summary, contacts

        except Exception as e:
            print(f"[STARTUP ERROR] checking unread mail: {e}")
            return "Could not check for new emails.", contacts

    async def _get_todays_events_summary(self) -> str:
        try:
            now = datetime.datetime.now().astimezone()
            start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end_of_day = now.replace(hour=23, minute=59, second=59, microsecond=999999)

            events_json = await self.calendar_list_events(
                time_min=start_of_day.isoformat(),
                time_max=end_of_day.isoformat(),
                max_results=5
            )

            if "No upcoming events found" in events_json:
                return "You have no events scheduled for today."

            events = json.loads(events_json)
            count = len(events)
            plural = "s" if count > 1 else ""
            titles = [e['summary'] for e in events[:3]]
            titles_str = ", ".join(titles)

            return f"You have {count} event{plural} on your calendar for today, starting with {titles_str}."

        except Exception as e:
            print(f"[STARTUP ERROR] checking calendar: {e}")
            return "Could not check your calendar."

    async def _get_startup_summary(self) -> str:
        try:
            (email_summary, contacts), event_summary = await asyncio.gather(
                self._get_unread_email_summary(),
                self._get_todays_events_summary()
            )
            self.recent_contacts = []
            for contact in contacts:
                self._merge_contact(contact)
            self._announced_unread_ids = {contact.get("id") for contact in contacts if contact.get("id")}
            await self._publish_people_list()
            return f"{email_summary} {event_summary}"
        except Exception as e:
            print(f"Error getting startup summary: {e}")
            return "There was an issue checking your accounts for updates."

    async def start(self):
        try:
            await self.ws.send_json({"type": "update_status", "text": "Checking for updates..."})
            await self._ensure_account_identity()
            startup_summary = await self._get_startup_summary()

            self.history.append({
                "role": "system",
                "content": f"Here is the user's current status: {startup_summary}. Formulate a friendly and proactive welcome message based on this information, then ask them what they'd like to do. Be conversational."
            })

            client = _client()
            payload = {"model": REALTIME_MODEL, "messages": self.history, "temperature": 0.7}
            headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
            r = await client.post(f"{OPENAI_BASE_URL.rstrip('/')}/v1/chat/completions", json=payload, headers=headers)
            if r.status_code >= 400:
                print(f"[OPENAI 4xx on start] {r.status_code} :: {r.text[:5000]}")
                initial_greeting = "Hello. I could not load your status, but I am ready. What do you want to do?"
            else:
                response_message = r.json()["choices"][0]["message"]
                initial_greeting = response_message.get("content", "Hello! How can I help you today?")
                self.history.append(response_message)

            await self.send_audio_response(initial_greeting, "Ready for your command...")

        except Exception as e:
            print(f"[AGENT START ERROR] {traceback.format_exc()}")
            fallback_greeting = "Hello! I'm ready. How can I help you?"
            self.history.append({"role": "assistant", "content": fallback_greeting})
            await self.send_audio_response(fallback_greeting, "Ready for your command...")
        finally:
            self._active = True
            if not self._new_email_poll_task:
                self._new_email_poll_task = asyncio.create_task(self._poll_for_new_emails())

    async def process_user_message(self, transcript: str):
        await self.append_chat("user", transcript)
        await self.ws.send_json({"type": "suggestions", "items": []})
        await self.ws.send_json({"type": "update_status", "text": "Thinking..."})
        self.history.append({"role": "user", "content": transcript})
        try:
            client = _client()
            payload = {"model": REALTIME_MODEL, "messages": self.history, "tools": self.tools, "tool_choice": "auto"}
            headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
            r = await client.post(f"{OPENAI_BASE_URL.rstrip('/')}/v1/chat/completions", json=payload, headers=headers)
            if r.status_code >= 400:
                print(f"[OPENAI 4xx] {r.status_code} :: {r.text[:5000]}")
                await self.send_audio_response("I had trouble understanding that. Can you rephrase?", "Tap the mic to reply...")
                return
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
            "gmail_search_emails": self.gmail_search_emails, "gmail_read_email": self.gmail_read_email,
            "gmail_summarize_email": self.gmail_summarize_email, "gmail_draft_new_email": self.gmail_draft_new_email,
            "gmail_draft_reply": self.gmail_draft_reply, "gmail_send_draft": self.gmail_send_draft,
            "gmail_delete_email": self.gmail_delete_email, "gmail_archive_email": self.gmail_archive_email,
            "gmail_mark_as_read": self.gmail_mark_as_read, "gmail_mark_as_unread": self.gmail_mark_as_unread,
            # Outlook
            "outlook_search_emails": self.outlook_search_emails, "outlook_read_email": self.outlook_read_email,
            "outlook_summarize_email": self.outlook_summarize_email, "outlook_draft_new_email": self.outlook_draft_new_email,
            "outlook_draft_reply": self.outlook_draft_reply, "outlook_send_draft": self.outlook_send_draft,
            "outlook_delete_email": self.outlook_delete_email, "outlook_archive_email": self.outlook_archive_email,
            "outlook_mark_as_read": self.outlook_mark_as_read, "outlook_mark_as_unread": self.outlook_mark_as_unread,
            # Calendar (unified)
            "calendar_list_events": self.calendar_list_events, "calendar_quick_add": self.calendar_quick_add,
            "calendar_create_event": self.calendar_create_event, "calendar_update_event_time": self.calendar_update_event_time,
            "calendar_delete_event": self.calendar_delete_event,
        }
        for tool_call in tool_calls:
            function_name = tool_call['function']['name']
            function_args = json.loads(tool_call['function']['arguments'] or "{}")
            function = tool_functions.get(function_name)
            if not function:
                warning = f"Tool '{function_name}' is not implemented."
                print(f"[TOOL WARNING] {warning}")
                self.history.append({"tool_call_id": tool_call['id'], "role": "tool", "name": function_name, "content": warning})
                continue

            status_msg = _tool_status_message(function_name, function_args, self.service_type)
            if status_msg:
                await self.ws.send_json({"type": "update_status", "text": status_msg})

            try:
                function_response = await function(**function_args)
            except Exception:
                function_response = f"Error executing tool: {traceback.format_exc().splitlines()[-1]}"
            self.history.append({"tool_call_id": tool_call['id'], "role": "tool", "name": function_name, "content": function_response})

        client = _client()
        payload = {"model": REALTIME_MODEL, "messages": self.history}
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
        r = await client.post(f"{OPENAI_BASE_URL.rstrip('/')}/v1/chat/completions", json=payload, headers=headers)
        if r.status_code >= 400:
            print(f"[OPENAI 4xx after tools] {r.status_code} :: {r.text[:5000]}")
            await self.send_audio_response("Done. Anything else?", "Tap the mic to reply...")
            return
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
        elif action == "manual_message":
            text = (data.get("text") or "").strip()
            if text:
                await self.process_user_message(text)

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
    finally:
        await manager.stop()

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
