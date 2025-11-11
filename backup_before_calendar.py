# app.py
"""
Conversational Gmail Voice Assistant (Single File) - v5.0 (GPT-5-chat-latest Agentic)
- Voice-first Gmail assistant with a modern, mobile-first chat UI.
- POWERED BY GPT-5-chat-latest: Uses tool-calling for smarter, more flexible conversations.
- FULL CONTEXT: Maintains conversation history for follow-up questions.
- PUSH-TO-TALK with INTERRUPT: Pressing the talk button immediately stops the assistant's voice.
- WebSockets for audio + JSON events; TTS + Whisper transcription.

NEW IN V5.0:
  - Mobile-First UI: Complete redesign for a modern, single-view chat experience ideal for phones.
  - Audio Interrupt: Tapping the mic button now instantly silences the assistant's response.
  - Simplified Flow: Removed "Start Assistant" button; connection is now automatic on first use.
  - UI Enhancements: Added pulsing animation for listening state and cleaner visuals.

Install:
  pip install fastapi uvicorn "websockets>=12" httpx python-dotenv \
              google-auth google-auth-oauthlib google-api-python-client

Run:
  uvicorn app:app --host 0.0.0.0 --port 8000 --reload

Env (.env):
  OPENAI_API_KEY=...
  OPENAI_BASE_URL=https://api.openai.com
  REALTIME_MODEL=gpt-5-chat-latest
  REALTIME_VOICE=breeze
  # Gmail OAuth (Needs gmail.modify scope)
  GOOGLE_CLIENT_ID=xxxxxxxx.apps.googleusercontent.com
  GOOGLE_CLIENT_SECRET=xxxxxxxx
  GOOGLE_REDIRECT_URI=http://localhost:8000/gmail/oauth2callback
"""

import os, io, json, base64, re, uuid, asyncio, traceback
from typing import Optional, List, Dict, Any
from email.message import EmailMessage

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, StreamingResponse, PlainTextResponse, RedirectResponse
from dotenv import load_dotenv

# Google / Gmail
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

load_dotenv()

# ---------- Configuration ----------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com")
REALTIME_MODEL = os.getenv("REALTIME_MODEL", "gpt-5-chat-latest")
REALTIME_VOICE = os.getenv("REALTIME_VOICE", "aria")

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:8000/gmail/oauth2callback")
GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.send",
]

if not OPENAI_API_KEY:
    raise RuntimeError("Set OPENAI_API_KEY in environment")

app = FastAPI()

# In-memory demo state (single user)
_GMAIL_CREDS: Optional[Credentials] = None
_GENERATED_AUDIO: Dict[str, bytes] = {}  # Store audio clips by UUID

# ---------- Global HTTP client (connection pooling) ----------
_httpx_client: Optional[httpx.AsyncClient] = None

@app.on_event("startup")
async def _startup():
    global _httpx_client
    _httpx_client = httpx.AsyncClient(
        timeout=httpx.Timeout(60.0, connect=10.0, read=50.0),
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
        limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
    )

@app.on_event("shutdown")
async def _shutdown():
    global _httpx_client
    if _httpx_client:
        await _httpx_client.aclose()
        _httpx_client = None

def _client() -> httpx.AsyncClient:
    if not _httpx_client:
        raise RuntimeError("HTTP client not initialized")
    return _httpx_client

# ======================= UI / HTML Page =======================

CONVERSATIONAL_HTML = """
<!doctype html><html><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no"/>
<title>Gmail Voice Assistant</title>
<style>
  :root {
    --bg:#f4f6fc; --card:#ffffff; --ink:#111827; --muted:#6b7280;
    --brand:#4f46e5; --brand-hover:#4338ca; --red:#ef4444;
    --border:#e5e7eb; --chip:#e0e7ff; --chip-ink:#3730a3;
  }
  * { box-sizing:border-box; -webkit-tap-highlight-color:transparent; }
  html, body { height:100%; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji";
    background:var(--bg); color:var(--ink); margin:0;
    display:flex; flex-direction:column;
  }
  .app-container {
    display:flex; flex-direction:column; height:100%;
    max-width:800px; width:100%; margin:0 auto; background:var(--card);
    box-shadow: 0 0 40px rgba(0,0,0,0.05);
  }
  header {
    padding:12px 16px; border-bottom:1px solid var(--border);
    display:flex; justify-content:space-between; align-items:center;
    flex-shrink:0;
  }
  header h1 { font-size:18px; margin:0; }
  .badge { background:var(--chip); color:var(--chip-ink); border-radius:12px; padding:3px 10px; font-size:12px; font-weight:500; }
  .chat-container {
    flex:1 1 auto; overflow-y:auto; padding:16px;
    display:flex; flex-direction:column; gap:12px;
  }
  .bubble {
    max-width:85%; padding:10px 14px; border-radius:18px; line-height:1.5;
    border:1px solid var(--border);
  }
  .bubble.user { margin-left:auto; background:#eff6ff; border-color:#dbeafe; color:#1e40af; }
  .bubble.assistant { margin-right:auto; background:#f9fafb; }
  .bubble.system { font-style:italic; text-align:center; background:#f3f4f6; color:var(--muted); font-size:13px; padding:6px 10px; border-radius:12px; }
  .bubble pre { white-space:pre-wrap; font-family:inherit; margin:0; }
  .draft { border:1px dashed #a5b4fc; border-radius:12px; padding:12px; background:#fafaff; margin-top:8px; }
  .draft h3 { margin:0 0 8px 0; font-size:14px; color: var(--brand); }
  .draft pre { white-space:pre-wrap; font-family:inherit; background:#fff; border:1px solid var(--border); padding:8px; border-radius:8px; }
  .draft .actions { display:flex; gap:8px; margin-top:10px; }
  .btn { padding:10px 14px; border:0; background:var(--brand); color:#fff; border-radius:10px; cursor:pointer; font-size:15px; transition:background-color .2s; }
  .btn:hover { background:var(--brand-hover); }
  .btn.secondary { background:#e5e7eb; color:#111827; }
  .btn.secondary:hover { background:#d1d5db; }
  .pill { display:inline-block; background:#ecfeff; color:#0e7490; border:1px solid #a5f3fc; padding:2px 8px; border-radius:999px; font-size:11px; margin-top:6px; }
  .context-display { font-size:12px; color:var(--muted); line-height:1.4; background:#fafafa; padding:8px 10px; border-radius:8px; border:1px solid var(--border); }
  .controls-bar {
    flex-shrink:0; padding:16px; background:var(--card);
    border-top:1px solid var(--border); box-shadow: 0 -4px 12px rgba(0,0,0,0.03);
    text-align:center;
  }
  #mic-btn {
    width:64px; height:64px; border-radius:50%; border:0;
    background:var(--brand); color:white; cursor:pointer;
    display:inline-flex; align-items:center; justify-content:center;
    transition: all .2s;
  }
  #mic-btn:disabled { background:var(--muted); cursor:not-allowed; }
  #mic-btn.listening { background:var(--red); animation: pulse 1.5s infinite; }
  @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(239, 68, 68, 0.7); } 70% { box-shadow: 0 0 0 12px rgba(239, 68, 68, 0); } 100% { box-shadow: 0 0 0 0 rgba(239, 68, 68, 0); } }
  #status-text { color:var(--muted); font-size:14px; margin-top:12px; min-height:20px; }
  .auth-view { padding: 24px; text-align:center; }
  .auth-view h2 { margin-top:0; }
</style>
</head><body>
<div id="app-container" class="app-container">
  <header>
    <h1>Gmail Assistant</h1><span class="badge">GPT-5-chat-latest</span>
  </header>
  <div class="chat-container" id="chat-container">
    <div id="auth-view" class="auth-view" style="display:none;">
      <h2>Welcome!</h2>
      <p id="auth-msg">Please connect your Gmail account to begin.</p>
      <a class="btn" id="login-btn" href="/gmail/login">Connect Gmail</a>
    </div>
    <div id="chat-log"></div>
    <div id="draft-wrap" style="display:none"></div>
    <div id="context-wrap" style="display:none"></div>
  </div>
  <div id="controls" class="controls-bar" style="display:none;">
    <button id="mic-btn" onclick="toggleListening()" disabled>
      <svg xmlns="http://www.w3.org/2000/svg" width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/><line x1="12" y1="19" x2="12" y2="22"/></svg>
    </button>
    <div id="status-text">Checking Gmail connection...</div>
  </div>
</div>

<script>
let socket;
let mediaRecorder;
let audioChunks = [];
let isListening = false;
let currentAudio = null; // Holds the current Audio object for interruption

const chatLog = document.getElementById('chat-log');
const chatContainer = document.getElementById('chat-container');
const micBtn = document.getElementById('mic-btn');
const statusText = document.getElementById('status-text');

function scrollToBottom() { chatContainer.scrollTop = chatContainer.scrollHeight; }

function appendChat(role, text, id = null) {
  const wrap = document.createElement('div');
  wrap.className = 'bubble ' + role;
  if (id) wrap.id = id;
  if (role === 'assistant') {
    const pre = document.createElement('pre');
    pre.textContent = text;
    wrap.appendChild(pre);
  } else {
    wrap.textContent = text;
  }
  chatLog.appendChild(wrap);
  scrollToBottom();
}

function updateContext(info) {
  let contextWrap = document.getElementById('context-wrap');
  if (info && info.id) {
    contextWrap.style.display = 'block';
    contextWrap.innerHTML = `
      <div class="context-display">
        <div><span class="pill">Current Context</span></div>
        <strong>From:</strong> ${info.from || 'N/A'}<br>
        <strong>Subject:</strong> ${info.subject || 'N/A'}
      </div>`;
  } else {
    contextWrap.style.display = 'none';
    contextWrap.innerHTML = '';
  }
  scrollToBottom();
}

function showDraft(to, subject, body){
  const draftWrap = document.getElementById('draft-wrap');
  draftWrap.innerHTML = `
    <div class="draft">
      <h3>Email draft (preview)</h3>
      <div><strong>To:</strong> <span>${to || '(none)'}</span></div>
      <div><strong>Subject:</strong> <span>${subject || '(none)'}</span></div>
      <div style="margin-top:6px;"><strong>Body:</strong></div>
      <pre>${body || ''}</pre>
      <div class="actions">
        <button class="btn" onclick="sendDraft()">Send</button>
        <button class="btn secondary" onclick="cancelDraft()">Cancel</button>
      </div>
    </div>`;
  draftWrap.style.display = 'block';
  scrollToBottom();
}
function hideDraft(){ document.getElementById('draft-wrap').style.display = 'none'; }
function updateStatus(text){ statusText.textContent = text; }

function stopCurrentAudio() {
  if (currentAudio) {
    currentAudio.pause();
    currentAudio.src = ''; // Release resource
    currentAudio = null;
  }
}

function toggleListening() {
  if (isListening) { stopRecording(); }
  else {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      connectWebSocket().then(startRecording);
    } else {
      startRecording();
    }
  }
}

async function startRecording() {
  try {
    stopCurrentAudio(); // --- KEY FEATURE: Interrupt assistant speech
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    mediaRecorder = new MediaRecorder(stream, { mimeType: 'audio/webm' });
    audioChunks = [];
    mediaRecorder.ondataavailable = e => { if (e.data && e.data.size > 0) audioChunks.push(e.data); };
    mediaRecorder.onstop = () => {
      stream.getTracks().forEach(t => t.stop());
      if (socket && socket.readyState === WebSocket.OPEN && audioChunks.length > 0) {
        const audioBlob = new Blob(audioChunks, { type: 'audio/webm' });
        socket.send(audioBlob);
        updateStatus('Transcribing and thinking...');
      }
    };
    mediaRecorder.start();
    isListening = true;
    micBtn.classList.add('listening');
    updateStatus('Listening... tap to stop.');
  } catch (e) {
    console.error('Mic error', e);
    updateStatus('Microphone access denied.');
  }
}

function stopRecording() {
  if (mediaRecorder && mediaRecorder.state === 'recording') { mediaRecorder.stop(); }
  isListening = false;
  micBtn.classList.remove('listening');
  updateStatus('Processing...');
}

async function checkAuth(){
  const r = await fetch('/gmail/status'); const j = await r.json();
  if (j.connected) {
    document.getElementById('auth-view').style.display = 'none';
    document.getElementById('controls').style.display = 'block';
    micBtn.disabled = false;
    updateStatus('Tap the mic to start.');
  } else {
    document.getElementById('auth-view').style.display = 'block';
    document.getElementById('controls').style.display = 'none';
  }
}

function connectWebSocket(){
  return new Promise((resolve, reject) => {
    updateStatus('Connecting to assistant...');
    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    socket = new WebSocket(`${proto}//${window.location.host}/ws`);

    socket.onopen = () => {
      console.log('WebSocket connected.');
      resolve();
    };
    socket.onclose = () => {
      console.log('WebSocket disconnected.');
      updateStatus('Session ended. Tap mic to reconnect.');
      micBtn.disabled = false;
    };
    socket.onerror = (err) => {
      console.error('WebSocket Error:', err);
      updateStatus('Connection error. Please refresh.');
      micBtn.disabled = false;
      reject(err);
    };
    socket.onmessage = (event) => {
      let msg;
      try { msg = JSON.parse(event.data); } catch { return; }

      switch (msg.type) {
        case 'play_audio':
          stopCurrentAudio();
          updateStatus(msg.status_text);
          currentAudio = new Audio(msg.url);
          currentAudio.play();
          currentAudio.onended = () => { currentAudio = null; };
          break;
        case 'update_status': updateStatus(msg.text); break;
        case 'conversation_end':
          updateStatus(msg.text);
          socket.close();
          break;
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

// Initial check
checkAuth();
</script>
</body></html>
"""

# ======================= OpenAI & Gmail Helpers =======================

async def tts_any(text: str) -> str:
    """Generates audio via TTS, stores it, returns a URL."""
    payload = {
        "model": "tts-1", "voice": REALTIME_VOICE, "input": text, "response_format": "mp3"
    }
    r = await _client().post(f"{OPENAI_BASE_URL.rstrip('/')}/v1/audio/speech", json=payload)
    r.raise_for_status()
    audio_id = str(uuid.uuid4())
    _GENERATED_AUDIO[audio_id] = r.content
    return f"/audio/{audio_id}"

async def transcribe_bytes(audio_bytes: bytes) -> str:
    files = {"file": ("speech.webm", audio_bytes, "audio/webm")}
    data = {"model": "whisper-1"}
    r = await _client().post(f"{OPENAI_BASE_URL.rstrip('/')}/v1/audio/transcriptions", data=data, files=files)
    r.raise_for_status()
    return r.json().get("text", "").strip()

def _require_gmail() -> Credentials:
    global _GMAIL_CREDS
    if not _GMAIL_CREDS or not _GMAIL_CREDS.valid:
        raise RuntimeError("Gmail not connected. Go to / to authenticate.")
    return _GMAIL_CREDS

def _gmail_service() -> Any:
    return build("gmail", "v1", credentials=_require_gmail(), cache_discovery=False)

def _get_email_body(msg: Dict) -> str:
    """Extracts the text/plain body from a Gmail message payload."""
    body_data = ""
    if 'parts' in msg.get('payload', {}):
        for part in msg['payload']['parts']:
            if part['mimeType'] == 'text/plain' and 'data' in part['body']:
                body_data = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
                break
    if not body_data and 'data' in msg.get('payload', {}).get('body', {}):
        body_data = base64.urlsafe_b64decode(msg['payload']['body']['data']).decode('utf-8')
    return body_data


# ======================= Conversational Logic (Agentic) =======================

SYSTEM_PROMPT = """You are a helpful, conversational Gmail voice assistant.
- Your goal is to help the user manage their inbox using your voice.
- You have tools to search, read, summarize, reply, compose, delete, and archive emails.
- After using a tool, you get a result. ALWAYS report this result to the user conversationally. For example, "I found 3 emails... The first is from..."
- Be concise. Don't add conversational filler.
- `read_email` gives a brief preview. If the user wants to know more or the email seems long, suggest using `summarize_email` for key points.
- When you `read_email`, it becomes the `current_email_context` for actions like `reply`, `delete`, `summarize`, etc., unless the user specifies another.
- Always inform the user what you're doing or what you've found.
"""

class ConversationManager:
    def __init__(self, ws: WebSocket):
        self.ws = ws
        self.history: List[Dict[str, Any]] = [{"role": "system", "content": SYSTEM_PROMPT}]
        self.last_draft: Optional[Dict[str, str]] = None
        self.current_email_context: Optional[Dict[str, str]] = None
        self.service = _gmail_service() if (_GMAIL_CREDS and _GMAIL_CREDS.valid) else None

    # ---------- UI & Audio Helpers ----------
    async def send_audio_response(self, text: str, status_text: str):
        await self.ws.send_json({"type": "chat_append", "role": "assistant", "text": text})
        audio_url = await tts_any(text)
        await self.ws.send_json({"type": "play_audio", "url": audio_url, "status_text": status_text})

    async def update_status(self, text: str):
        await self.ws.send_json({"type": "update_status", "text": text})

    async def append_chat(self, role: str, text: str):
        await self.ws.send_json({"type": "chat_append", "role": role, "text": text})

    async def update_context_display(self):
        await self.ws.send_json({"type": "context_update", "context": self.current_email_context})

    async def show_draft(self, to: str, subject: str, body: str):
        self.last_draft = {"to": to, "subject": subject, "body": body}
        await self.ws.send_json({"type": "draft_preview", "to": to, "subject": subject, "body": body})

    async def clear_draft(self):
        self.last_draft = None
        await self.ws.send_json({"type": "draft_clear"})

    # ---------- Agent Tool Definitions ----------
    @property
    def tools(self):
        return [
            {
                "type": "function",
                "function": {
                    "name": "gmail_search_emails",
                    "description": "Searches for emails in the user's inbox based on a query.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {"type": "string", "description": "Gmail search query (e.g., 'from:elon@musk.com is:unread')."},
                            "max_results": {"type": "integer", "description": "Maximum number of emails to return.", "default": 5},
                        },
                        "required": ["query"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "gmail_read_email",
                    "description": "Reads a brief preview of an email and sets it as the current context.",
                    "parameters": {
                        "type": "object",
                        "properties": { "message_id": {"type": "string", "description": "The ID of the message to read. If not provided, uses the current context."} },
                        "required": []
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "gmail_summarize_email",
                    "description": "Generates a concise summary of the email currently in context. Use this for long emails or when the user wants the key points.",
                    "parameters": {"type": "object", "properties": {}},
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "gmail_draft_new_email",
                    "description": "Creates a draft for a new email (not a reply).",
                    "parameters": {
                        "type": "object", "properties": {
                            "to": {"type": "string", "description": "The recipient's email address."},
                            "subject": {"type": "string", "description": "The subject of the email."},
                            "body": {"type": "string", "description": "The content of the email."}
                        }, "required": ["to", "subject", "body"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "gmail_draft_reply",
                    "description": "Creates a draft reply to the email in the current context.",
                    "parameters": {
                        "type": "object", "properties": { "body": {"type": "string", "description": "The content of the email reply."} },
                        "required": ["body"]
                    }
                }
            },
            { "type": "function", "function": { "name": "gmail_send_draft", "description": "Sends the most recently created draft.", "parameters": { "type": "object", "properties": {} } } },
            {
                "type": "function",
                "function": {
                    "name": "gmail_delete_email",
                    "description": "Deletes an email. This is permanent after 30 days.",
                    "parameters": {
                        "type": "object", "properties": { "message_id": {"type": "string", "description": "The ID of the message to delete. If not provided, uses the current context."} },
                        "required": []
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "gmail_archive_email",
                    "description": "Archives an email by removing it from the inbox.",
                    "parameters": {
                        "type": "object", "properties": { "message_id": {"type": "string", "description": "The ID of the message to archive. If not provided, uses the current context."} },
                        "required": []
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "gmail_mark_as_read",
                    "description": "Marks an email as read.",
                     "parameters": {
                        "type": "object", "properties": { "message_id": {"type": "string", "description": "The ID of the message to mark as read. If not provided, uses the current context."} },
                        "required": []
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "gmail_mark_as_unread",
                    "description": "Marks an email as unread.",
                     "parameters": {
                        "type": "object", "properties": { "message_id": {"type": "string", "description": "The ID of the message to mark as unread. If not provided, uses the current context."} },
                        "required": []
                    }
                }
            }
        ]

    # ---------- Tool Implementations (Gmail API Calls) ----------
    def _parse_headers(self, headers: List[Dict]) -> Dict[str, str]:
        return {h['name'].lower(): h['value'] for h in headers}

    async def gmail_search_emails(self, query: str, max_results: int = 5) -> str:
        results = self.service.users().messages().list(userId='me', q=query, maxResults=max_results).execute()
        messages = results.get('messages', [])
        if not messages:
            return f"No emails found matching your search: '{query}'"

        email_list = []
        for msg in messages:
            meta = self.service.users().messages().get(userId='me', id=msg['id'], format='metadata', metadataHeaders=['From', 'Subject']).execute()
            headers = self._parse_headers(meta.get('payload', {}).get('headers', []))
            email_list.append({
                "id": msg['id'],
                "from": headers.get('from', 'Unknown Sender').split('<')[0].strip(),
                "subject": headers.get('subject', '(No Subject)')
            })
        return json.dumps(email_list)

    async def gmail_read_email(self, message_id: Optional[str] = None) -> str:
        target_id = message_id or (self.current_email_context and self.current_email_context.get('id'))
        if not target_id:
            return "Error: No message ID provided and no email in context. Ask the user which email they mean."

        msg = self.service.users().messages().get(userId='me', id=target_id, format='full').execute()
        headers = self._parse_headers(msg.get('payload', {}).get('headers', []))
        body_preview = _get_email_body(msg)[:800]

        self.current_email_context = {
            'id': msg['id'], 'threadId': msg['threadId'], 'from': headers.get('from', ''),
            'to': headers.get('to', ''), 'subject': headers.get('subject', ''),
            'message-id': headers.get('message-id', ''), 'references': headers.get('references', '')
        }
        await self.update_context_display()

        return json.dumps({
            "from": self.current_email_context['from'], "subject": self.current_email_context['subject'],
            "summary": msg.get('snippet', 'Could not load snippet.'), "body_preview": body_preview
        })

    async def gmail_summarize_email(self) -> str:
        if not self.current_email_context or not self.current_email_context.get('id'):
            return "Error: No email in context to summarize. Please read an email first."
        
        target_id = self.current_email_context['id']
        msg = self.service.users().messages().get(userId='me', id=target_id, format='full').execute()
        full_body = _get_email_body(msg)

        if not full_body:
            return "Could not find any content to summarize in this email."
        
        # Use the LLM to perform the summarization
        summarization_prompt = f"Summarize the following email content concisely, focusing on the key points, action items, and overall sentiment. The user is hearing this summary, so make it natural and easy to understand.\n\nEMAIL CONTENT:\n---\n{full_body}\n---"
        payload = {
            "model": REALTIME_MODEL,
            "messages": [{"role": "user", "content": summarization_prompt}],
            "temperature": 0.2
        }
        r = await _client().post(f"{OPENAI_BASE_URL.rstrip('/')}/v1/chat/completions", json=payload)
        r.raise_for_status()
        summary = r.json()["choices"][0]["message"]["content"]
        return summary

    async def gmail_draft_new_email(self, to: str, subject: str, body: str) -> str:
        self.current_email_context = None
        await self.update_context_display()
        await self.show_draft(to, subject, body)
        return "New email draft created and shown to the user. Ask them to confirm sending."

    async def gmail_draft_reply(self, body: str) -> str:
        if not self.current_email_context:
            return "Error: No email in context to reply to. Please read an email first."
        subject = self.current_email_context.get('subject', '')
        if not subject.lower().startswith("re:"):
            subject = f"Re: {subject}"
        await self.show_draft(self.current_email_context['from'], subject, body)
        return "Reply draft created. Ask user to confirm."

    async def gmail_send_draft(self) -> str:
        if not self.last_draft:
            return "Error: No draft available to send."
        try:
            profile = self.service.users().getProfile(userId='me').execute()
            message = EmailMessage()
            message.set_content(self.last_draft['body'])
            message['To'] = self.last_draft['to']
            message['From'] = profile['emailAddress']
            message['Subject'] = self.last_draft['subject']

            if self.current_email_context and self.current_email_context.get('message-id'):
                message['In-Reply-To'] = self.current_email_context['message-id']
                refs = self.current_email_context.get('references', '').strip()
                message['References'] = (refs + " " if refs else "") + self.current_email_context['message-id']
                body = {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode(), 'threadId': self.current_email_context['threadId']}
                self.service.users().messages().send(userId='me', body=body).execute()
                await self.gmail_mark_as_read(self.current_email_context['id'])
                result_msg = "Reply sent and the original email has been marked as read."
            else:
                body = {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}
                self.service.users().messages().send(userId='me', body=body).execute()
                result_msg = "Email sent successfully."

            await self.clear_draft()
            self.current_email_context = None
            await self.update_context_display()
            return result_msg
        except HttpError as e:
            return f"Error sending email: {e}"

    async def _context_action(self, message_id: Optional[str], action_func: callable, success_msg: str, clear_ctx: bool = True) -> str:
        target_id = message_id or (self.current_email_context and self.current_email_context.get('id'))
        if not target_id:
            return "Error: No message ID provided and no email in context."
        action_func(userId='me', id=target_id).execute()
        if clear_ctx and self.current_email_context and self.current_email_context.get('id') == target_id:
            self.current_email_context = None
            await self.update_context_display()
        return success_msg.format(id=target_id)

    async def gmail_delete_email(self, message_id: Optional[str] = None) -> str:
        return await self._context_action(message_id, self.service.users().messages().trash, "Email {id} deleted.")

    async def gmail_archive_email(self, message_id: Optional[str] = None) -> str:
        action = lambda **kwargs: self.service.users().messages().modify(**kwargs, body={'removeLabelIds': ['INBOX']})
        return await self._context_action(message_id, action, "Email {id} archived.")

    async def gmail_mark_as_read(self, message_id: Optional[str] = None) -> str:
        action = lambda **kwargs: self.service.users().messages().modify(**kwargs, body={'removeLabelIds': ['UNREAD']})
        return await self._context_action(message_id, action, "Email {id} marked as read.", clear_ctx=False)

    async def gmail_mark_as_unread(self, message_id: Optional[str] = None) -> str:
        action = lambda **kwargs: self.service.users().messages().modify(**kwargs, body={'addLabelIds': ['UNREAD']})
        return await self._context_action(message_id, action, "Email {id} marked as unread.", clear_ctx=False)

    # ---------- Main Agent Loop ----------
    async def start(self):
        if not self.service:
            await self.send_audio_response("Gmail is not connected. Please connect it on the web page first.", "Authentication required.")
            await self.ws.close()
            return
        initial_greeting = "Hello! I'm your Gmail assistant. How can I help? You can ask me to search, summarize, or compose emails."
        self.history.append({"role": "assistant", "content": initial_greeting})
        await self.send_audio_response(initial_greeting, "Ready for your command...")

    async def process_user_message(self, transcript: str):
        await self.append_chat("user", transcript)
        self.history.append({"role": "user", "content": transcript})

        try:
            payload = {"model": REALTIME_MODEL, "messages": self.history, "tools": self.tools, "tool_choice": "auto"}
            r = await _client().post(f"{OPENAI_BASE_URL.rstrip('/')}/v1/chat/completions", json=payload)
            r.raise_for_status()
            response_message = r.json()["choices"][0]["message"]
            self.history.append(response_message)

            if response_message.get("tool_calls"):
                await self.execute_tool_calls(response_message["tool_calls"])
            else:
                await self.send_audio_response(response_message["content"], "Tap the mic to reply...")

        except Exception as e:
            print(f"[AGENT ERROR] {traceback.format_exc()}")
            await self.send_audio_response(f"I encountered an unexpected error. Please try again.", "Error")

    async def execute_tool_calls(self, tool_calls: List[Dict]):
        tool_functions = {
            "gmail_search_emails": self.gmail_search_emails, "gmail_read_email": self.gmail_read_email,
            "gmail_summarize_email": self.gmail_summarize_email,
            "gmail_draft_new_email": self.gmail_draft_new_email, "gmail_draft_reply": self.gmail_draft_reply,
            "gmail_send_draft": self.gmail_send_draft, "gmail_delete_email": self.gmail_delete_email,
            "gmail_archive_email": self.gmail_archive_email, "gmail_mark_as_read": self.gmail_mark_as_read,
            "gmail_mark_as_unread": self.gmail_mark_as_unread,
        }

        for tool_call in tool_calls:
            function_name = tool_call['function']['name']
            function_to_call = tool_functions.get(function_name)
            function_args = json.loads(tool_call['function']['arguments'])
            function_response = ""

            await self.append_chat("system", f"Calling tool: {function_name}({json.dumps(function_args, indent=2)})")

            try:
                if function_to_call:
                    function_response = await function_to_call(**function_args)
                else:
                    function_response = f"Error: Tool '{function_name}' not found."
            except Exception as e:
                print(f"[TOOL EXECUTION ERROR] in {function_name}: {traceback.format_exc()}")
                function_response = f"Error executing tool '{function_name}': {e}"

            self.history.append({"tool_call_id": tool_call['id'], "role": "tool", "name": function_name, "content": function_response})

        payload = {"model": REALTIME_MODEL, "messages": self.history}
        r = await _client().post(f"{OPENAI_BASE_URL.rstrip('/')}/v1/chat/completions", json=payload)
        r.raise_for_status()
        final_response = r.json()["choices"][0]["message"]

        self.history.append(final_response)
        await self.send_audio_response(final_response["content"], "Tap the mic to reply...")

    async def handle_ws_packet(self, data: Dict[str, Any]):
        action = (data.get("action") or "").lower()
        if action == "send_draft":
            await self.process_user_message("Yes, go ahead and send the draft.")
        elif action == "cancel_draft":
            await self.clear_draft()
            await self.process_user_message("Cancel the draft I was working on.")

# ======================= FastAPI Endpoints =======================

@app.get("/", response_class=HTMLResponse)
async def home():
    return HTMLResponse(CONVERSATIONAL_HTML)

@app.get("/audio/{audio_id}")
async def get_audio(audio_id: str):
    audio_bytes = _GENERATED_AUDIO.pop(audio_id, None)
    if audio_bytes:
        return StreamingResponse(io.BytesIO(audio_bytes), media_type="audio/mpeg")
    return PlainTextResponse("Not Found", status_code=404)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    manager = ConversationManager(websocket)
    await manager.start()

    try:
        while True:
            packet = await websocket.receive()
            if packet.get("type") == "websocket.disconnect":
                break
            if packet.get("bytes"):
                audio_bytes = packet["bytes"]
                transcript = ""
                try:
                    transcript = await transcribe_bytes(audio_bytes)
                except Exception as e:
                    print(f"[STT ERROR] {e}")
                if not transcript:
                    await manager.send_audio_response("Sorry, I didn't catch that. Could you say it again?", "Didn't hear you...")
                    continue
                await manager.process_user_message(transcript)

            elif packet.get("text"):
                try:
                    data = json.loads(packet["text"])
                    await manager.handle_ws_packet(data)
                except Exception:
                    continue
    except WebSocketDisconnect:
        print("Client disconnected.")
    finally:
        pass

# --- Gmail OAuth Flow ---
@app.get("/gmail/status")
def gmail_status():
    ok = bool(_GMAIL_CREDS and _GMAIL_CREDS.valid)
    return {"connected": ok}

@app.get("/gmail/login")
def gmail_login(request: Request):
    app.state.oauth_flow_request = request
    cfg = {"web": {
        "client_id": GOOGLE_CLIENT_ID, "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uris": [GOOGLE_REDIRECT_URI],
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token"
    }}
    flow = Flow.from_client_config(cfg, scopes=GMAIL_SCOPES, redirect_uri=GOOGLE_REDIRECT_URI)
    auth_url, state = flow.authorization_url(access_type="offline", include_granted_scopes="true", prompt="consent")
    app.state.oauth_state = state
    return RedirectResponse(auth_url)

@app.get("/gmail/oauth2callback")
async def gmail_oauth2callback(code: str, state: str, request: Request):
    if state != getattr(app.state, "oauth_state", None):
        return PlainTextResponse("Invalid state", status_code=400)

    cfg = {"web": {
        "client_id": GOOGLE_CLIENT_ID, "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uris": [GOOGLE_REDIRECT_URI],
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token"
    }}
    flow = Flow.from_client_config(cfg, scopes=GMAIL_SCOPES, state=state, redirect_uri=GOOGLE_REDIRECT_URI)
    flow.fetch_token(code=code)

    global _GMAIL_CREDS
    _GMAIL_CREDS = flow.credentials
    return RedirectResponse("/")