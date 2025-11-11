# app.py
"""
Conversational Gmail Voice Assistant (Single File)
- A voice-first interface for managing your Gmail primary inbox.
- Greets you and summarizes your unread emails upon starting.
- Engages in a continuous conversation to read, reply to, or manage emails.
- Uses WebSockets for real-time, back-and-forth audio communication.

Install:
  pip install fastapi uvicorn "websockets>=12" httpx python-dotenv \
              google-auth google-auth-oauthlib google-api-python-client

Run:
  uvicorn app:app --host 0.0.0.0 --port 8000 --reload

Env (.env):
  OPENAI_API_KEY=...
  OPENAI_BASE_URL=https://api.openai.com
  REALTIME_MODEL=gpt-4o-mini
  REALTIME_VOICE=aria
  # Gmail OAuth (Needs gmail.modify scope)
  GOOGLE_CLIENT_ID=xxxxxxxx.apps.googleusercontent.com
  GOOGLE_CLIENT_SECRET=xxxxxxxx
  GOOGLE_REDIRECT_URI=http://localhost:8000/gmail/oauth2callback
"""

import os, io, json, base64, re, uuid
from typing import Optional, List, Dict, Any
from email.message import EmailMessage

import httpx, websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, PlainTextResponse, RedirectResponse
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
REALTIME_MODEL = os.getenv("REALTIME_MODEL", "gpt-4o-mini")
REALTIME_VOICE = os.getenv("REALTIME_VOICE", "aria")

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:8000/gmail/oauth2callback")
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.modify"] # Read, send, and modify

if not OPENAI_API_KEY:
    raise RuntimeError("Set OPENAI_API_KEY in environment")

app = FastAPI()

# In-memory demo state (single user)
_GMAIL_CREDS: Optional[Credentials] = None
_GENERATED_AUDIO: Dict[str, bytes] = {} # Store audio clips by UUID

# ======================= UI / HTML Page =======================

CONVERSATIONAL_HTML = """
<!doctype html><html><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Gmail Voice Assistant</title>
<style>
body{font-family:system-ui,sans-serif;background:#f0f2f5;color:#1c1e21;display:flex;justify-content:center;align-items:center;height:100vh;margin:0}
.container{text-align:center;background:white;padding:40px;border-radius:16px;box-shadow:0 4px 20px rgba(0,0,0,0.1);max-width:500px;width:90%}
h1{font-size:24px;margin-bottom:10px}
p{color:#606770;margin-bottom:25px}
.status{min-height:50px;font-size:16px;margin:20px 0;padding:10px;background:#e7f3ff;border-radius:8px;border:1px solid #cce0ff;display:flex;justify-content:center;align-items:center}
.btn{padding:12px 24px;border:0;background:#007bff;color:#fff;border-radius:8px;cursor:pointer;font-size:16px;transition:background-color .3s}
.btn:hover{background:#0056b3}
.btn:disabled{background:#ccc;cursor:not-allowed}
.auth-link{margin-top:20px;font-size:14px}
</style>
</head><body>
<div class="container">
  <h1>Gmail Voice Assistant</h1>
  <p>Your AI assistant for managing your inbox with just your voice.</p>
  <div id="auth-section">
    <p id="auth-msg">Checking Gmail connection...</p>
    <a class="btn" id="login-btn" href="/gmail/login" style="display:none">Connect Gmail</a>
  </div>
  <div id="assistant-section" style="display:none">
    <button class="btn" id="start-btn" onclick="startAssistant()">Start Assistant</button>
    <div class="status" id="status-box">Ready to start.</div>
  </div>
</div>

<script>
let socket;
let mediaRecorder;
let audioChunks = [];
let isListening = false;

async function checkAuth(){
  const r = await fetch('/gmail/status'); const j = await r.json();
  if(j.connected){
    document.getElementById('auth-section').style.display = 'none';
    document.getElementById('assistant-section').style.display = 'block';
  } else {
    document.getElementById('auth-msg').textContent = 'Please connect your Gmail account to begin.';
    document.getElementById('login-btn').style.display = 'inline-block';
  }
}

function updateStatus(text, listening=false){
  document.getElementById('status-box').textContent = text;
  isListening = listening;
}

function startAssistant(){
  document.getElementById('start-btn').disabled = true;
  updateStatus('Connecting to assistant...');
  
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  socket = new WebSocket(`${proto}//${window.location.host}/ws`);

  socket.onopen = () => updateStatus('Connected! Initializing...');
  socket.onclose = () => {
    updateStatus('Session ended.');
    document.getElementById('start-btn').disabled = false;
  };
  socket.onerror = (err) => {
    console.error('WebSocket Error:', err);
    updateStatus('Connection error. Please refresh.');
    document.getElementById('start-btn').disabled = false;
  };
  socket.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    if(msg.type === 'play_audio'){
      updateStatus(msg.status_text);
      const audio = new Audio(msg.url);
      audio.play();
      audio.onended = () => {
        if (msg.prompt_user) {
          startRecording();
        }
      };
    } else if (msg.type === 'update_status') {
        updateStatus(msg.text);
    } else if (msg.type === 'conversation_end'){
        updateStatus(msg.text);
        socket.close();
    }
  };
}

async function startRecording() {
  try {
    updateStatus('Listening...', true);
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    mediaRecorder = new MediaRecorder(stream, { mimeType: 'audio/webm' });
    audioChunks = [];
    mediaRecorder.ondataavailable = e => audioChunks.push(e.data);
    mediaRecorder.onstop = () => {
      stream.getTracks().forEach(track => track.stop());
      if (socket && socket.readyState === WebSocket.OPEN && audioChunks.length > 0) {
        const audioBlob = new Blob(audioChunks, { type: 'audio/webm' });
        socket.send(audioBlob);
        updateStatus('Thinking...');
      }
      isListening = false;
    };
    mediaRecorder.start();
    // Stop recording after 5 seconds of silence or max duration
    setTimeout(() => { if (mediaRecorder.state === 'recording') mediaRecorder.stop(); }, 5000);
  } catch (e) {
    console.error('Mic error', e);
    updateStatus('Microphone access denied.');
  }
}

checkAuth();
</script>
</body></html>
"""


# ======================= OpenAI & Gmail Helpers =======================

async def tts_any(text: str) -> str:
    """Generates audio, stores it, and returns its unique URL."""
    voice = REALTIME_VOICE
    url = f"{OPENAI_BASE_URL}/v1/audio/speech"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": "gpt-4o-mini-tts", "voice": voice, "input": text, "response_format": "mp3"}
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(url, headers=headers, json=payload)
        r.raise_for_status()
        audio_bytes = r.content
    
    audio_id = str(uuid.uuid4())
    _GENERATED_AUDIO[audio_id] = audio_bytes
    return f"/audio/{audio_id}"


async def transcribe_bytes(audio_bytes: bytes) -> str:
    url = f"{OPENAI_BASE_URL}/v1/audio/transcriptions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    files = {"file": ("command.webm", audio_bytes, "audio/webm")}
    data = {"model": "whisper-1"}
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(url, headers=headers, data=data, files=files)
        r.raise_for_status()
        return r.json().get("text", "").strip()


async def process_command(transcript: str, context: Dict[str, Any]) -> Dict[str, Any]:
    """Interprets user command using GPT-4o-mini with conversation context."""
    email_list_str = "\n".join([f'{i+1}. From: {e["from"]}, Subject: {e["subject"]}' for i, e in enumerate(context["emails"])])
    
    system_prompt = f"""
You are a voice assistant command processor for Gmail. Analyze the user's transcript and the current context to determine the correct action. Respond with a JSON object.

CONTEXT:
- Current State: {context['state']}
- Current Email Index: {context.get('current_index', 'None')}
- Unread Emails List (for context only, indices are 1-based):
{email_list_str if email_list_str else "No emails to process."}

POSSIBLE ACTIONS:
1.  read_email: User wants to hear an email.
    - `index`: The 0-based index of the email to read.
    - Handle phrases like "read the first one" (index 0), "the third one" (index 2), or by matching sender/subject.
2.  reply: User wants to reply to the current email.
    - `content`: The dictated message. If the transcript is just "reply", the content should be null, as you need to prompt for the message.
3.  confirm: User acknowledges/is done with the current email (marks it as read).
4.  next_email: User wants to move to the next email.
5.  stop: User wants to end the session.
6.  unknown: The command is unclear.

Your JSON output must have one key: "action", with a nested object containing action details.

EXAMPLES:
- User: "read the first one" -> {{"action": {{"type": "read_email", "index": 0}}}}
- User: "what's the one from booking.com about" -> {{"action": {{"type": "read_email", "index": ...}}}}
- (State=AWAITING_ACTION) User: "okay got it" -> {{"action": {{"type": "confirm"}}}}
- (State=AWAITING_ACTION) User: "reply" -> {{"action": {{"type": "reply", "content": null}}}}
- (State=PROMPTING_REPLY) User: "I'll be there in 15 minutes, thanks" -> {{"action": {{"type": "reply", "content": "I'll be there in 15 minutes, thanks"}}}}
- User: "stop" -> {{"action": {{"type": "stop"}}}}
"""

    url = f"{OPENAI_BASE_URL}/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": transcript}
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.1
    }
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(url, headers=headers, json=payload)
        r.raise_for_status()
        return json.loads(r.json()["choices"][0]["message"]["content"])


def _require_gmail() -> Credentials:
    global _GMAIL_CREDS
    if not _GMAIL_CREDS or not _GMAIL_CREDS.valid:
        raise RuntimeError("Gmail not connected. Go to / to authenticate.")
    return _GMAIL_CREDS

def _gmail_service() -> Any:
    return build("gmail", "v1", credentials=_require_gmail(), cache_discovery=False)

# ======================= Conversational Logic =======================

class ConversationManager:
    def __init__(self, ws: WebSocket):
        self.ws = ws
        self.state = "INIT"  # INIT -> GREETING -> AWAITING_COMMAND -> READING -> AWAITING_ACTION -> PROMPTING_REPLY -> END
        self.emails = []
        self.current_index = -1
        try:
            self.service = _gmail_service()
        except RuntimeError:
            self.service = None

    async def send_audio_response(self, text: str, status_text: str, prompt_user: bool = True):
        audio_url = await tts_any(text)
        await self.ws.send_json({
            "type": "play_audio",
            "url": audio_url,
            "status_text": status_text,
            "prompt_user": prompt_user
        })
        
    async def send_status_update(self, text: str):
        await self.ws.send_json({"type": "update_status", "text": text})

    async def end_conversation(self, text: str):
        audio_url = await tts_any(text)
        await self.ws.send_json({
            "type": "play_audio",
            "url": audio_url,
            "status_text": "Session ended.",
            "prompt_user": False
        })
        await self.ws.close()

    def _parse_headers(self, payload_headers: List[Dict[str, str]]) -> Dict[str, str]:
        h = {k["name"].lower(): k["value"] for k in payload_headers}
        return {"from": h.get("from", ""), "subject": h.get("subject", ""), "date": h.get("date", ""),
                "to": h.get("to", ""), "message-id": h.get("message-id", ""),"references": h.get("references", "")}

    def _decode_body(self, msg: Dict[str, Any]) -> str:
        payload = msg.get("payload", {})
        parts = payload.get("parts")
        data = payload.get("body", {}).get("data")
        if data:
            return base64.urlsafe_b64decode(data.encode()).decode("utf-8", errors="ignore")
        if parts:
            for p in parts:
                if p.get("mimeType") == "text/plain" and p.get("body", {}).get("data"):
                    return base64.urlsafe_b64decode(p["body"]["data"].encode()).decode("utf-8", errors="ignore")
            for p in parts:
                if p.get("body", {}).get("data"):
                    return base64.urlsafe_b64decode(p["body"]["data"].encode()).decode("utf-8", errors="ignore")
        return ""

    async def start(self):
        if not self.service:
            await self.send_audio_response("Gmail is not connected. Please go to the homepage to authenticate.", "Authentication required.", False)
            await self.ws.close()
            return

        self.state = "GREETING"
        try:
            # Fetch unread emails from the primary inbox only
            results = self.service.users().messages().list(
                userId='me', 
                labelIds=['INBOX', 'UNREAD', 'CATEGORY_PERSONAL'], 
                maxResults=10
            ).execute()
            
            messages = results.get('messages', [])
            if not messages:
                await self.send_audio_response("Your primary inbox has no unread emails. Great job! Goodbye.", "Primary inbox is clear!", False)
                self.state = "END"
                await self.ws.close()
                return

            for msg in messages:
                meta = self.service.users().messages().get(userId='me', id=msg['id'], format='metadata', metadataHeaders=['From', 'Subject']).execute()
                headers = self._parse_headers(meta['payload']['headers'])
                self.emails.append({'id': msg['id'], 'from': headers['from'], 'subject': headers['subject']})
            
            count = len(self.emails)
            summary_list = "; ".join([f"from {e['from'].split('<')[0].strip()}" for e in self.emails[:3]])
            greeting = f"Hello! You have {count} unread emails in your primary inbox. You have messages {summary_list}. What would you like to do?"
            await self.send_audio_response(greeting, f"{count} unread emails in primary. Listening...")
            self.state = "AWAITING_COMMAND"

        except Exception as e:
            print(f"[GMAIL ERROR] {e}")
            await self.send_audio_response("Sorry, I couldn't connect to your Gmail account right now.", "Gmail connection error.", False)
            self.state = "END"
            await self.ws.close()

    async def handle_transcript(self, transcript: str):
        if not transcript:
            await self.send_audio_response("I didn't catch that. Please try again.", "Didn't hear you. Listening again...")
            return

        context = {"state": self.state, "emails": self.emails, "current_index": self.current_index}
        try:
            command_json = await process_command(transcript, context)
            action = command_json.get("action", {})
            action_type = action.get("type")
        except Exception as e:
            print(f"Error processing command: {e}")
            action_type = "unknown"

        # State-based action handling
        if action_type == "read_email":
            self.current_index = action.get("index", 0)
            if 0 <= self.current_index < len(self.emails):
                await self.read_current_email()
            else:
                await self.send_audio_response("I couldn't find that email. Please specify another.", "Email not found. Listening...")

        elif action_type == "confirm":
            await self.mark_as_read_current()

        elif action_type == "next_email":
            if 0 <= self.current_index < len(self.emails) - 1:
                self.current_index += 1
                await self.read_current_email()
            else:
                await self.send_audio_response("That was the last email.", "No more emails. Listening for command...")
                self.state = "AWAITING_COMMAND"

        elif action_type == "reply":
            content = action.get("content")
            if self.state == "PROMPTING_REPLY" and content:
                await self.send_reply(content)
            elif self.state == "AWAITING_ACTION":
                self.state = "PROMPTING_REPLY"
                await self.send_audio_response("Okay, what should the reply say?", "Dictate your reply now...")
            else:
                 await self.send_audio_response("Sorry, I can't reply from this state.", "Invalid command. Listening...")

        elif action_type == "stop":
            await self.end_conversation("Okay, closing the session. Goodbye!")

        else: # unknown
            await self.send_audio_response("I'm not sure how to help with that. You can ask me to read an email, reply, or say stop.", "Command unclear. Listening...")

    async def read_current_email(self):
        self.state = "READING"
        email_info = self.emails[self.current_index]
        await self.send_status_update(f"Fetching email from {email_info['from']}...")
        
        full_msg = self.service.users().messages().get(userId='me', id=email_info['id'], format='full').execute()
        body = self._decode_body(full_msg) or full_msg.get("snippet", "(no body)")
        
        # Summarize for voice
        summary_prompt = f"Concisely summarize this email for a driver. Keep it under 60 words, but include names, dates, and key details.\n\nEMAIL BODY:\n{body[:2000]}"
        response = await httpx.post(f"{OPENAI_BASE_URL}/v1/chat/completions",
                                    headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                                    json={"model": "gpt-4o-mini", "messages": [{"role": "user", "content": summary_prompt}]})
        summary = response.json()["choices"][0]["message"]["content"]

        full_text_to_speak = f"Email from {email_info['from']}. Subject: {email_info['subject']}. Summary: {summary}. What would you like to do? You can say reply, confirm, or next."
        await self.send_audio_response(full_text_to_speak, "Read summary. Awaiting action (reply, confirm, next)...")
        self.state = "AWAITING_ACTION"

    async def mark_as_read_current(self):
        email_id = self.emails[self.current_index]['id']
        self.service.users().messages().modify(userId='me', id=email_id, body={'removeLabelIds': ['UNREAD']}).execute()
        await self.send_audio_response("Okay, marked as read.", "Marked as read. Listening...")
        self.state = "AWAITING_COMMAND" # Go back to general command mode

    async def send_reply(self, content: str):
        original_msg_id = self.emails[self.current_index]['id']
        try:
            original_msg = self.service.users().messages().get(userId="me", id=original_msg_id, format="metadata", metadataHeaders=["Subject", "From", "To", "Message-ID", "References"]).execute()
            headers = self._parse_headers(original_msg["payload"]["headers"])

            message = EmailMessage()
            message.set_content(content)
            
            my_profile = self.service.users().getProfile(userId='me').execute()
            my_email = my_profile['emailAddress']

            # Determine correct recipient
            sender_email_match = re.search(r'<(.*?)>', headers["from"])
            sender_email = sender_email_match.group(1) if sender_email_match else headers["from"]
            message["To"] = sender_email
            message["From"] = my_email
            message["Subject"] = "Re: " + headers["subject"]
            message["In-Reply-To"] = headers["message-id"]
            message["References"] = headers.get("references", "") + " " + headers["message-id"]

            encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
            create_message = {"raw": encoded_message, "threadId": original_msg["threadId"]}
            
            self.service.users().messages().send(userId="me", body=create_message).execute()
            self.service.users().messages().modify(userId='me', id=original_msg_id, body={'removeLabelIds': ['UNREAD']}).execute()
            
            await self.send_audio_response("Your reply has been sent and the email marked as read.", "Reply sent. Listening...")
        except HttpError as error:
            print(f"[GMAIL] Error sending reply: {error}")
            await self.send_audio_response("Sorry, there was an error sending your reply.", "Send error. Listening...")

        self.state = "AWAITING_COMMAND"


# ======================= FastAPI Endpoints =======================

@app.get("/", response_class=HTMLResponse)
async def home():
    return HTMLResponse(CONVERSATIONAL_HTML)

@app.get("/audio/{audio_id}")
async def get_audio(audio_id: str):
    audio_bytes = _GENERATED_AUDIO.get(audio_id)
    if audio_bytes:
        return StreamingResponse(io.BytesIO(audio_bytes), media_type="audio/mpeg")
    return PlainTextResponse("Not Found", status_code=404)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    manager = ConversationManager(websocket)
    await manager.start()

    try:
        while manager.state != "END":
            audio_bytes = await websocket.receive_bytes()
            transcript = await transcribe_bytes(audio_bytes)
            print(f"[TRANSCRIPT]: {transcript}")
            await manager.handle_transcript(transcript)
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
def gmail_login():
    cfg = {"web": {"client_id": GOOGLE_CLIENT_ID, "client_secret": GOOGLE_CLIENT_SECRET, "redirect_uris": [GOOGLE_REDIRECT_URI], "auth_uri": "https://accounts.google.com/o/oauth2/auth", "token_uri": "https://oauth2.googleapis.com/token"}}
    flow = Flow.from_client_config(cfg, scopes=GMAIL_SCOPES, redirect_uri=GOOGLE_REDIRECT_URI)
    auth_url, state = flow.authorization_url(access_type="offline", include_granted_scopes="true", prompt="consent")
    app.state.oauth_state = state
    return RedirectResponse(auth_url)

@app.get("/gmail/oauth2callback")
def gmail_oauth2callback(code: str, state: str, request: Request):
    if state != getattr(app.state, "oauth_state", None):
        return PlainTextResponse("Invalid state", status_code=400)
    cfg = {"web": {"client_id": GOOGLE_CLIENT_ID, "client_secret": GOOGLE_CLIENT_SECRET, "redirect_uris": [GOOGLE_REDIRECT_URI], "auth_uri": "https://accounts.google.com/o/oauth2/auth", "token_uri": "https://oauth2.googleapis.com/token"}}
    flow = Flow.from_client_config(cfg, scopes=GMAIL_SCOPES, state=state, redirect_uri=GOOGLE_REDIRECT_URI)
    flow.fetch_token(code=code)
    global _GMAIL_CREDS
    _GMAIL_CREDS = flow.credentials
    return RedirectResponse("/")