# app.py
"""
Email→Voice Assistant + Gmail Inbox (Single File, Conversational)
- Greets you and announces unread email count on start.
- Guides you through emails via voice commands.
- "Hear all" or select → summarize each + speak (Realtime WS → HTTP fallback)
- After each email, you can speak a command: respond / confirm / ignore

Install:
  pip install fastapi uvicorn httpx "websockets>=12" python-dotenv twilio \
              google-auth google-auth-oauthlib google-api-python-client

Run:
  uvicorn app:app --host 0.0.0.0 --port 8000 --reload

Env (.env):
  (Same as before)
"""

import os, io, json, base64, re
from typing import Optional, List, Dict, Any
from email.message import EmailMessage

import httpx, websockets
from fastapi import FastAPI, Form, UploadFile, Query, Request
from fastapi.responses import (
    HTMLResponse, StreamingResponse, JSONResponse, PlainTextResponse, RedirectResponse
)
from dotenv import load_dotenv

# Twilio (optional – for VoIP page)
from twilio.jwt.access_token import AccessToken
from twilio.jwt.access_token.grants import VoiceGrant
from twilio.twiml.voice_response import VoiceResponse

# Google / Gmail
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


load_dotenv()

# ---------- OpenAI config ----------
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY")
OPENAI_BASE_URL  = os.getenv("OPENAI_BASE_URL", "https://api.openai.com")
REALTIME_MODEL   = os.getenv("REALTIME_MODEL", "gpt-4o-mini")
REALTIME_VOICE   = os.getenv("REALTIME_VOICE", "shimmer") # Let's use a different voice for variety

# ---------- Twilio (optional) ----------
TWILIO_ACCOUNT_SID    = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_API_KEY_SID    = os.getenv("TWILIO_API_KEY_SID")
TWILIO_API_KEY_SECRET = os.getenv("TWILIO_API_KEY_SECRET")
TWIML_APP_SID         = os.getenv("TWIML_APP_SID")

# ---------- Gmail OAuth ----------
GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI  = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:8000/gmail/oauth2callback")
GMAIL_SCOPES         = ["https://www.googleapis.com/auth/gmail.modify"]

if not OPENAI_API_KEY:
    raise RuntimeError("Set OPENAI_API_KEY in environment")

app = FastAPI()

# In-memory demo state (single user)
_LAST_AUDIO: bytes = b""
_GMAIL_CREDS: Optional[Credentials] = None

# ======================= OpenAI Helpers =======================

async def summarize_with_gpt(text: str, max_words: int = 60) -> str:
    system = (
        f"You write extremely concise summaries for drivers. "
        f"Keep it under {max_words} words. Keep names, dates, and amounts."
    )
    url = f"{OPENAI_BASE_URL}/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": "gpt-4o-mini",
               "messages": [{"role": "system", "content": system},
                            {"role": "user", "content": f"Summarize clearly:\n\n{text}"}]}
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(url, headers=headers, json=payload)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"].strip()


async def tts_via_realtime(text: str, voice: Optional[str] = None, fmt: str = "mp3") -> bytes:
    voice = voice or REALTIME_VOICE
    url = f"wss://api.openai.com/v1/realtime?model={REALTIME_MODEL}"
    headers = [("Authorization", f"Bearer {OPENAI_API_KEY}"), ("OpenAI-Beta", "realtime=v1")]
    audio = io.BytesIO()
    async with websockets.connect(url, extra_headers=headers, max_size=None) as ws:
        await ws.send(json.dumps({
            "type": "response.create",
            "response": {"modalities": ["audio"], "instructions": text,
                         "audio": {"voice": voice, "format": fmt}}
        }))
        while True:
            msg = await ws.recv()
            if isinstance(msg, (bytes, bytearray)): audio.write(msg); continue
            try: evt = json.loads(msg)
            except Exception: continue
            t = evt.get("type")
            if t == "response.output_audio.delta":
                b64 = evt.get("delta")
                if b64: audio.write(base64.b64decode(b64))
            elif t == "response.completed": break
            elif t == "error": raise RuntimeError(str(evt))
    return audio.getvalue()

async def tts_http_fallback(text: str, voice: Optional[str] = None, fmt: str = "mp3") -> bytes:
    voice = voice or REALTIME_VOICE
    url = f"{OPENAI_BASE_URL}/v1/audio/speech"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": "tts-1", "voice": voice, "input": text, "response_format": fmt}
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(url, headers=headers, json=payload)
        r.raise_for_status()
        return r.content

async def tts_any(text: str, voice: Optional[str] = None, fmt: str = "mp3") -> bytes:
    try:
        return await tts_via_realtime(text, voice=voice, fmt=fmt)
    except Exception as e:
        print(f"[Realtime failed → fallback]:", e)
        return await tts_http_fallback(text, voice=voice, fmt=fmt)

async def transcribe_bytes(audio_bytes: bytes, filename: str = "audio.mp3") -> str:
    url = f"{OPENAI_BASE_URL}/v1/audio/transcriptions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    files = {"file": (filename, audio_bytes, "audio/mpeg")}
    data = {"model": "whisper-1"}
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(url, headers=headers, data=data, files=files)
        r.raise_for_status()
        return r.json().get("text", "").strip()

async def interpret_intent(text: str, context: str) -> str:
    system_prompt = ""
    if context == "initial_greeting":
        system_prompt = (
            "You are an intent detection system. The user is responding to the question 'Would you like to hear your emails?'. "
            "If they say yes, 'sure', 'okay', 'proceed', etc., output 'PROCEED'. "
            "If they say no, 'not now', 'later', etc., output 'DECLINE'. "
            "Otherwise, output 'UNKNOWN'."
        )
    else: # context is an email_id
        system_prompt = (
            "You are an intent detection system for a voice-based email client. "
            "The possible actions are: 'reply' or 'confirm'.\n"
            "- If the command is to reply, output 'REPLY:' followed by the verbatim message. Example: 'REPLY: I will be there in 10 minutes.'\n"
            "- If the command is to confirm, acknowledge, or mark as read (e.g., 'got it', 'okay', 'confirm', 'next'), output 'CONFIRM'.\n"
            "- Otherwise, output 'UNKNOWN'."
        )

    url = f"{OPENAI_BASE_URL}/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    messages = [{"role": "system", "content": system_prompt}, {"role": "user", "content": text}]
    payload = {"model": "gpt-4o-mini", "messages": messages, "temperature": 0.1}
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(url, headers=headers, json=payload)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"].strip()


# ======================= Gmail Helpers =======================
def _require_gmail() -> Credentials:
    global _GMAIL_CREDS
    if not _GMAIL_CREDS or not _GMAIL_CREDS.valid:
        raise RuntimeError("Gmail not connected. Go to /gmail/login.")
    return _GMAIL_CREDS

def _gmail_service() -> Any:
    creds = _require_gmail()
    return build("gmail", "v1", credentials=creds, cache_discovery=False)

def _parse_headers(payload_headers: List[Dict[str, str]]) -> Dict[str, str]:
    h = {k["name"].lower(): k["value"] for k in payload_headers}
    return {"from": h.get("from", ""), "subject": h.get("subject", ""), "date": h.get("date", ""),
            "to": h.get("to", ""), "message-id": h.get("message-id", ""), "references": h.get("references", "")}

def _decode_body(msg: Dict[str, Any]) -> str:
    payload = msg.get("payload", {})
    parts = payload.get("parts")
    data = payload.get("body", {}).get("data")
    if data: return base64.urlsafe_b64decode(data.encode()).decode("utf-8", errors="ignore")
    if parts:
        for p in parts:
            if p.get("mimeType") == "text/plain" and p.get("body", {}).get("data"):
                return base64.urlsafe_b64decode(p["body"]["data"].encode()).decode("utf-8", errors="ignore")
        for p in parts:
            if p.get("body", {}).get("data"):
                return base64.urlsafe_b64decode(p["body"]["data"].encode()).decode("utf-8", errors="ignore")
    return ""

def _mark_as_read(service: Any, msg_id: str):
    try:
        service.users().messages().modify(userId="me", id=msg_id, body={"removeLabelIds": ["UNREAD"]}).execute()
        print(f"[GMAIL] Marked message {msg_id} as read.")
    except HttpError as error: print(f"[GMAIL] Error marking as read: {error}")

def _create_and_send_reply(service: Any, original_msg_id: str, reply_body: str):
    try:
        original_msg = service.users().messages().get(userId="me", id=original_msg_id, format="metadata",
            metadataHeaders=["Subject", "From", "To", "Message-ID", "References"]).execute()
        headers = _parse_headers(original_msg["payload"]["headers"])
        message = EmailMessage()
        message.set_content(reply_body)
        sender_email = re.search(r'<(.*?)>', headers["from"])
        recipient_email = re.search(r'<(.*?)>', headers["to"])
        my_profile = service.users().getProfile(userId='me').execute()
        my_email = my_profile['emailAddress']
        reply_to_address = sender_email.group(1) if sender_email else headers["from"]
        if my_email in reply_to_address: reply_to_address = recipient_email.group(1) if recipient_email else headers["to"]
        message["To"] = reply_to_address
        message["From"] = my_email
        message["Subject"] = "Re: " + headers["subject"]
        message["In-Reply-To"] = headers["message-id"]
        message["References"] = headers.get("references", "") + " " + headers["message-id"]
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        create_message = {"raw": encoded_message, "threadId": original_msg["threadId"]}
        sent_message = service.users().messages().send(userId="me", body=create_message).execute()
        print(f"[GMAIL] Reply sent: {sent_message['id']}")
    except HttpError as error: print(f"[GMAIL] Error sending reply: {error}")
    except Exception as e: print(f"[GMAIL] General error creating reply: {e}")

# ======================= UI Pages =======================

CONVERSATIONAL_HTML = """
<!doctype html><html><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Email Voice Assistant</title>
<style>
body{font-family:system-ui,Segoe UI,Roboto,Arial,sans-serif;background:#111;color:#fff;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}
.container{text-align:center;padding:20px}
#status{font-size:1.5em;margin-bottom:20px;min-height:2.2em}
#authBox a{color:#4dabf7;text-decoration:none;background:#333;padding:10px 20px;border-radius:8px}
.spinner{border:4px solid rgba(255,255,255,.3);border-radius:50%;border-top:4px solid #fff;width:40px;height:40px;animation:spin 1s linear infinite;margin:20px auto;display:none}
@keyframes spin{0%{transform:rotate(0deg)}100%{transform:rotate(360deg)}}
</style>
</head><body>
<div class="container">
  <h1>Email Voice Assistant</h1>
  <p id="status">Initializing...</p>
  <div class="spinner" id="spinner"></div>
  <div id="authBox" style="display:none;">
    <p id="authMsg"></p>
    <a id="loginBtn" href="/gmail/login">Connect Gmail</a>
  </div>
</div>

<script>
let conversationContext = 'idle';
let unreadEmails = [];
let emailCursor = 0;

const statusEl = document.getElementById('status');
const spinnerEl = document.getElementById('spinner');

async function checkAuth(){
  statusEl.textContent = 'Checking Gmail connection...';
  const r = await fetch('/gmail/status'); const j = await r.json();
  if(j.connected){
    document.getElementById('authBox').style.display='none';
    startConversation();
  }else{
    document.getElementById('authBox').style.display='block';
    statusEl.textContent = 'Please connect your Gmail account to begin.';
    document.getElementById('authMsg').textContent = 'Authentication required.';
  }
}

async function startConversation() {
  statusEl.textContent = "Checking for new mail...";
  const r = await fetch('/gmail/unread');
  unreadEmails = (await r.json()).items;
  
  const greetingAudio = new Audio('/assistant/greeting?cb=' + Date.now());
  await greetingAudio.play();
  
  if (unreadEmails.length > 0) {
    conversationContext = 'initial_greeting';
    greetingAudio.onended = () => listenForCommand();
  } else {
    statusEl.textContent = "No unread emails. Have a great day!";
  }
}

async function processNextEmail() {
  if (emailCursor >= unreadEmails.length) {
    statusEl.textContent = "You're all caught up!";
    const outroAudio = new Audio('/assistant/outro?cb=' + Date.now());
    await outroAudio.play();
    conversationContext = 'idle';
    return;
  }
  const email = unreadEmails[emailCursor];
  conversationContext = email.id;
  
  statusEl.textContent = `Reading email ${emailCursor + 1} of ${unreadEmails.length}...`;
  await fetch('/gmail/speak?id='+encodeURIComponent(email.id), {method:'POST'});
  const summaryAudio = new Audio('/audio.mp3?cb=' + Date.now());
  await summaryAudio.play();
  summaryAudio.onended = () => listenForCommand();
}

async function listenForCommand() {
    statusEl.textContent = "Listening for your command...";
    spinnerEl.style.display = 'block';
    try {
        const stream = await navigator.mediaDevices.getUserMedia({audio: true});
        const rec = new MediaRecorder(stream, {mimeType: 'audio/webm'});
        let chunks = [];
        rec.ondataavailable = e => chunks.push(e.data);
        rec.onstop = async () => {
            stream.getTracks().forEach(track => track.stop());
            spinnerEl.style.display = 'none';
            if (chunks.length === 0) {
                statusEl.textContent = "Didn't hear anything.";
                return;
            }
            statusEl.textContent = 'Processing...';
            const blob = new Blob(chunks, {type: 'audio/webm'});
            const fd = new FormData();
            fd.append('file', blob, 'command.webm');
            fd.append('context', conversationContext);

            const res = await fetch('/assistant/command', {method: 'POST', body: fd});
            const data = await res.json();

            const feedbackAudio = new Audio(data.audio_url + '?cb=' + Date.now());
            await feedbackAudio.play();
            statusEl.textContent = data.text_feedback;
            
            feedbackAudio.onended = () => {
                if(data.action === 'PROCEED') {
                    emailCursor = 0;
                    processNextEmail();
                } else if (data.action === 'CONTINUE') {
                    emailCursor++;
                    processNextEmail();
                } else if (data.action === 'DECLINE' || data.action === 'END') {
                    statusEl.textContent = "Okay, have a great day!";
                }
            };
        };
        rec.start();
        setTimeout(() => { if (rec.state === 'recording') rec.stop(); }, 5000);
    } catch (e) {
        console.warn('Mic err', e);
        statusEl.textContent = 'Microphone permission denied.';
        spinnerEl.style.display = 'none';
    }
}

window.onload = checkAuth;
</script>
</body></html>
"""

# ======================= Base Pages & API =======================

@app.get("/", response_class=RedirectResponse)
async def home():
    return RedirectResponse("/inbox")

@app.get("/inbox", response_class=HTMLResponse)
async def inbox_page():
    return HTMLResponse(CONVERSATIONAL_HTML)

@app.get("/audio.mp3")
async def audio_mp3():
    return StreamingResponse(io.BytesIO(_LAST_AUDIO), media_type="audio/mpeg")

# ======================= Gmail Flow =======================
@app.get("/gmail/status")
def gmail_status():
    ok = bool(_GMAIL_CREDS and _GMAIL_CREDS.valid)
    return {"connected": ok}

@app.get("/gmail/login")
def gmail_login():
    cfg = {"web": {"client_id": GOOGLE_CLIENT_ID, "project_id": "gmail-voice-demo",
                   "auth_uri": "https://accounts.google.com/o/oauth2/auth", "token_uri": "https://oauth2.googleapis.com/token",
                   "client_secret": GOOGLE_CLIENT_SECRET, "redirect_uris": [GOOGLE_REDIRECT_URI]}}
    flow = Flow.from_client_config(cfg, scopes=GMAIL_SCOPES)
    flow.redirect_uri = GOOGLE_REDIRECT_URI
    auth_url, state = flow.authorization_url(access_type="offline", include_granted_scopes="true", prompt="consent")
    app.state.oauth_state = state
    return RedirectResponse(auth_url)

@app.get("/gmail/oauth2callback")
def gmail_oauth2callback(request: Request, code: str, state: str):
    if state != getattr(app.state, "oauth_state", None): return PlainTextResponse("Invalid state", status_code=400)
    cfg = {"web": {"client_id": GOOGLE_CLIENT_ID, "project_id": "gmail-voice-demo",
                   "auth_uri": "https://accounts.google.com/o/oauth2/auth", "token_uri": "https://oauth2.googleapis.com/token",
                   "client_secret": GOOGLE_CLIENT_SECRET, "redirect_uris": [GOOGLE_REDIRECT_URI]}}
    flow = Flow.from_client_config(cfg, scopes=GMAIL_SCOPES, state=state)
    flow.redirect_uri = GOOGLE_REDIRECT_URI
    flow.fetch_token(code=code)
    global _GMAIL_CREDS
    _GMAIL_CREDS = flow.credentials
    return RedirectResponse("/inbox")

ONLY_PRIMARY = os.getenv("ONLY_PRIMARY", "false").lower() in ("1", "true", "yes")
@app.get("/gmail/unread")
def gmail_unread(max: int = 20):
    svc = _gmail_service()
    base_labels = ["INBOX", "UNREAD"]
    if ONLY_PRIMARY: base_labels.append("CATEGORY_PERSONAL")
    items = []
    try:
        msgs = svc.users().messages().list(userId="me", labelIds=base_labels, maxResults=max,
                                           includeSpamTrash=False).execute().get("messages", [])
        for m in msgs:
            full = svc.users().messages().get(userId="me", id=m["id"], format="metadata",
                metadataHeaders=["From", "Subject", "Date"]).execute()
            h = _parse_headers(full.get("payload", {}).get("headers", []))
            items.append({"id": m["id"], "from": h["from"], "subject": h["subject"], "date": h["date"]})
    except Exception as e: print(f"[GMAIL] list error: {e}")
    return {"count": len(items), "items": items}

@app.post("/gmail/speak")
async def gmail_speak(id: str = Query(...)):
    svc = _gmail_service()
    full = svc.users().messages().get(userId="me", id=id, format="full").execute()
    body = _decode_body(full) or full.get("snippet", "(no body)")
    summary = await summarize_with_gpt(body)
    global _LAST_AUDIO
    _LAST_AUDIO = await tts_any(summary, voice=REALTIME_VOICE)
    return {"ok": True}

# ======================= Conversational Endpoints =======================

@app.get("/assistant/greeting")
async def assistant_greeting():
    try:
        unread_data = gmail_unread()
        count = unread_data["count"]
        if count == 0:
            text = "Hello! You have no unread emails. Looks like you're all caught up."
        elif count == 1:
            text = "Hello! You have one new email. Would you like me to read the summary?"
        else:
            text = f"Hello! You have {count} unread emails. Would you like me to read the summaries?"
        
        global _LAST_AUDIO
        _LAST_AUDIO = await tts_any(text, voice=REALTIME_VOICE)
        return StreamingResponse(io.BytesIO(_LAST_AUDIO), media_type="audio/mpeg")
    except Exception as e:
        print(f"[ERROR] assistant_greeting: {e}")
        return PlainTextResponse("Error generating greeting", status_code=500)

@app.get("/assistant/outro")
async def assistant_outro():
    global _LAST_AUDIO
    _LAST_AUDIO = await tts_any("You're all caught up. Have a great day!", voice=REALTIME_VOICE)
    return StreamingResponse(io.BytesIO(_LAST_AUDIO), media_type="audio/mpeg")

@app.post("/assistant/command")
async def assistant_command(file: UploadFile, context: str = Form(...)):
    try:
        svc = _gmail_service()
        audio_bytes = await file.read()
        transcript = await transcribe_bytes(audio_bytes, file.filename or "command.webm")
        print(f"[COMMAND] Context: {context}, Transcript: '{transcript}'")
        
        if not transcript: raise ValueError("Empty transcript")

        intent = await interpret_intent(transcript, context)
        print(f"[COMMAND] Intent: {intent}")

        feedback_text = "Sorry, I didn't understand."
        action = "REPEAT" # Default action

        if context == 'initial_greeting':
            if intent == 'PROCEED':
                feedback_text = "Okay, starting with the first email."
                action = 'PROCEED'
            elif intent == 'DECLINE':
                feedback_text = "Alright. Let me know when you're ready."
                action = 'DECLINE'
        else: # Context is an email_id
            if intent.startswith("REPLY:"):
                reply_text = intent.split("REPLY:", 1)[1].strip()
                _create_and_send_reply(svc, context, reply_text)
                _mark_as_read(svc, context)
                feedback_text = "Okay, your reply has been sent."
                action = 'CONTINUE' # Move to next email
            elif intent == "CONFIRM":
                _mark_as_read(svc, context)
                feedback_text = "Got it. Moving to the next email."
                action = 'CONTINUE'
        
        global _LAST_AUDIO
        _LAST_AUDIO = await tts_any(feedback_text, voice=REALTIME_VOICE)
        
        return JSONResponse({
            "action": action,
            "text_feedback": feedback_text,
            "audio_url": "/audio.mp3"
        })

    except Exception as e:
        print(f"[ERROR] assistant_command failed: {e}")
        # ### THIS IS THE FIXED BLOCK ###
        global _LAST_AUDIO
        _LAST_AUDIO = await tts_any("Sorry, an error occurred.", voice=REALTIME_VOICE)
        return JSONResponse({
            "action": "END",
            "text_feedback": "An error occurred.",
            "audio_url": "/audio.mp3"
        }, status_code=500)