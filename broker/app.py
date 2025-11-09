#broker_app.py_new:
import os, requests
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError
import json
load_dotenv()
app = FastAPI()
templates = Jinja2Templates(directory="templates")

ADMIN_IP = os.getenv("NODE4_IP")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")
app.add_middleware(SessionMiddleware, secret_key=os.getenv("BROKER_SESSION_KEY"))

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
def login(request: Request, username: str = Form(...), password: str = Form(...)):
    r = requests.post(f"http://{ADMIN_IP}:8000/api/login", data={"username": username, "password": password}, headers={"x-api-key": ADMIN_API_KEY})
    if r.json().get("ok"):
        request.session["user"] = username
        return RedirectResponse("/", 303)
    return RedirectResponse("/login", 303)

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login")
    h = {"x-api-key": ADMIN_API_KEY}
    topics = requests.get(f"http://{ADMIN_IP}:8000/api/topics/active", headers=h).json()
    return templates.TemplateResponse("broker.html", {"request": request, "topics": topics})
    

@app.get("/api/test-route")
def test_kafka_message_flow_all():
    bootstrap_servers = "10.147.17.199:9092"

    # Fetch active topics from Admin
    h = {"x-api-key": ADMIN_API_KEY}
    r = requests.get(f"http://{ADMIN_IP}:8000/api/topics/active", headers=h)
    active_topics = [t["name"] for t in r.json()] if r.ok else []

    if not active_topics:
        return {"ok": False, "message": "No active topics found in Admin DB"}

    results = {}

    for topic in active_topics:
        try:
            # --- Send a ping message to the topic (optional) ---
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            ping = {"ping": "hello_kafka", "topic": topic}
            producer.send(topic, ping)
            producer.flush()
            producer.close()

            # --- Consume messages from the same topic ---
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='latest',
                enable_auto_commit=False,
                consumer_timeout_ms=5000,  # 5s wait
                value_deserializer=lambda v: v.decode('utf-8')
            )

            got_message = False
            for msg in consumer:
                try:
                    value = json.loads(msg.value)
                except Exception:
                    value = {"raw_message": msg.value}

                results[topic] = {
                    "status": "✅ Message received",
                    "received": value
                }
                got_message = True
                break

            if not got_message:
                results[topic] = {"status": "⚠️ No new message received"}

            consumer.close()

        except Exception as e:
            results[topic] = {"status": "❌ Error", "error": str(e)}

    return {
        "ok": True,
        "summary": results,
        "topic_count": len(active_topics)
    }


@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)
