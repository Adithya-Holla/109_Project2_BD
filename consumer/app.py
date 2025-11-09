import os, threading, requests, csv
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
templates = Jinja2Templates(directory="templates")

ADMIN_IP = os.getenv("NODE4_IP")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")
KAFKA_BROKER = os.getenv("NODE2_IP")

app.add_middleware(SessionMiddleware, secret_key=os.getenv("CONSUMER_SESSION_KEY", "secret"))


# ---------------- CSV LOGGER ----------------
def write_to_csv(topic, message, username):
    # Create a directory per consumer (if it doesn't exist)
    user_dir = os.path.join("outputs", username)
    os.makedirs(user_dir, exist_ok=True)

    # Create a file for each topic inside that folder
    filename = os.path.join(user_dir, f"{topic}.csv")

    with open(filename, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([topic, message])


# ---------------- CONSUMER FUNCTION ----------------
def consume_topic(topic, username):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=f"{username}_group"
    )
    print(f"✅ Started consuming topic: {topic}")
    for msg in consumer:
        message = msg.value.decode("utf-8")
        print(f"[{topic}] {message}")
        write_to_csv(topic, message, username)


# ---------------- ROUTES ----------------
@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@app.post("/login")
def login(request: Request, username: str = Form(...)):
    request.session["user"] = username
    return RedirectResponse("/", 303)


@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", 303)


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    username = request.session.get("user")
    if not username:
        return RedirectResponse("/login")

    headers = {"x-api-key": ADMIN_API_KEY}

    # Fetch all active topics
    r_topics = requests.get(f"http://{ADMIN_IP}:8000/api/topics/active", headers=headers)
    topics = r_topics.json() if r_topics.status_code == 200 else []

    # Fetch user's subscribed topics
    r_subs = requests.get(f"http://{ADMIN_IP}:8000/api/subscriptions/{username}", headers=headers)
    subs = r_subs.json() if r_subs.status_code == 200 else []

    return templates.TemplateResponse("consumer.html", {
        "request": request,
        "user": username,
        "topics": topics,
        "subs": subs
    })


@app.post("/subscribe")
def subscribe(request: Request, topic: str = Form(...)):
    username = request.session.get("user")
    if not username:
        return RedirectResponse("/login")

    headers = {"x-api-key": ADMIN_API_KEY}

    # Step 1: Request access first (so admin must approve)
    data_access = {"username": username, "topic": topic, "role": "consumer"}
    r1 = requests.post(f"http://{ADMIN_IP}:8000/api/access_request", headers=headers, data=data_access)
    if r1.status_code == 200:
        print(f"📨 Access request sent for topic '{topic}' by {username}")

    # Step 2: Register subscription (for tracking)
    data_sub = {"username": username, "topic": topic}
    r2 = requests.post(f"http://{ADMIN_IP}:8000/api/register_subscription", headers=headers, data=data_sub)
    if r2.status_code == 200:
        print(f"✅ Subscription registered for {username} on {topic}")

    return RedirectResponse("/", 303)

@app.post("/unsubscribe")
def unsubscribe(request: Request, topic: str = Form(...)):
    username = request.session.get("user")
    if not username:
        return RedirectResponse("/login")

    headers = {"x-api-key": ADMIN_API_KEY}

    # Notify admin to remove subscription
    r = requests.post(f"http://{ADMIN_IP}:8000/api/unsubscribe/{username}/{topic}", headers=headers)
    if r.status_code == 200:
        print(f"🗑️ {username} unsubscribed from {topic}")
    else:
        print(f"⚠️ Failed to unsubscribe {username} from {topic}: {r.status_code}")

    return RedirectResponse("/", 303)


@app.post("/start_consuming")
def start_consuming(request: Request, topic: str = Form(...)):
    """Manually start consuming after admin approval."""
    username = request.session.get("user")
    if not username:
        return RedirectResponse("/login")

    headers = {"x-api-key": ADMIN_API_KEY}
    # Check if user has permission to consume
    r = requests.get(f"http://{ADMIN_IP}:8000/api/check_access/{username}/{topic}/consumer", headers=headers)
    allowed = r.json().get("allowed")

    if allowed:
        threading.Thread(target=consume_topic, args=(topic, username), daemon=True).start()
        print(f"🎧 Consumer {username} started listening to {topic}")
    else:
        print(f"🚫 Access denied for {username} to topic {topic}")

    return RedirectResponse("/", 303)
