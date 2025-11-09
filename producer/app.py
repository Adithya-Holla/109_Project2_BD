#Import all requirements
import os, csv, random, datetime, time, threading, queue, requests
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError
from dotenv import load_dotenv

# ---------- INITIAL SETUP ----------
load_dotenv()
app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key=os.getenv("PROD_SESSION_KEY"))
templates = Jinja2Templates(directory="templates")

ADMIN_IP = os.getenv("NODE4_IP")
BROKER = os.getenv("NODE2_IP") + ":9092"
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")
CSV_FILE = "data.csv"

send_queue = queue.Queue()
stop_flag = False
input_thread_started = False

# ---------- KAFKA PRODUCER ----------
producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: v.encode("utf-8"),
    acks='all',
    retries=5,
    linger_ms=10
)

# ---------- TEST DATA ----------sample data generated
TOPICS = ["sports_news", "tech_updates", "weather_report", "finance_ticker"]
MESSAGES = {
    "sports_news": ["Goal scored!", "Match ended 2-1."],
    "tech_updates": ["AI model released", "Python 3.14 out!"],
    "weather_report": ["Sunny in Mumbai", "Rain in Bangalore"],
    "finance_ticker": ["NIFTY up 1%", "Sensex down 200 pts"]
}


# ---------- CSV GENERATION ----------generating a csv with this dummy data
def generate_dummy_csv(rows=200):
    with open(CSV_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp", "topic", "message"])
        writer.writeheader()
        for _ in range(rows):
            topic = random.choice(TOPICS)
            msg = random.choice(MESSAGES[topic])
            ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow({"timestamp": ts, "topic": topic, "message": msg})
    print(f"🪶 LOG: Dummy CSV generated with {rows} rows")


# ---------- SAFE SEND ----------
def safe_send(topic, message):
    for attempt in range(4):
        try:
            print(f"🛰 Attempting to send → {topic}: {message}")
            future = producer.send(topic, message)
            result = future.get(timeout=10)
            print(f"✅ Sent → {topic} [partition={result.partition}, offset={result.offset}]")
            return True
        except Exception as e:
            print(f"❌ Send error (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)
    print("🚨 Failed to send after retries.")
    return False


# ---------- PUBLISHER THREAD ----------
def publisher_thread():
    print("🪶 LOG: Publisher thread started")
    while not stop_flag:
        try:
            username, topic, message = send_queue.get(timeout=1)
            print(f"📬 LOG: Publisher dequeued → user={username}, topic={topic}, message={message}")
        except queue.Empty:
            continue

        headers = {"x-api-key": ADMIN_API_KEY}
        try:
            r = requests.get(f"http://{ADMIN_IP}:8000/api/check_access/{username}/{topic}/producer", headers=headers)
            allowed = r.json().get("allowed", False)
        except Exception as e:
            print(f"⚠ LOG: Admin check failed ({e}), skipping...")
            allowed = False

        if allowed:
            print(f"✅ LOG: Access granted by admin for topic {topic}")
            safe_send(topic, message)
        else:
            print(f"🚫 LOG: Access denied by admin for topic {topic} (topic may still be pending)")

        send_queue.task_done()
        time.sleep(0.2)


# ---------- INPUT LISTENER ----------
def input_listener_thread(username="prod1"):
    global input_thread_started
    if input_thread_started:
        print("🪶 LOG: Input listener already running, skipping new start.")
        return
    input_thread_started = True

    print(f"🪶 LOG: Input listener started for user={username}")
    if not os.path.exists(CSV_FILE):
        generate_dummy_csv()

    with open(CSV_FILE, "r") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
            send_queue.put((username, row["topic"], row["message"]))
            print(f"🧾 LOG: Queued #{i} → topic={row['topic']}, message={row['message']}")
            time.sleep(1)


# ---------- TOPIC WATCHER ----------
def topic_watcher_thread():
    print("🪶 LOG: Topic watcher thread started")
    admin_client = KafkaAdminClient(bootstrap_servers=BROKER)
    created = set()
    seen_approved = set()

    while not stop_flag:
        headers = {"x-api-key": ADMIN_API_KEY}
        try:
            r1 = requests.get(f"http://{ADMIN_IP}:8000/api/topics/approved", headers=headers)
            r2 = requests.get(f"http://{ADMIN_IP}:8000/api/topics/active", headers=headers)
            approved = [t["name"] for t in (r1.json() + r2.json())]
        except Exception as e:
            print(f"⚠ LOG: Could not fetch topics from admin ({e})")
            time.sleep(5)
            continue

        existing = admin_client.list_topics()
        print(f"📡 LOG: Watching topics. Approved from admin: {approved}")

        # 🔹 Print as soon as Admin approves (fake auto-activation message)
        for t in approved:
            if t not in seen_approved:
                print(f"✨ LOG: Admin approved topic '{t}' → '{t}' auto-activated (simulated)")
                seen_approved.add(t)

        for t in approved:
            if t not in existing and t not in created:
                try:
                    admin_client.create_topics([NewTopic(name=t, num_partitions=1, replication_factor=1)])
                    created.add(t)
                    print(f"✅ LOG: Topic created in Kafka → {t}")

                    # auto activate on admin side (still here)
                    requests.post(f"http://{ADMIN_IP}:8000/api/topic_activate/{t}", headers=headers)
                    print(f"🟢 LOG: Topic auto-activated via Admin API → {t}")

                except TopicAlreadyExistsError:
                    pass
                except Exception as e:
                    print(f"❌ LOG: Error creating topic {t}: {e}")

        time.sleep(5)

def topic_deletion_watcher_thread():
    """Checks for admin-approved deletions and removes those topics from Kafka."""
    print("🧹 LOG: Topic deletion watcher started")
    admin_client = KafkaAdminClient(bootstrap_servers=BROKER)
    processed = set()

    while not stop_flag:
        headers = {"x-api-key": ADMIN_API_KEY}
        try:
            r = requests.get(f"http://{ADMIN_IP}:8000/api/delete_requests", headers=headers)
            delete_reqs = r.json() if r.status_code == 200 else []
        except Exception as e:
            print(f"⚠ LOG: Could not fetch delete requests from admin ({e})")
            time.sleep(5)
            continue

        for req in delete_reqs:
            topic = req["topic_name"]
            status = req["status"]

            if status == "approved" and topic not in processed:
                print(f"🗑 LOG: Deleting Kafka topic '{topic}' (approved by admin)")
                try:
                    admin_client.delete_topics([topic], timeout_ms=10000)
                    print(f"✅ LOG: Kafka topic '{topic}' deleted successfully")

                    # Tell admin to finalize DB cleanup
                    requests.post(
                        f"http://{ADMIN_IP}:8000/api/topic_delete_finalize/{topic}",
                        headers=headers
                    )
                except Exception as e:
                    print(f"❌ LOG: Error deleting topic '{topic}': {e}")
                processed.add(topic)

        time.sleep(10)



# ---------- BACKGROUND THREADS ----------
threading.Thread(target=publisher_thread, daemon=True).start()
threading.Thread(target=topic_watcher_thread, daemon=True).start()
threading.Thread(target=topic_deletion_watcher_thread, daemon=True).start()


# ---------- FASTAPI ROUTES ----------
@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@app.post("/login")
def login(request: Request, username: str = Form(...), password: str = Form(...)):
    r = requests.post(
        f"http://{ADMIN_IP}:8000/api/login",
        data={"username": username, "password": password},
        headers={"x-api-key": ADMIN_API_KEY},
    )
    if r.json().get("ok"):
        request.session["user"] = username
        return RedirectResponse("/", 303)
    return RedirectResponse("/login", 303)


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login")
    headers = {"x-api-key": ADMIN_API_KEY}
    topics = requests.get(f"http://{ADMIN_IP}:8000/api/topics/active", headers=headers).json()
    return templates.TemplateResponse("producer.html", {"request": request, "topics": topics, "user": request.session.get("user")})


@app.post("/start_stream")
def start_stream(request: Request):
    username = request.session.get("user", "prod1")
    print(f"🪶 LOG: Start stream called by user={username}")
    threading.Thread(target=input_listener_thread, args=(username,), daemon=True).start()
    return RedirectResponse("/", 303)


@app.post("/create_topic")
def create_topic(request: Request, topic: str = Form(...)):
    username = request.session.get("user", "prod1")
    headers = {"x-api-key": ADMIN_API_KEY}

    r = requests.post(
        f"http://{ADMIN_IP}:8000/api/topic_request/{topic}",
        data={"requested_by": username},
        headers=headers
    )
    print(f"🪶 LOG: Requested topic '{topic}' → Response: {r.json()}")
    return RedirectResponse("/", 303)

# ---------- ✅ TOPIC DELETE REQUEST ----------
@app.post("/delete_topic")
def delete_topic(request: Request, topic: str = Form(...)):
    username = request.session.get("user", "prod1")
    headers = {"x-api-key": ADMIN_API_KEY}

    # Producer requests admin to delete topic
    try:
        r = requests.post(
            f"http://{ADMIN_IP}:8000/api/topic_delete_request/{topic}",
            data={"requested_by": username},
            headers=headers
        )
        if r.status_code == 200:
            print(f"🗑 LOG: Delete request sent for topic '{topic}' by {username}")
        else:
            print(f"⚠ LOG: Failed to send delete request for {topic}: {r.status_code}")
    except Exception as e:
        print(f"❌ LOG: Error while requesting delete for {topic}: {e}")

    return RedirectResponse("/", 303)


# ---------- ✅ LIVE MANUAL PUBLISH ----------
@app.post("/publish")
def publish_message(request: Request, topic: str = Form(...), message: str = Form(...)):
    username = request.session.get("user", "prod1")
    send_queue.put((username, topic, message))
    print(f"🧾 LOG: Manual publish → topic={topic}, message={message}, user={username}")
    return RedirectResponse("/", 303)


@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)

