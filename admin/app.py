import os, sqlite3, bcrypt
from fastapi import FastAPI, Request, Form, Header, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
templates = Jinja2Templates(directory="templates")

DB = os.getenv("DB_FILE", "topics.db")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")
app.add_middleware(SessionMiddleware, secret_key=os.getenv("ADMIN_SESSION_KEY", "admin_secret_key"))

# database stuff
def get_db():
    conn = sqlite3.connect(DB, timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

if not os.path.exists(DB):
    conn = get_db()
    conn.executescript("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        password_hash BLOB,
        role TEXT CHECK(role IN ('admin', 'producer', 'consumer', 'broker'))
    );
    CREATE TABLE topics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        requested_by TEXT,
        status TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE access_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT,
        topic_name TEXT,
        role TEXT,
        status TEXT,
        requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE permissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT,
        topic_name TEXT,
        role TEXT,
        granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE user_subscriptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT,
        topic_name TEXT,
        subscribed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
      CREATE TABLE delete_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic_name TEXT,
        requested_by TEXT,
        status TEXT DEFAULT 'pending',
        requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );                 
    """)
    pw = bcrypt.hashpw(b"admin123", bcrypt.gensalt())
    conn.execute("INSERT INTO users (username,password_hash,role) VALUES (?,?,?)", ("admin", pw, "admin"))
    conn.commit()
    conn.close()
    print("✅ Database initialized and admin user created (admin / admin123)")

def require_key(x_api_key: Optional[str] = Header(None)):
    if x_api_key != ADMIN_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
def login(request: Request, username: str = Form(...), password: str = Form(...)):
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
    conn.close()
    if user and bcrypt.checkpw(password.encode(), user["password_hash"]) and user["role"] == "admin":
        request.session["user"] = username
        return RedirectResponse("/", 303)
    return RedirectResponse("/login", 303)

@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", 303)

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login")

    conn = get_db()
    topics = conn.execute("SELECT * FROM topics").fetchall()
    access_reqs = conn.execute("SELECT * FROM access_requests").fetchall()
    permissions = conn.execute("SELECT * FROM permissions").fetchall()
    subs = conn.execute("SELECT * FROM user_subscriptions").fetchall()
    deletes = conn.execute("SELECT * FROM delete_requests").fetchall()
    conn.close()

    return templates.TemplateResponse("admin_dashboard.html", {
        "request": request,
        "topics": topics,
        "access_reqs": access_reqs,
        "permissions": permissions,
        "subs": subs,
        "deletes": deletes
    })

@app.get("/register", response_class=HTMLResponse)
def register_page(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login")
    return templates.TemplateResponse("register.html", {"request": request})

@app.post("/register")
def register_user(request: Request, username: str = Form(...), password: str = Form(...), role: str = Form(...)):
    if not request.session.get("user"):
        return RedirectResponse("/login")
    conn = get_db()
    pw = bcrypt.hashpw(password.encode(), bcrypt.gensalt())
    conn.execute("INSERT INTO users (username,password_hash,role) VALUES (?,?,?)", (username, pw, role))
    conn.commit()
    conn.close()
    return RedirectResponse("/", 303)

@app.post("/api/login")
def api_login(username: str = Form(...), password: str = Form(...)):
    conn = get_db()
    u = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
    conn.close()
    if u and bcrypt.checkpw(password.encode(), u["password_hash"]):
        return {"ok": True, "role": u["role"]}
    return {"ok": False}

@app.post("/api/topic_request/{name}")
def api_topic_request(name: str, requested_by: str = Form(...), x_api_key: Optional[str] = Header(None)):
    require_key(x_api_key)
    conn = get_db()
    conn.execute("""
        INSERT OR IGNORE INTO topics (name, requested_by, status)
        VALUES (?, ?, 'pending')
    """, (name, requested_by))
    conn.commit()
    conn.close()
    return {"status": "pending", "topic": name, "requested_by": requested_by}

@app.post("/api/topic_approve/{name}")
def api_topic_approve(name: str):
    conn = get_db()
    conn.execute("UPDATE topics SET status='approved' WHERE name=?", (name,))
    conn.commit()
    conn.close()
    return RedirectResponse("/", 303)

@app.post("/api/topic_activate/{name}")
def api_topic_activate(name: str, x_api_key: Optional[str] = Header(None)):
    """Activate a topic (used by producer or admin)."""
    require_key(x_api_key)
    conn = get_db()
    conn.execute("UPDATE topics SET status='active' WHERE name=?", (name,))
    conn.commit()
    conn.close()
    return {"status": "active"}

@app.post("/api/topic_activate_manual/{name}")
def api_topic_activate_manual(name: str):
    """Manually activate a topic from the admin dashboard."""
    conn = get_db()
    conn.execute("UPDATE topics SET status='active' WHERE name=?", (name,))
    conn.commit()
    conn.close()
    return RedirectResponse("/", 303)

@app.get("/api/topics/approved")
def api_topics_approved(x_api_key: Optional[str] = Header(None)):
    """Return all approved topics."""
    require_key(x_api_key)
    conn = get_db()
    rows = conn.execute("SELECT * FROM topics WHERE status='approved'").fetchall()
    conn.close()
    return [dict(x) for x in rows]

@app.get("/api/topics/active")
def api_topics_active(x_api_key: Optional[str] = Header(None)):
    """Return all active topics."""
    require_key(x_api_key)
    conn = get_db()
    rows = conn.execute("SELECT * FROM topics WHERE status='active'").fetchall()
    conn.close()
    return [dict(x) for x in rows]

@app.post("/api/access_request")
def api_access_request(username: str = Form(...), topic: str = Form(...), role: str = Form(...), x_api_key: Optional[str] = Header(None)):
    """Producer or consumer requests access to a topic."""
    require_key(x_api_key)
    conn = get_db()
    conn.execute("""
        INSERT OR IGNORE INTO access_requests (username, topic_name, role, status)
        VALUES (?, ?, ?, 'pending')
    """, (username, topic, role))
    conn.commit()
    conn.close()
    return {"status": "pending", "user": username, "topic": topic, "role": role}

@app.post("/api/approve_access/{username}/{topic}/{role}")
def api_approve_access(username: str, topic: str, role: str):
    """Admin approves a pending access request."""
    conn = get_db()
    conn.execute("""
        UPDATE access_requests
        SET status='approved'
        WHERE username=? AND topic_name=? AND role=?
    """, (username, topic, role))
    conn.execute("""
        INSERT INTO permissions (username, topic_name, role)
        VALUES (?, ?, ?)
    """, (username, topic, role))
    conn.commit()
    conn.close()
    return RedirectResponse("/", 303)

@app.get("/api/check_access/{username}/{topic}/{role}")
def api_check_access(username: str, topic: str, role: str, x_api_key: Optional[str] = Header(None)):
    """Check if a user has access permission for a topic."""
    require_key(x_api_key)
    conn = get_db()
    row = conn.execute("""
        SELECT * FROM permissions
        WHERE username=? AND topic_name=? AND role=?
    """, (username, topic, role)).fetchone()

    if row:
        conn.close()
        return {"allowed": True}

    topic_row = conn.execute("""
        SELECT requested_by, status FROM topics WHERE name=?
    """, (topic,)).fetchone()

    conn.close()

    if topic_row and topic_row["requested_by"] == username and topic_row["status"] == "active" and role == "producer":
        return {"allowed": True}

    return {"allowed": False}

@app.post("/api/register_subscription")
def api_register_subscription(username: str = Form(...), topic: str = Form(...), x_api_key: Optional[str] = Header(None)):
    """Register a consumer subscription to a topic."""
    require_key(x_api_key)
    conn = get_db()
    conn.execute("INSERT INTO user_subscriptions (username, topic_name) VALUES (?, ?)", (username, topic))
    conn.commit()
    conn.close()
    return {"ok": True}

@app.get("/api/subscriptions/{username}")
def api_get_user_subscriptions(username: str, x_api_key: Optional[str] = Header(None)):
    """List all topics a consumer is subscribed to."""
    require_key(x_api_key)
    conn = get_db()
    rows = conn.execute("SELECT topic_name FROM user_subscriptions WHERE username=?", (username,)).fetchall()
    conn.close()
    return [r["topic_name"] for r in rows]

@app.post("/api/topic_delete_request/{name}")
def api_topic_delete_request(name: str, requested_by: str = Form(...), x_api_key: Optional[str] = Header(None)):
    require_key(x_api_key)
    conn = get_db()
    conn.execute("INSERT INTO delete_requests (topic_name, requested_by) VALUES (?, ?)", (name, requested_by))
    conn.commit()
    conn.close()
    return {"status": "pending", "topic": name}

@app.post("/api/topic_delete_approve/{name}")
def api_topic_delete_approve(name: str):
    """Admin approves topic deletion — producer will handle Kafka cleanup."""
    conn = get_db()
    conn.execute("UPDATE delete_requests SET status='approved' WHERE topic_name=?", (name,))
    conn.commit()
    conn.close()
    print(f"✅ Admin approved deletion for topic '{name}' (waiting for producer cleanup)")
    return RedirectResponse("/", 303)

@app.post("/api/unsubscribe/{username}/{topic}")
def api_unsubscribe(username: str, topic: str, x_api_key: Optional[str] = Header(None)):
    require_key(x_api_key)
    conn = get_db()
    conn.execute("DELETE FROM user_subscriptions WHERE username=? AND topic_name=?", (username, topic))
    conn.commit()
    conn.close()
    return {"ok": True}

@app.post("/api/unsubscribe/{username}/{topic}")
def api_unsubscribe(username: str, topic: str, x_api_key: Optional[str] = Header(None)):
    require_key(x_api_key)
    conn = get_db()
    conn.execute("DELETE FROM user_subscriptions WHERE username=? AND topic_name=?", (username, topic))
    conn.commit()
    conn.close()
    return {"ok": True}

@app.get("/api/delete_requests")
def api_get_delete_requests(x_api_key: Optional[str] = Header(None)):
    """Producers poll this to see which deletions are approved."""
    require_key(x_api_key)
    conn = get_db()
    rows = conn.execute("SELECT topic_name, status FROM delete_requests").fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.post("/api/topic_delete_finalize/{name}")
def api_topic_delete_finalize(name: str, x_api_key: Optional[str] = Header(None)):
    """Producer notifies admin after successful Kafka deletion → cleans DB."""
    require_key(x_api_key)
    conn = get_db()
    conn.execute("DELETE FROM topics WHERE name=?", (name,))
    conn.execute("DELETE FROM permissions WHERE topic_name=?", (name,))
    conn.execute("DELETE FROM access_requests WHERE topic_name=?", (name,))
    conn.execute("DELETE FROM user_subscriptions WHERE topic_name=?", (name,))
    conn.execute("DELETE FROM delete_requests WHERE topic_name=?", (name,))
    conn.commit()
    conn.close()
    print(f" DB cleanup complete for deleted topic '{name}'")
    return {"ok": True}

@app.post("/api/admin_create_topic")
def api_admin_create_topic(name: str = Form(...)):
    """Admin manually creates a topic record (auto-activation by producer)."""
    conn = get_db()
    conn.execute("""
        INSERT OR REPLACE INTO topics (name, requested_by, status)
        VALUES (?, 'admin', 'active')
    """, (name,))
    conn.commit()
    conn.close()
    print(f" Admin created topic '{name}' (producer will activate in Kafka)")
    return RedirectResponse("/", 303)


@app.post("/api/admin_delete_topic/{name}")
def api_admin_delete_topic(name: str):
    """Admin manually initiates deletion (same as producer’s delete request)."""
    conn = get_db()
    conn.execute("""
        INSERT OR IGNORE INTO delete_requests (topic_name, requested_by, status)
        VALUES (?, 'admin', 'pending')
    """, (name,))
    conn.commit()
    conn.close()
    print(f" Admin initiated delete request for topic '{name}'")
    return RedirectResponse("/", 303)