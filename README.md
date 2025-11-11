#  Distributed Kafka–FastAPI System

- **Admin** — manages users, topics, permissions, and approvals  
- **Producer** — creates topics, publishes messages, requests deletions  
- **Consumer** — subscribes to topics, consumes messages, logs to CSV  
- **Broker** — runs the Kafka cluster managing message flow  

---

##  Project Architecture
```
kafka-lab-project/
├── admin/
│   ├── app.py
│   ├── templates/
│   │   ├── admin_dashboard.html
│   │   ├── login.html
│   │   └── register.html
│   └── .env
│
├── producer/
│   ├── app.py
│   ├── templates/
│   │   ├── producer.html
│   │   └── login.html
│   └── .env
│
├── consumer/
│   ├── app.py
│   ├── templates/
│   │   ├── consumer.html
│   │   └── login.html
│   └── .env
│
├── broker/
│   ├── templates/
│   │   ├── broker.html
│   │   └── login.html
│   └── .env
│   └── app.py
└── README.md
```

##  Installation of Dependencies

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install fastapi uvicorn kafka-python requests python-dotenv bcrypt Jinja2
pip install itsdangerous python-multipart

##  Running it

### admin:
uvicorn app:app --host 0.0.0.0 --port 8000

### broker:
bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

uvicorn app:app --host 0.0.0.0 --port 8002

for checking: bin/kafka-topics.sh --bootstrap-server 10.147.17.199:9092 --list

### producer:
uvicorn app:app --host 0.0.0.0 --port 8001

### consumer:
uvicorn app:app --host 0.0.0.0 --port 8003

