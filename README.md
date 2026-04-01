# VenaNow — Financial Intelligence Pipeline

A production-ready backend for processing Nigerian bank statements and generating
intelligent financial insights.

## Architecture

```
venanow/
├── pipeline/
│   ├── ingestion.py        # PDF/CSV/Excel statement parser
│   ├── cleaner.py          # Data cleaning & deduplication
│   ├── classifier.py       # Transaction classification engine
│   ├── recurring.py        # Subscription & recurring detection
│   └── processor.py        # Orchestrator — runs the full pipeline
├── api/
│   ├── main.py             # FastAPI application
│   ├── routes/
│   │   ├── statements.py   # Upload & process endpoints
│   │   ├── dashboard.py    # Dashboard data endpoints
│   │   ├── recommendations.py
│   │   └── chat.py         # AI assistant endpoint
│   └── dependencies.py     # DB session, auth
├── models/
│   ├── schema.sql          # PostgreSQL schema
│   ├── database.py         # SQLAlchemy setup
│   └── orm.py              # ORM models
├── analytics/
│   ├── health_score.py     # Financial health score engine
│   ├── recommender.py      # Recommendation engine
│   ├── forecaster.py       # Cash flow forecasting
│   └── insights.py         # Behavioral insights generator
├── utils/
│   ├── currency.py         # Naira formatting & Nigerian bank utils
│   └── logger.py           # Structured logging
├── tests/
│   ├── test_ingestion.py
│   ├── test_classifier.py
│   └── test_recommender.py
├── sample_data/
│   └── generate_sample.py  # Generate demo bank statement
├── requirements.txt
├── .env.example
└── docker-compose.yml
```

## Quickstart

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set environment variables
cp .env.example .env

# 3. Start PostgreSQL (via Docker)
docker-compose up -d db

# 4. Run migrations
python -m models.database create_tables

# 5. Start API
uvicorn api.main:app --reload --port 8000

# 6. Generate & test with sample data
python sample_data/generate_sample.py
curl -X POST http://localhost:8000/api/statements/upload \
  -F "file=@sample_data/sample_statement.csv" \
  -F "user_id=1"
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /api/statements/upload | Upload bank statement |
| GET | /api/dashboard/{user_id} | Full dashboard data |
| GET | /api/transactions/{user_id} | Transaction list with filters |
| GET | /api/recommendations/{user_id} | Personalized recommendations |
| GET | /api/health-score/{user_id} | Financial health score |
| POST | /api/chat | AI assistant query |
| GET | /api/subscriptions/{user_id} | Recurring payments |
| GET | /api/goals/{user_id} | Goal progress |
