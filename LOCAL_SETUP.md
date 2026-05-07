# Local Development Setup — Composable CDP

## ✅ Pre-Setup (Already Completed)

- ✅ **Python 3.11.15** installed (via Homebrew)
- ✅ PostgreSQL 18.3 + Redis 7 (Docker) - Running
- ✅ Database restored from backup (`cdp_meta.backup`)
- ✅ Backend dependencies installed & virtual environment created
- ✅ All code compatible with Python 3.11

---

## ⚙️ Requirements

- **Python:** 3.11.15 (installed via Homebrew)
- **Docker:** v28.4.0+
- **Node.js:** For frontend (npm)
- **macOS:** Tested on Apple Silicon (M1/M2)

---

## 🚀 Running the Application

### Terminal 1: Backend (FastAPI)

```bash
cd "/Users/praveenvishnoi/Desktop/RPSG Projects/cdp/backend"
source .venv/bin/activate
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

**Backend ready at:** http://localhost:8000
**API Docs:** http://localhost:8000/docs
**OpenAPI JSON:** http://localhost:8000/openapi.json

---

### Terminal 2: Frontend (React)

```bash
cd "/Users/praveenvishnoi/Desktop/RPSG Projects/cdp/frontend"
npm install  # If not already done
npm run dev
```

**Frontend ready at:** http://localhost:5173

---

## 📊 Database Access

**PostgreSQL (Docker):**
- **Host:** localhost:5432
- **User:** cdp
- **Password:** cdp
- **Database:** cdp_meta
- **Schemas:** bronze, silver, silver_identity, silver_gold, gold, identity, reverse_etl

**Connect via psql:**
```bash
psql -h localhost -U cdp -d cdp_meta
```

**Redis (Docker):**
- **Host:** localhost:6379
- **CLI:** `docker exec cdp-redis-1 redis-cli`

---

## 🐳 Docker Services Status

```bash
cd "/Users/praveenvishnoi/Desktop/RPSG Projects/cdp"
docker-compose ps  # Check all services

# View logs
docker-compose logs postgres  # PostgreSQL logs
docker-compose logs redis     # Redis logs
```

---

## 📝 API Configuration

The backend uses environment variables from `docker-compose.yml`:

```env
CDP_DATABASE_URL=postgresql+asyncpg://cdp:cdp@localhost:5432/cdp_meta
CDP_REDIS_URL=redis://localhost:6379/0
CDP_DEBUG=true
CDP_AWS_REGION=ap-south-1
```

For AWS Athena access, add credentials:
```env
CDP_AWS_ACCESS_KEY_ID=your_key
CDP_AWS_SECRET_ACCESS_KEY=your_secret
ANTHROPIC_API_KEY=your_api_key  # Optional for NL segmentation
```

---

## 🛑 Stopping Everything

```bash
# Stop Docker services
docker-compose down

# Keep data volumes
docker-compose down --volumes  # Remove all data

# Kill backend/frontend - Ctrl+C in respective terminals
```

---

## 🔧 Troubleshooting

**Backend won't start:**
```bash
# Check if database is running
docker exec cdp-postgres-1 pg_isready -U cdp

# Check logs
docker logs cdp-postgres-1
```

**Redis connection issues:**
```bash
docker exec cdp-redis-1 redis-cli ping
# Should return: PONG
```

**Frontend won't compile:**
```bash
cd frontend
rm -rf node_modules package-lock.json
npm install
npm run dev
```

---

## 📚 Project Structure

```
cdp/
├── backend/             # FastAPI server
│   ├── app/
│   │   ├── api/        # API routes
│   │   ├── models/     # SQLAlchemy models
│   │   ├── schemas/    # Pydantic schemas
│   │   ├── services/   # Business logic
│   │   └── core/       # Configuration
│   └── requirements.txt
├── frontend/            # React + Vite
│   ├── src/
│   ├── package.json
│   └── vite.config.ts
├── dwh/                # Data warehouse (dbt)
├── docker-compose.yml  # Services definition
└── README.md
```

---

## 📖 Next Steps

1. Open http://localhost:3000 (Frontend)
2. Explore the API at http://localhost:8000/docs
3. Check database tables with: `psql -h localhost -U cdp -d cdp_meta -c "\dt public.*"`
4. Review [README.md](./README.md) for architecture details
