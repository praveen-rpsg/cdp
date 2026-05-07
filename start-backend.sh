#!/bin/bash
# Start the Composable CDP Backend

cd "$(dirname "$0")/backend"
source .venv/bin/activate

echo "🚀 Starting Backend Server..."
echo "📍 Backend will be available at: http://localhost:8000"
echo "📚 API Docs: http://localhost:8000/docs"
echo ""

uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
