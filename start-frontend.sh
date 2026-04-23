#!/bin/bash
# Start the Composable CDP Frontend

cd "$(dirname "$0")/frontend"

# Install dependencies if needed
if [ ! -d "node_modules" ]; then
    echo "📦 Installing dependencies..."
    npm install
fi

echo "🚀 Starting Frontend Server..."
echo "📍 Frontend will be available at: http://localhost:5173"
echo ""

npm run dev
