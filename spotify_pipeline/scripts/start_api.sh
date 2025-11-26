#!/bin/bash
# Script to start the FastAPI server

set -e

echo "🚀 Starting Spotify Prediction API..."
echo ""

# Set XGBoost library path for macOS
if [[ "$OSTYPE" == "darwin"* ]]; then
    LIBOMP_PATH=""
    if command -v brew &>/dev/null; then
        LIBOMP_PREFIX="$(brew --prefix libomp 2>/dev/null || true)"
        if [[ -n "$LIBOMP_PREFIX" && -d "$LIBOMP_PREFIX/lib" ]]; then
            LIBOMP_PATH="$LIBOMP_PREFIX/lib"
        fi
    fi

    # Fallback to common Homebrew locations if brew prefix lookup failed
    if [[ -z "$LIBOMP_PATH" ]]; then
        for candidate in /opt/homebrew/opt/libomp/lib /usr/local/opt/libomp/lib "$HOME/homebrew/opt/libomp/lib"; do
            if [[ -d "$candidate" ]]; then
                LIBOMP_PATH="$candidate"
                break
            fi
        done
    fi

    if [[ -n "$LIBOMP_PATH" ]]; then
        export DYLD_LIBRARY_PATH="$LIBOMP_PATH:${DYLD_LIBRARY_PATH:-}"
        echo "✅ XGBoost library path configured: $LIBOMP_PATH"
    else
        echo "⚠️  libomp not found. Install it with 'brew install libomp' to enable XGBoost."
    fi
fi

# Navigate to spotify_pipeline directory
cd "$(dirname "$0")"

# Install dependencies if needed
if ! command -v uv &> /dev/null; then
    echo "❌ Error: uv is not installed"
    echo "Install it with: pip install uv"
    exit 1
fi

echo "📦 Installing dependencies..."
uv sync

echo ""
echo "✅ Starting API server on http://localhost:8000"
echo "📖 API docs available at http://localhost:8000/docs"
echo ""

# Run the FastAPI server
uv run uvicorn api:app --reload --host 0.0.0.0 --port 8000
