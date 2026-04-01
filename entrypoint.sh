#!/bin/bash

echo "========================================="
echo "  IMDb Analysis Container Ready!"
echo "========================================="

read -p "Run benchmarks? (y/n): " -r REPLY
echo

if [[ "$REPLY" =~ ^[Yy] ]]; then
    read -p "Trials (default 1): " TRIALS
    TRIALS=${TRIALS:-1}

    echo "Running benchmarks with $TRIALS trial(s)..."
    bash ./benchmark.sh "$TRIALS"
else
    echo "Skipping benchmarks."
fi

echo ""
echo "Dropping you into the interactive shell. Type 'exit' to leave."
exec bash