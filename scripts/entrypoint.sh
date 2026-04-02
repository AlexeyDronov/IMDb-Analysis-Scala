#!/bin/bash

if [ ! -t 0 ]; then
echo "Error: This container must be run with an interactive terminal (-it or tty: true)"
exit 1
fi

GREEN="\033[1;32m"
CYAN="\033[1;36m"
YELLOW="\033[1;33m"
RESET="\033[0m"

clear

echo -e "${CYAN}"
echo "========================================"
echo "    ██╗███╗   ███╗██████╗ ██████╗ "
echo "    ██║████╗ ████║██╔══██╗██╔══██╗"
echo "    ██║██╔████╔██║██║  ██║██████╔╝"
echo "    ██║██║╚██╔╝██║██║  ██║██╔══██╗"
echo "    ██║██║ ╚═╝ ██║██████╔╝██████╔╝"
echo "    ╚═╝╚═╝     ╚═╝╚═════╝ ╚═════╝ "
echo ""
echo "        IMDb Analysis Container"
echo "========================================"
echo -e "${RESET}"

sleep 0.5

echo -e "${GREEN}[✓] Environment ready${RESET}"
echo -e "${GREEN}[✓] Scala + Spark loaded${RESET}"
echo -e "${GREEN}[✓] Dataset mounted${RESET}"

echo ""
echo -ne "${YELLOW}>> Run benchmarks? (y/n): ${RESET}"
read -r REPLY


echo

if [[ "$REPLY" =~ ^[Yy] ]]; then
    read -p "Trials (default 1): " TRIALS
    TRIALS=${TRIALS:-1}

    echo "Running benchmarks with $TRIALS trial(s)..."
    bash ./scripts/benchmark.sh "$TRIALS"
else
    echo "Skipping benchmarks."
fi

echo ""
echo "Dropping you into the interactive shell. Type 'exit' to leave."
exec bash
