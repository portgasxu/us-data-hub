#!/bin/bash
# US Data Hub — Watcher Daemon Startup Script
# Starts the event-driven watcher as a background service

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$SCRIPT_DIR/logs/watcher.pid"
LOG_FILE="$SCRIPT_DIR/logs/watcher-daemon.log"

mkdir -p "$SCRIPT_DIR/logs"

start() {
    if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        echo "Watcher daemon already running (PID: $(cat $PID_FILE))"
        return 1
    fi

    echo "Starting watcher daemon..."
    cd "$SCRIPT_DIR/.."
    nohup python3 scripts/watcher.py --daemon --interval 300 >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "Watcher daemon started (PID: $(cat $PID_FILE))"
}

stop() {
    if [ -f "$PID_FILE" ]; then
        kill $(cat "$PID_FILE") 2>/dev/null
        rm -f "$PID_FILE"
        echo "Watcher daemon stopped"
    else
        echo "Watcher daemon not running"
    fi
}

status() {
    if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        echo "Watcher daemon running (PID: $(cat $PID_FILE))"
        echo "Log: tail -20 $LOG_FILE"
        tail -20 "$LOG_FILE"
    else
        echo "Watcher daemon not running"
    fi
}

case "$1" in
    start) start ;;
    stop) stop ;;
    restart) stop; sleep 1; start ;;
    status) status ;;
    *) echo "Usage: $0 {start|stop|restart|status}"; exit 1 ;;
esac
