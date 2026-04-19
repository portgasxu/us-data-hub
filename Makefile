.PHONY: init status collect screener factors monitor report pipeline watch watch-stop watch-status clean

# US Data Hub — Makefile
# Unified CLI for all operations. Run: make <command> ARGS="--help"

PYTHON = python3

# Database
init:
	$(PYTHON) scripts/run.py init

status:
	$(PYTHON) scripts/run.py status

# Data Collection
collect:
	$(PYTHON) scripts/run.py collect

collect-sec:
	$(PYTHON) scripts/collect.py --source sec $(ARGS)

collect-news:
	$(PYTHON) scripts/collect.py --source google_news $(ARGS)

collect-lb:
	$(PYTHON) scripts/collect.py --source longbridge $(ARGS)

collect-reddit:
	$(PYTHON) scripts/collect.py --source reddit $(ARGS)

# Analysis
screener:
	$(PYTHON) scripts/run.py screener

factors:
	$(PYTHON) scripts/run.py factors

alphalens:
	$(PYTHON) scripts/run.py alphalens

# Monitoring & Management
monitor:
	$(PYTHON) scripts/run.py monitor

report:
	$(PYTHON) scripts/run.py report

# Watcher (event-driven)
watch:
	bash scripts/watcher-daemon.sh start

watch-stop:
	bash scripts/watcher-daemon.sh stop

watch-status:
	bash scripts/watcher-daemon.sh status

# Full pipeline
pipeline:
	$(PYTHON) scripts/run.py pipeline

# Utilities
clean:
	rm -f data/us_data_hub.db
	rm -f data/raw/*
	rm -f data/processed/*
	echo "✅ Cleaned"
