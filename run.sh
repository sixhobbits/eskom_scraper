#!/bin/bash

# set working dir
cd "$(dirname "$0")"
source venv/bin/activate
python eskom_dataroom_scraper.py
python restructure_files.py aaaa 
sqlite3 eskom_metrics.sqlite < create_indexes.sql
cp eskom_metrics.sqlite /home/hosting/metabase-data/eskom_metrics.sqlite
