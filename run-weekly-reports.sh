#!/bin/bash

# set working dir
cd "$(dirname "$0")"

# download all the reports
source venv/bin/activate
python scrape_weekly_reports.py

# create .txt versions
cd weekly_system_status_reports
for file in *.pdf; do pdftotext -layout "$file"; done
cd ..

# extract csv and sqlite data
python process_weekly_reports.py weekly_system_status_reports

# copy to metabase
cp eskom_system_status_reports.sqlite /home/hosting/metabase-data/eskom_system_status_reports.sqlite
