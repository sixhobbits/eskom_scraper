import os
import re
import datetime
import csv
import traceback
import pandas as pd
import sqlalchemy
import sys

reportdir = sys.argv[1]
all_txt_files = os.listdir(reportdir)
all_txt_files = [x for x in all_txt_files if x.endswith(".txt")]
print("processing all txt files found")
print(all_txt_files)

outfile = open("all_status_reports.csv", 'w')
writer = csv.writer(outfile)

header = ['report_generated_date', 'forecast_date', 'week_number', 'rsa_contracted_forecast', 
          'residual_forecast', 'available_dispatchable_capacity', 'available_capacity_less_or_ua', 
          'planned_maintenance', 'unplanned_outage_assumption', 'planned_risk_level', 'likely_risk_scenario', 'report_file_name']
writer.writerow(header)

df = pd.DataFrame(columns=header)
db = sqlalchemy.create_engine('sqlite:///eskom_weekly_reports.sqlite')


for fle in all_txt_files:
    try:
        with open(os.path.join(reportdir, fle)) as f:
            s = f.read()

        outlook = s.split("52 Week Outlook")[1]
        outlook = outlook.split("\n\n\n")[0]
        outlook_lines = outlook.split("\n")
        print(len(outlook_lines))
        
    
        # loop through until we find the first line that starts 
        # with a date

        for ln, line in enumerate(outlook_lines):
            line = line.strip()
            
            if re.match(r'^\d{2}-[A-Za-z]{3}-\d{2}', line):
                print("FOUND MATCH")
                print(line)
                for _ in range(52):
                    line = outlook_lines[ln]
                    line = line.strip()
                    line = line.split(" ")
                    line = [x.strip() for x in line if x]
                    ln += 1
                    if len(line) >= 10 and line[0].count("-") == 2:
                        line = line[:10]
                        week_date = datetime.datetime.strptime(line[0], '%d-%b-%y').date()
                        _, _, _, _, year, week = fle.split("_")
                        week = week.split(".txt")[0].replace("w", "")
                        # in theory reports are released on wednesdays, but in reality they 
                        # are often late
                        report_date = datetime.date.fromisocalendar(int(year), int(week), 3)
                        line = [report_date.isoformat(), week_date.isoformat()] + [int(x) for x in line[1:]] + [fle]
                        row_df = pd.DataFrame([line], columns=header)
                        df = pd.concat([df, row_df], ignore_index=True)
                        try:
                            writer.writerow(line)
                        except Exception as ie:
                            print(ie)
                break
            elif "Assumption (UA) " in line:
                    pattern = r'\((-?\d+)\sMW\)\s+\((-?\d+)\sMW\)'
                    matches = re.findall(pattern, line)

                    if matches:
                        planned_risk_level, likely_risk_level = matches[0]
                        print(f'{report_date}, {planned_risk_level}, {likely_risk_level}')
                    else:
                        print('No matches found.')
        

    except Exception as e:
        print(e)

    
outfile.close()

db = sqlalchemy.create_engine('sqlite:///eskom_system_status_reports.sqlite')
df.to_sql("weekly_system_status_reports", db, if_exists="replace")
