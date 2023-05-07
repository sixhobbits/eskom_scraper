import os
import sys
import re
import datetime
import pandas as pd
import sqlalchemy

HEADER = [
    "report_generated_date",
    "forecast_date",
    "week_number",
    "rsa_contracted_forecast",
    "residual_forecast",
    "available_dispatchable_capacity",
    "available_capacity_less_or_ua",
    "planned_maintenance",
    "unplanned_outage_assumption",
    "planned_risk_level",
    "likely_risk_scenario",
    "report_file_name",
]


BAD_FILES = {
    "Medium-Term-System-Adequacy-Outlook-2023-2027.txt",
    "Weekly_System_Status_Report_2021_w1.txt",
    "Weekly_System_Status_Report_2021_w10.txt",
    "Weekly_System_Status_Report_2021_w11.txt",
    "Weekly_System_Status_Report_2021_w12.txt",
    "Weekly_System_Status_Report_2021_w13.txt",
    "Weekly_System_Status_Report_2021_w14.txt",
    "Weekly_System_Status_Report_2021_w15.txt",
    "Weekly_System_Status_Report_2021_w16.txt",
    "Weekly_System_Status_Report_2021_w17.txt",
    "Weekly_System_Status_Report_2021_w18.txt",
    "Weekly_System_Status_Report_2021_w19.txt",
    "Weekly_System_Status_Report_2021_w2.txt",
    "Weekly_System_Status_Report_2021_w20.txt",
    "Weekly_System_Status_Report_2021_w21.txt",
    "Weekly_System_Status_Report_2021_w22.txt",
    "Weekly_System_Status_Report_2021_w23.txt",
    "Weekly_System_Status_Report_2021_w24.txt",
    "Weekly_System_Status_Report_2021_w25.txt",
    "Weekly_System_Status_Report_2021_w26.txt",
    "Weekly_System_Status_Report_2021_w27.txt",
    "Weekly_System_Status_Report_2021_w28.txt",
    "Weekly_System_Status_Report_2021_w29.txt",
    "Weekly_System_Status_Report_2021_w3.txt",
    "Weekly_System_Status_Report_2021_w30.txt",
    "Weekly_System_Status_Report_2021_w31.txt",
    "Weekly_System_Status_Report_2021_w32.txt",
    "Weekly_System_Status_Report_2021_w33.txt",
    "Weekly_System_Status_Report_2021_w34.txt",
    "Weekly_System_Status_Report_2021_w35.txt",
    "Weekly_System_Status_Report_2021_w36.txt",
    "Weekly_System_Status_Report_2021_w37.txt",
    "Weekly_System_Status_Report_2021_w38.txt",
    "Weekly_System_Status_Report_2021_w39.txt",
    "Weekly_System_Status_Report_2021_w4.txt",
    "Weekly_System_Status_Report_2021_w40.txt",
    "Weekly_System_Status_Report_2021_w41.txt",
    "Weekly_System_Status_Report_2021_w42.txt",
    "Weekly_System_Status_Report_2021_w43.txt",
    "Weekly_System_Status_Report_2021_w44.txt",
    "Weekly_System_Status_Report_2021_w45.txt",
    "Weekly_System_Status_Report_2021_w46.txt",
    "Weekly_System_Status_Report_2021_w47.txt",
    "Weekly_System_Status_Report_2021_w48.txt",
    "Weekly_System_Status_Report_2021_w49.txt",
    "Weekly_System_Status_Report_2021_w5.txt",
    "Weekly_System_Status_Report_2021_w50.txt",
    "Weekly_System_Status_Report_2021_w51.txt",
    "Weekly_System_Status_Report_2021_w52.txt",
    "Weekly_System_Status_Report_2021_w6.txt",
    "Weekly_System_Status_Report_2021_w7.txt",
    "Weekly_System_Status_Report_2021_w8.txt",
    "Weekly_System_Status_Report_2021_w9.txt",
    "Weekly_System_Status_Report_2022_w1.txt",
    "Weekly_System_Status_Report_2022_w10.txt",
    "Weekly_System_Status_Report_2022_w11.txt",
    "Weekly_System_Status_Report_2022_w12.txt",
    "Weekly_System_Status_Report_2022_w13.txt",
    "Weekly_System_Status_Report_2022_w14.txt",
    "Weekly_System_Status_Report_2022_w15.txt",
    "Weekly_System_Status_Report_2022_w16.txt",
    "Weekly_System_Status_Report_2022_w17.txt",
    "Weekly_System_Status_Report_2022_w18.txt",
    "Weekly_System_Status_Report_2022_w19.txt",
    "Weekly_System_Status_Report_2022_w2.txt",
    "Weekly_System_Status_Report_2022_w20.txt",
    "Weekly_System_Status_Report_2022_w21.txt",
    "Weekly_System_Status_Report_2022_w22.txt",
    "Weekly_System_Status_Report_2022_w23.txt",
    "Weekly_System_Status_Report_2022_w24.txt",
    "Weekly_System_Status_Report_2022_w25.txt",
    "Weekly_System_Status_Report_2022_w26.txt",
    "Weekly_System_Status_Report_2022_w27.txt",
    "Weekly_System_Status_Report_2022_w3.txt",
    "Weekly_System_Status_Report_2022_w4.txt",
    "Weekly_System_Status_Report_2022_w44.txt",
    "Weekly_System_Status_Report_2022_w5.txt",
    "Weekly_System_Status_Report_2022_w6.txt",
    "Weekly_System_Status_Report_2022_w7.txt",
    "Weekly_System_Status_Report_2022_w8.txt",
    "Weekly_System_Status_Report_2022_w9.txt",
    "Weekly_System_Status_Report_2023_w14.txt",
    "Weekly_System_Status_Report_2023_w6.txt",
}


def extract_year_and_week_from_report_name(fle):
    # the reports are called e.g. Weekly_System_Status_Report_2023_w10.txt
    # or Weekly_System_Status_Report_2023_w9.txt
    # grab the year and week components
    fle = fle.replace(".txt", "")
    _, _, _, _, year, week = fle.split("_")
    week = week.replace("w", "")
    return year, week


def extract_outlook(txt_file_path):
    """
    IN:   The full .txt file contents of a converted Eskom weekly system report.

    OUT:  A clean list of lists with the raw data from the 52 week outlook report.
          Also add in the report date and convert the data to the correct type

    NOTE: The earlier reports followed a different format, so this
          will only work on reports generated from second half of 2022 onwards.

          Some of the "52 week reports" actually contain data for 51-54 weeks
    """

    fle = os.path.basename(txt_file_path)
    report_year, report_week = extract_year_and_week_from_report_name(fle)

    with open(txt_file_path) as f:
        txt = f.read()

    outlook = txt.split("52 Week Outlook")[1]
    outlook = outlook.split("\n\n\n")[0]

    ## There's also some other crap mixed in, so extract and clean
    ## the interesting lines that make up the main table
    potential_outlook_lines = outlook.split("\n")
    actual_outlook_lines = []
    for ln, line in enumerate(potential_outlook_lines):
        line = line.strip()

        # some lines have N/A values and then they are useless
        # so far only seen in 2023_w15
        if "#N/A" in line:
            continue

        # if the line starts with a date like 10-Apr-23, then we want it
        # as it's part of the table
        if re.match(r"^\d{2}-[A-Za-z]{3}-\d{2}", line):
            elems = line.split("   ")
            elems = [elem.strip() for elem in elems if elem]

            (
                forecast_date,
                week_number,
                rsa_contracted_forecast,
                residual_forecast,
                available_dispatchable_capacity,
                available_capacity_less_or_ua,
                planned_maintenance,
                unplanned_outage_assumption,
                planned_risk_level,
                likely_risk_scenario,
                *_,
            ) = elems

            # assume the report was released on a wednesday even though in reality they
            # often are not, but all we have is the year and week.
            report_date = datetime.date.fromisocalendar(
                int(report_year), int(report_week), 3
            )
            forecast_date = datetime.datetime.strptime(forecast_date, "%d-%b-%y").date()

            clean = (
                report_date.isoformat(),
                forecast_date.isoformat(),
                int(week_number),
                int(rsa_contracted_forecast),
                int(residual_forecast),
                int(available_dispatchable_capacity),
                int(available_capacity_less_or_ua),
                int(planned_maintenance),
                int(unplanned_outage_assumption),
                int(planned_risk_level),
                int(likely_risk_scenario),
                fle,
            )
            actual_outlook_lines.append(clean)
    return actual_outlook_lines


def process_all_reports(directory):
    """Processes all .txt reports and saves
    - a csv version of each
    - a combined csv version of all of them
    - a sqlite3 database of the combined version"""
    all_txt_files = os.listdir(directory)
    all_txt_files = [
        x for x in all_txt_files if x.endswith(".txt") and x not in BAD_FILES
    ]

    combined_dataframe = pd.DataFrame(columns=HEADER)

    for fle in all_txt_files:
        txt_file_path = os.path.join(directory, fle)
        try:
            clean_lines = extract_outlook(txt_file_path)
            df = pd.DataFrame(clean_lines, columns=HEADER)
            combined_dataframe = pd.concat([combined_dataframe, df], ignore_index=True)
            df.to_csv(os.path.join(directory, fle.replace(".txt", ".csv")), index=False)

        except Exception as e:
            print("Failed to process report")
            print(fle)
            print(e)

    combined_dataframe.to_csv(
        os.path.join(directory, "combined_dataframe.csv"), index=False
    )

    db = sqlalchemy.create_engine("sqlite:///eskom_system_status_reports.sqlite")
    combined_dataframe.to_sql("weekly_system_status_reports", db, if_exists="replace")


if __name__ == "__main__":
    process_all_reports(sys.argv[1])
