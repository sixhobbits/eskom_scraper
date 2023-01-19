import os
import shutil
from collections import defaultdict
import sys
import pandas as pd
import sqlalchemy



# we have a timestamped folder for each scrape, with subdirectories per category and files per subdir
# we want to reverse this to group all of the files of the same time into a single directory, e.g. 
# we move from `scrape_date/category/file.csv` to `category_file/scrape_date.csv`



def fix_Total_monthly_OCGT_Eskom_IPP_and_GT_energy_utilisation(df):
    # FIN_YEARS_DESCR	Legend_Descr	ESKOM_OCGT_IPP
    # 202104 / 202204	2021-22 Total (Eskom+IPP)	215,90000000000000
    # 202104 / 202204	2022-23 Total (Eskom+IPP)	327,10000000000000
    # This file is structured very badly. The FIN_YEARS_DESCR contains duplicate values
    # but specifies the month. Legend_Descr contains the "real" year, but not the month
    # so we combine them and parse each into the real date string
    print(df.columns)
    print("--")
    def f(x):
        return x["Legend_Descr"][:7] + "-" + x["FIN_YEARS_DESCR"][4:6]
    df["FinYearMonth"] = df.apply(lambda x: f(x), axis=1)
    df = df.drop(["Legend_Descr", "FIN_YEARS_DESCR"], axis=1)
    return df



def process_scrape_dir(dashboards_scrape_dir, dest_dir):
    print("processing", dashboards_scrape_dir, dest_dir)
    categories = os.listdir(dashboards_scrape_dir)
    categories = [x for x in categories if os.path.isdir(os.path.join(dashboards_scrape_dir, x)) and x != "weekly_system_status_reports" and x!= "annual_reports"]
    if not os.path.isdir(dest_dir):
        os.mkdir(dest_dir)
    for category in categories:
        files = os.listdir(os.path.join(dashboards_scrape_dir, category))
        for fle in files:
            root, ext = os.path.splitext(fle)
            cat_file = f"{category}_{root}"
            cat_file = os.path.join(dest_dir, cat_file)
            if not os.path.isdir(cat_file):
                print(f"making {cat_file}")
                os.mkdir(cat_file)
            original_file = os.path.join(dashboards_scrape_dir, category, fle)
            destination_file = os.path.join(cat_file, dashboards_scrape_dir + ext)
            shutil.copyfile(original_file, destination_file)

def save_to_sqlite(src_folder, table_name, index_column):

    csvs = os.listdir(src_folder)
    csvs = [os.path.join(src_folder, x) for x in csvs if x.endswith(".csv") and x.startswith("20")]
    all_dfs = []

    for csv in csvs:
        try:
            df = pd.read_csv(csv, sep=";", decimal=",")
        except Exception as e:
            # Some of the "CSVs" are actually HTML files showing a 404 page
            # This is probably the case if pandas fails to read the CSV
            print("Couldn't read", csv)
        all_dfs.append(df)

    # Concatenate all dataframes and drop the missing values
    # Then drop duplicates based on the index column as most of the CSVs overlap 
    # e.g. we scrape daily but they cover a 2 week period so we are only getting one new row each day
    # also set the index column and then drop it as it keeps the original one too 
    # Save them all to sqlite and replace the old tables as we are working with a full new dataset of CSVs
    # TODO: Rather append, or improve efficiency some other way as this will get slower over time
    df = pd.concat(all_dfs)
    if src_folder.endswith("ocgt-usage_Total_monthly_OCGT_Eskom_IPP_and_GT_energy_utilisation"):
        print("applying fix")
        df = fix_Total_monthly_OCGT_Eskom_IPP_and_GT_energy_utilisation(df)
    df = df.dropna()
    df = df.drop_duplicates(subset=[index_column])
    df = df.set_index(df[index_column])
    df = df.drop([index_column], axis=1)
    db = sqlalchemy.create_engine('sqlite:///eskom_metrics.sqlite')
    df.to_sql(table_name, db, if_exists="replace")

def run(dest_folder): 
    scrape_folders = [x for x in os.listdir() if x.startswith("20") and os.path.isdir(x)]
    for src_folder in scrape_folders:
        process_scrape_dir(src_folder, dest_folder)
    save_to_sqlite(os.path.join(dest_folder, "supply-side_Station_Build_Up"), "station_build_up", "Date_Time_Hour_Beginning")
    save_to_sqlite(os.path.join(dest_folder, "outage-performance_Monthly_Eskom_generation_capacity_breakdown"), "monthly_generation_capacity_breakdown", "YEAR_MONTH")
    save_to_sqlite(os.path.join(dest_folder, "ocgt-usage_Total_monthly_OCGT_Eskom_IPP_and_GT_energy_utilisation"), "ocgt_utilisation_monthly_eskom_ipp", "FinYearMonth")


if __name__ == "__main__":
    dest_folder = sys.argv[1]
    run(dest_folder)
