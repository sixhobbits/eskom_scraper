import os
import shutil
from collections import defaultdict
import sys
import pandas as pd
import sqlalchemy
import pickle



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
    def f(x):
        print(x)
        try:
            return x["Legend_Descr"][:7] + "-" + x["FIN_YEARS_DESCR"][4:6]
        except:
            print("couldn't fix ocgt fin year")
    df["FinYearMonth"] = df.apply(lambda x: f(x), axis=1)
    df = df.drop(["Legend_Descr", "FIN_YEARS_DESCR"], axis=1)
    return df

def fix_actual_forecast_demand(df):
    # The older files have a forecast and the newer ones have actuals
    # using the default combination method, we end up throwing away the forecast
    # data as it is "NaN" in the newer files.
    # Here we take the latest not-nan values instead
    df['source'] = df['source'].apply(lambda x: x.split("/")[-1][:10])

    # Sort the DataFrame first by DateTimeKey and then by source in descending order
    df = df.sort_values(by=['DateTimeKey', 'source'], ascending=[True, False])

    # Group the DataFrame by 'DateTimeKey'
    grouped = df.groupby('DateTimeKey')

    # Define a custom aggregation function to pick the first non-NaN value in each group
    def first_non_nan(series):
        non_na_series = series.dropna()
        return non_na_series.iloc[0] if not non_na_series.empty else float('nan')


    # Apply the custom aggregation function to each group
    df = grouped.agg({
        'Residual Forecast': first_non_nan,
        'RSA Contracted Forecast': first_non_nan,
        'Residual Demand': first_non_nan,
        'RSA Contracted Demand': first_non_nan,
        'source': 'first'
    }).reset_index()
    return df

def fix_station_build_up(df):
    # Eskom pushed bad data on 24 and 25 april 2023 that showed way 
    # more generation capability across thermal, nuclear and renewables
    # than was real. Ignore this data.
    mask = ~df['source'].str.contains('/2023_04_24T|/2023_04_25T')
    return df[mask]


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
    sorted(csvs)
    print("working on",csvs)
    all_dfs = []

    for csv in csvs:
        csv_date = csv.split("/")[-1][:10]
        try:
            if csv_date < "2023_04_26":
                print("....appending with semicolons",csv)
                df = pd.read_csv(csv, sep=";", decimal=",")
            elif csv_date > "2023_05_01":
                print("....appending with commas",csv)
                df = pd.read_csv(csv, sep=",", decimal=".")
            else:
                print("....skipping:", csv)
                continue
                # we are missing some files between 29 April and 4 May 2023
                # eskom switched 
                # - from semicolon separators and full stop decimals to using commas for everything on 26 April
                # - data from 26->29 april is corrupt and unusable
                # - we are missing data until 5 may, so not sur when they fixed it
                # - 5 may (onwards?) they are using comma separators and full stop decimals

            df['source'] = csv
            all_dfs.append(df)
        except Exception as e:
            print(e)
            # Some of the "CSVs" are actually HTML files showing a 404 page
            # This is probably the case if pandas fails to read the CSV
            print("Couldn't read", csv)

    # Concatenate all dataframes and drop the missing values
    # Then drop duplicates based on the index column as most of the CSVs overlap 
    # e.g. we scrape daily but they cover a 2 week period so we are only getting one new row each day
    # also set the index column and then drop it as it keeps the original one too 
    # Save them all to sqlite and replace the old tables as we are working with a full new dataset of CSVs
    # TODO: Rather append, or improve efficiency some other way as this will get slower over time
    df = pd.concat(all_dfs)
    df.to_csv("tmp.csv")
    if src_folder.endswith("ocgt-usage_Total_monthly_OCGT_Eskom_IPP_and_GT_energy_utilisation"):
        print("applying ocgt usage fix")
        df = fix_Total_monthly_OCGT_Eskom_IPP_and_GT_energy_utilisation(df)

    elif src_folder.endswith("demand-side_System_hourly_actual_and_forecasted_demand"):
        print("applying demand side forcast fix")
        df = fix_actual_forecast_demand(df)

    elif src_folder.endswith("supply-side_Station_Build_Up"):
        print("applying station build up fix")
        df = fix_station_build_up(df)
    print(df.shape)
    print(df.head())
    print("....dropping na and duplicated")
    # with open(table_name + ".pickle", "wb") as f:
    #     pickle.dump(df, f)
    
    ## sometimes everything is blank except date column, but it's ok for some of the forecast demand stuff to be NA
    df = df.dropna(thresh=3)  

    df = df.drop_duplicates(subset=[index_column], keep='last')
    print("....dropped duplicated")
    print(df.shape)
    print(".... setting index")
    df = df.set_index(df[index_column])
    df = df.drop([index_column], axis=1)
    print("....saving to sql")
    print(df.shape)
    print(df.head())
    db = sqlalchemy.create_engine('sqlite:///eskom_metrics.sqlite')
    df.to_sql(table_name, db, if_exists="replace")

def run(dest_folder): 
    scrape_folders = [x for x in os.listdir() if x.startswith("20") and os.path.isdir(x)]
    for src_folder in scrape_folders:
        process_scrape_dir(src_folder, dest_folder)
    save_to_sqlite(os.path.join(dest_folder, "supply-side_Station_Build_Up"), "station_build_up", "Date_Time_Hour_Beginning")
    save_to_sqlite(os.path.join(dest_folder, "outage-performance_Monthly_Eskom_generation_capacity_breakdown"), "monthly_generation_capacity_breakdown", "YEAR_MONTH")
    save_to_sqlite(os.path.join(dest_folder, "ocgt-usage_Total_monthly_OCGT_Eskom_IPP_and_GT_energy_utilisation"), "ocgt_utilisation_monthly_eskom_ipp", "FinYearMonth")
    save_to_sqlite(os.path.join(dest_folder, "ocgt-usage_Load_factor_last_7_Days_OCGT"), "ocgt_utilisation_load_7_days_eskom_ocgt", "Date_Time_Hour_Beginning")
    # eek Start Date;Weekly EAF;Weekly PCLF;Weekly UCLF;Weekly OCLF;Year;Fin_Year
    save_to_sqlite(os.path.join(dest_folder, "outage-performance_Weekly_Eskom_generation_capacity_breakdown"), "outage_performance_weekly_eskom_generation_capacity_breakdown", "Week Start Date")
    save_to_sqlite(os.path.join(dest_folder, "outage-performance_Hourly_UCLF_and_OCLF_Trend"), "outage_performance_hourly_uclf_oclf", "DateTimeKey")
    save_to_sqlite(os.path.join(dest_folder, "outage-performance_Weekly_unplanned_outages"), "outage_performance_weekly_unplanned", "Week_min_DateKey")
    save_to_sqlite(os.path.join(dest_folder, "supply-side_Pumped_storage_gen_hours_gas_generation_and_manual_load_reduction"), "supply_side_pumped_storage", "Date")
    save_to_sqlite(os.path.join(dest_folder, "demand-side_System_hourly_actual_and_forecasted_demand"), "demand_side_actual_forecast_demand", "DateTimeKey")


if __name__ == "__main__":
    dest_folder = sys.argv[1]
    run(dest_folder)
