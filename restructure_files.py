import pandas as pd
import os
import shutil
from collections import defaultdict

scrape_folders = [x for x in os.listdir() if x.startswith("20") and os.path.isdir(x)]

# we have a timestamped folder for each scrape, with subdirectories per category and files per subdir
# we want to reverse this to group all of the files of the same time into a single directory, e.g. 
# we move from `scrape_date/category/file.csv` to `category_file/scrape_date.csv`

def process_scrape_dir(dashboards_scrape_dir, dest_dir):
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
            print("copying file")
            print(original_file)
            print(destination_file)
            print("--")
            shutil.copyfile(original_file, destination_file)
            
for folder in scrape_folders:
    process_scrape_dir(folder, "restr_test")
