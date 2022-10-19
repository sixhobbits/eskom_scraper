# convert the PDFs to text with layout using
# for f in ./*.pdf; do pdftotext $f -layout; done

import os

eaf_prefix = "Energy Availability Factor (Eskom EAF)"

txt_files = os.listdir(os.getcwd())
txt_files = [x for x in txt_files if x.startswith("Weekly_System_Status_Report_") and x.endswith("txt")]

all_eafs = []

for tf in txt_files:
    year, week = tf.split("Weekly_System_Status_Report_")[1].split(".")[0].split("_")
    year = int(year)
    week = week[1:]  # remove w suffix
    if len(week) == 1:
        week = "0" + week
    with open(tf) as f:
        lines = f.read().split("\n")
    for line in lines:
        if line.startswith(eaf_prefix):
            _, eafs = line.split(eaf_prefix)
            eafs = eafs.split()
            eaf = float(eafs[13]) # gives 14 weeks history, we only want this week

            all_eafs.append(f"{year}_{week},{eaf}")
all_eafs = sorted(all_eafs)
for eaf in all_eafs:
    print(eaf)




