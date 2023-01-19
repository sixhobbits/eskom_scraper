# Eskom data

There are a few different sources of Eskom data

* Annual reports (PDF, need manual extraction)
* Weekly reports (PDF, need manual extraction)
* Data dashboards (CSV downloads, need to be scraped daily)
* 5 years of data (big CSV, needs to be requested manually) 

The `eskom_dataroom_scraper.py` file has functions to scrape the PDF and CSV files. Currently this script can grab a full snapshot of all available data, but that's around 300MB per scrape and the annual and weekly reports don't change, so it doesn't make sense to scrape and store everything regularly. 

I've also imported the 5-year data dump as a Sqlite file and create some visualisations that are available on Metabase here https://metabase.dwyer.co.za/public/dashboard/8a1e3f60-e53f-44c4-b045-cdcb35254ecb. Contact me if you'd like to be added to the metabase so you can create your own queries.

## Goals

I still want to create ETL stuff to load data from the weekly CSVs and maybe the PDFs into a centralised database, and add Metabase dashboards for these too. Although Eskom provides some visualisations in their "data room", these often only go back for a limited amount of time, so it's useful to incrementally scrape this data and create longer-term visualisations.


## Codebridge Project

See also https://hackdash.codebridge.org.za/projects/6350fcecb781e300066ea429. I'm on https://zatech.co.za if you want to get involved.
