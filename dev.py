import os
from bs4 import BeautifulSoup
import requests
from datetime import datetime

def get_soup_from_url(url):
    parser = 'lxml' 
    resp = requests.get(url)
    soup = BeautifulSoup(resp.text, parser)
    return soup

def get_links_from_url(url):
    soup = get_soup_from_url(url)
    urls = []
    for link in soup.find_all('a', href=True):
        url = link['href']
        urls.append(url)
    return urls

def get_pdf_urls(urls):
    return [x for x in urls if x.endswith(".pdf")]

def get_csv_urls(urls):
    return [x for x in urls if x.endswith(".csv")]

def get_dataportal_urls(urls):
    return [x for x in urls if "/dataportal/" in x and "?page_id=" in x]

def save_all_files_from_urls(urls, subfolder):
    print("save_all_files_from_urls")
    print(urls)
    if not os.path.exists(subfolder):
        os.mkdir(subfolder)
    for url in urls:
        try:
            filename = url.split("/")[-1]
            file_contents = requests.get(url).content
            with open(f"{subfolder}/{filename}", "wb") as f:
                f.write(file_contents)
        except Exception as e:
           print("error saving file")
           print(url)
           print(e)

def scrape_weekly_reports():
    print("scrape_weekly_reports()")
    weekly_reports_url = "https://www.eskom.co.za/eskom-divisions/tx/system-adequacy-reports/"
    urls = get_links_from_url(weekly_reports_url)
    pdf_urls = get_pdf_urls(urls)
    save_all_files_from_urls(pdf_urls, "weekly_system_status_reports")

def scrape_annual_reports():
    print("Scrape_annual_reports()")
    annual_reporst_url = "https://www.eskom.co.za/investors/integrated-results/"
    urls = get_links_from_url(annual_reporst_url)
    pdf_urls = get_pdf_urls(urls)
    save_all_files_from_urls(pdf_urls, "annual_reports")

def scrape_csvs(dashboard_url):
    urls = get_links_from_url(dashboard_url)
    csv_urls = get_csv_urls(urls)
    return csv_urls

def scrape_dashboard(dashboard_url):
    print("scraping dashboard", dashboard_url)
    # a dashboard like "demand side" has links to several reports (also dashboards)
    # each report usually has a single graph + csv download, but some have multiple
    urls = get_links_from_url(dashboard_url)
    dataportal_urls = get_dataportal_urls(urls)
    all_csv_urls = set()
    for dataportal_url in dataportal_urls:
        print(dataportal_url)
        if dashboard_url.endswith("/"):
            subdir = dashboard_url.split("/")[-2]
        else:
            subdir = dashboard_url.split("/")[-1]
        csv_urls = scrape_csvs(dataportal_url)
        all_csv_urls.update(csv_urls)

    # save_all_files_from_urls(all_csv_urls, subdir)

def scrape_all_dashboards():
    print("scrape_all_dashboards()")
    dashboard_urls = [
            'https://www.eskom.co.za/dataportal/outage-performance/',
    ]
    for dashboard_url in dashboard_urls:
        try:
            scrape_dashboard(dashboard_url)
        except Exception as e:
            print("Couldn't scrape dashboard")
            print(dashboard_url)
            print(e)

def run():
    scrape_dir = datetime.now().isoformat().replace(":","_").replace(".", "_").replace("-", "_")
    os.mkdir(scrape_dir)
    os.chdir(scrape_dir)
    # scrape_weekly_reports()
    # scrape_annual_reports()
    scrape_all_dashboards()

if __name__ == '__main__':
    run()
