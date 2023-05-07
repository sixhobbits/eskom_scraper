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
    annual_reports_url = "https://www.eskom.co.za/investors/integrated-results/"
    urls = get_links_from_url(annual_reports_url)
    pdf_urls = get_pdf_urls(urls)
    save_all_files_from_urls(pdf_urls, "annual_reports")

def run():
    try:
        scrape_dir = "weekly_annual_" + datetime.now().isoformat().replace(":","_").replace(".", "_").replace("-", "_")
        os.mkdir(scrape_dir)
        os.chdir(scrape_dir)
        scrape_weekly_reports()
        # scrape_annual_reports()
        # scrape_all_dashboards()
    except Exception as e:
        r = requests.get("https://api.dwyer.co.za/pokegareth/eskomscrapefail")
        print("Couldn't scrape")
        print(e)

if __name__ == '__main__':
    run()
