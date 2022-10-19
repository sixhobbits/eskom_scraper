import os
from bs4 import BeautifulSoup
import requests

def get_pdf_links_from_url(url):
    parser = 'lxml' 
    resp = requests.get(url)
    soup = BeautifulSoup(resp.text, parser)
    pdf_urls = []
    for link in soup.find_all('a', href=True):
        url = link['href']
        if url.endswith("pdf"):
            pdf_urls.append(url)

    return pdf_urls

def save_all_pdfs(pdf_urls, subfolder):
    if not os.path.exists(subfolder):
        os.mkdir(subfolder)
    for url in pdf_urls:
        try:
            filename = url.split("/")[-1]
            file_contents = requests.get(url).content
            with open(f"{subfolder}/{filename}", "wb") as f:
                f.write(file_contents)
        except Exception as e:
           print("error saving pdf")
           print(url)
           print(e)

def scrape_weekly_reports():
    weekly_reports_url = "https://www.eskom.co.za/eskom-divisions/tx/system-adequacy-reports/"
    pdf_urls = get_pdf_links_from_url(weekly_reports_url)
    save_all_pdfs(pdf_urls, "weekly_system_status_reports")

def scrape_annual_reports():
    annual_reporst_url = "https://www.eskom.co.za/investors/integrated-results/"
    pdf_urls = get_pdf_links_from_url(annual_reporst_url)
    save_all_pdfs(pdf_urls, "annual_reports")

if __name__ == '__main__':
    # scrape_weekly_reports()
    scrape_annual_reports()
