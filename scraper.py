# weekly reports scraper

from bs4 import BeautifulSoup
import requests

weekly_reports_url = "https://www.eskom.co.za/eskom-divisions/tx/system-adequacy-reports/"

parser = 'lxml' 
resp = requests.get(weekly_reports_url)
soup = BeautifulSoup(resp.text, parser)

for link in soup.find_all('a', href=True):
    url = link['href']
    if url.endswith("pdf"):
        filename = url.split("/")[-1]
        file_contents = requests.get(url).content
        with open(f"weekly_system_status_reports/{filename}", "wb") as f:
            f.write(file_contents)

