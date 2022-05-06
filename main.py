import csv
import json
import os.path
import threading
import traceback

import requests
from bs4 import BeautifulSoup

langs = {
    "EN": "https://medikamio.com/en-gb/diseases/index",
    "DE": "https://medikamio.com/de-de/krankheiten/index",
    "FR": "https://medikamio.com/fr-fr/maladies/index",
    "IT": "https://medikamio.com/it-it/malattie/index",
    "NL": "https://medikamio.com/nl-nl/ziekten/index",
    "ES": "https://medikamio.com/es-es/enfermedades/index",
    "PT": "https://medikamio.com/pt-pt/medicamentos"
}
site = "https://medikamio.com"
headers = ['URL', 'Language', 'Disease', 'International Classification (ICD)', 'Basics', 'Causes', 'Symptoms',
           'Diagnosis', 'Therapy', 'Forecast', 'Prevent', 'Tips', 'Possible risk factors', 'Possible causes',
           'Lifestyle change (without medication)', 'Hypertension and (very) high overall risk',
           'Diabetes (according to European Society of Hypertension)', 'High blood pressure',
           'Severe kidney disease (>1g/d protein in urine)', 'Good blood pressure control, low to moderate risk',
           'Good blood pressure control, high risk', 'CHD, stroke, TIA, kidney disease', 'Start of therapy'
           ]

debug = True
thread_count = 10
semaphore = threading.Semaphore(thread_count)
lock = threading.Lock()
m = f"Medikamio.csv"


def getData(lang, url):
    try:
        print(f"Working on {url}")
        soup = getSoup(url)
        # print(soup)
        data = {
            "URL": url,
            "Language": lang,
            "Disease": soup.find('h1', {'class': "title"}).text,
        }
        for tr in soup.find_all('tr'):
            tds = tr.find_all('td')
            if len(tds) == 2:
                data[tds[0].text] = tds[1].text
        for section in soup.find_all('section', {"class": "content blog-content"}):
            data[section.find('h2').text.strip()] = section.find('div').text
        print(json.dumps(data, indent=4))
        # print(data.keys())
        append(data)
    except:
        if debug:
            traceback.print_exc()
        with open('Error.txt', 'a') as efile:
            efile.write(url + "\n")


def append(data):
    with lock:
        with open(m, 'a', newline='', encoding='utf8', errors='ignore') as outfile:
            csv.DictWriter(outfile, fieldnames=headers).writerow(data)


def main():
    logo()
    scraped = []
    if not os.path.isfile(m):
        with open(m, 'w', newline='') as outfile:
            csv.DictWriter(outfile, fieldnames=headers).writeheader()
    else:
        with open(m) as outfile:
            for line in csv.DictReader(outfile, fieldnames=headers):
                scraped.append(line['URL'])
    threads = []
    for lang in langs.keys():
        url = langs[lang]
        soup = getSoup(url)
        pagecount = soup.find_all('li', {"class": "ais-Pagination-item"})[-1].find('a')['href'].split("=")[-1]
        print(f"{lang} Page count {pagecount}")
        for i in range(1, int(pagecount)):
            print(f"Working on page# {i}")
            for li in soup.find_all('li', {"class": "index-hits-item"}):
                a = f"{site}{li.find('a')['href']}"
                if a not in scraped:
                    t = threading.Thread(target=getData, args=(lang, a,))
                    t.start()
                    threads.append(t)
                else:
                    print(f"Already scraped {a}")
            soup = getSoup(f"{url}?page={i}")
    for thread in threads:
        thread.join()


def logo():
    print(r"""
                         .___.__  __                     .__         
      _____    ____    __| _/|__||  | _______     _____  |__|  ____  
     /     \ _/ __ \  / __ | |  ||  |/ /\__  \   /     \ |  | /  _ \ 
    |  Y Y  \\  ___/ / /_/ | |  ||    <  / __ \_|  Y Y  \|  |(  <_> )
    |__|_|  / \___  >\____ | |__||__|_ \(____  /|__|_|  /|__| \____/ 
          \/      \/      \/          \/     \/       \/             
=========================================================================
            medikamio scraper by github.com/evilgenius786
=========================================================================
[+] Multithreaded
[+] CSV/JSON output
[+] Resumable
_________________________________________________________________________
""")


def getSoup(url):
    return BeautifulSoup(requests.get(url).content, 'lxml')


if __name__ == '__main__':
    main()
