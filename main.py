import concurrent.futures
import csv
import logging
import sys
import time

import numpy as np
import re
import requests

from bs4 import BeautifulSoup

class Dict_Scraper:
    def __init__(self, language):
        self.language = language
        self.logger = Dict_Scraper.set_logger(f"{self.language}_dict")
        self.func_dict = {"ilocano":self.preprocess_ilocano, 
                          "tagalog":self.preprocess_tagalog,
                          "cebuano":self.preprocess_cebuano,
                          "hiligaynon":self.preprocess_hiligaynon}

    def __str__(self):
        return f"{self.language} Dictionary Scraper"
    

    def send_request(self, link, **kwargs):
        for i in range(3):
            try:
                self.logger.info('Attempt number %s of %s', i+1, 3)
                r = requests.get(link, **kwargs, timeout=30)
                break
            except requests.exceptions.ConnectTimeout as err:
                self.logger.debug('Connection Timemout Error: %s', err)
                time.sleep(np.random.choice([x/10 for x in range(7,22)]))
                continue
            except requests.exceptions.RequestException:
                self.logger.debug('Connection Failed Error: %s', err)
                sys.exit()
        else:
            self.logger.debug('Number of attemps exceeded. Exiting script')
            sys.exit()
        return r


    def preprocess_ilocano(self, definition):
        pattern = re.compile(r'^(.*\.)\s*(.*)')
        if match := re.search(pattern, definition):
            speech_part = match.group(1)
            meaning = match.group(2)
        else:
            speech_part = None
            meaning = definition
        return speech_part, meaning

    def preprocess_tagalog(self, definition):
        speech_part = None
        return speech_part, definition

    def preprocess_hiligaynon(self, definition):
        speech_part = None
        return speech_part, definition
    
    def preprocess_cebuano(self, definition):
        speech_part = None
        return speech_part, definition

    def parse_response(self, letter):
        terms_list = []
        link = f'https://{self.language}.pinoydictionary.com/list/{letter}/'
        while True:
            self.logger.info('Connecting to %s', link)
            r = self.send_request(link)
            if r.status_code == 404:
                break
            else:
                soup = BeautifulSoup(r.text, 'lxml')
                tag_list = soup.find_all(name="div", class_="word-group")
                for word in tag_list:
                    term = word.find(name="h2", class_="word-entry").text
                    definition = word.find(name="div", class_="definition").text
                    # self.logger.debug('%s, %s', term, definition)
                    speech_part, meaning = self.func_dict[self.language](definition)
                    terms_list.append((term, speech_part, meaning))
                try:
                    link = soup.find(name="a", title="Next Page").get("href")
                except AttributeError:
                    break
        return terms_list
    

    def save_data(self, terms_list):
        filename = f'./scraped_data/{self.language}.csv'
        header = ["term", "speech_part", "meaning"]
        with open(filename, "w", encoding="utf-8", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(terms_list)

    def main(self):
        alphabet = list(map(chr, range(97, 123)))
        terms_list = []
        # alphabet = ('a', 'b')

        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(self.parse_response, alphabet)

            for result in results:
                terms_list.extend(result)

        self.logger.info('Wrote %s number of words', len(terms_list))
        self.save_data(terms_list)


    @classmethod
    def initialize(cls):
        language = input('Language: ')
        return cls(language)
    
    @classmethod
    def set_logger(cls, filename):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler(f"./logs/{filename}.log", mode="w", encoding='utf-8')
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

if __name__ == '__main__':
    start = time.perf_counter()

    app=Dict_Scraper.initialize()
    app.main()

    finish = time.perf_counter()

    app.logger.info('Finished in %s second(s)', {round(finish-start,2)})

    