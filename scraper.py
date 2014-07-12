#!/bin/env python2
from bs4 import BeautifulSoup as bs
from datetime import datetime
import urlparse
import requests
import logging
import time

import scraperwiki


class BratislavaScraper(object):
    LIST_URL_TPL = '/register/vismo/zobraz_dok.asp?id_org=700026&stranka={page}&tzv=1&pocet={limit}&sz=zmena_formalni&sz=nazev&sz=strvlastnik'
    DOMAIN = 'http://www.bratislava.sk'
    LISTING_AMOUNT = 10

    HTTP_OK_CODES = [200]


    def __init__(self, sleep=1/2):
        self.sleep = sleep


    def get_content(self, path):
        url = urlparse.urljoin(self.DOMAIN, path)
        logging.info('Requesting content from url: "{}"'.format(url))
        response = requests.get(url)

        if response.status_code not in self.HTTP_OK_CODES:
            logging.error('Could not load category list from "{}" (CODE: {})'.format(url, response.status_code))

        return response.text


    def scrape(self):
        page = 1
        content = self.get_content(self.LIST_URL_TPL.format(limit=self.LISTING_AMOUNT, page=page))
        self.parse_list(content)
        

    def parse_list(self, html):
        soup = bs(html)

        table = soup.find('div', {'id': 'kategorie'}).find('table', {'class': 'seznam'})
        
        for table_row in table.tbody.find_all('tr'):
            cells = table_row.find_all('td')
            row = {}

            # date
            try:
                row['date'] = datetime.strptime(cells[0].text, '%d.%m.%Y')
            except ValueError:
                row['date'] = cells[0].text

            # person
            if cells[2].find('a'):
                row['responsible_person_id'] = self.scrape_person(cells[2].a)
            else:
                row['responsible_person_id'] = None


    def scrape_person(self, a):
        person_tpl = '/register/vismo/o_osoba.asp?id_org=700026&id_o={}'
        href = urlparse.urlparse(a['href'])
        params = urlparse.parse_qs(href.query)
        id_o = int(params['id_o'][0])
        
        # check whether the person is already in db
        person = scraperwiki.sqlite.select('id FROM people WHERE id=?', data=[id_o])
        if person:
            return id_o

        content = self.get_content(person_tpl.format(id_o))
        person = self.parse_person(content)
        
        logging.info('Inserting person "{}" into database'.format(person['name']))
        scraperwiki.sqlite.save(['id'], person, table_name='people')


    def parse_person(self, html):
        soup = bs(html)

        dl = soup.find('div', {'id': 'osobnost'}).dl
        '''
        ...
        '''




def create_db():
    # TODO try to use ordered dictionary
    people = {'id': 0, 'name': str(), 'email': str()}
    scraperwiki.sqlite.dt.create_table(people, table_name='people', error_if_exists=False)


if __name__ == '__main__':
    # create tables explicitly
    create_db()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.ERROR)
    scraper = BratislavaScraper(sleep=1/10)
    scraper.scrape()