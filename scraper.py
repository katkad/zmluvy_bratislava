#!/bin/env python2
from bs4 import BeautifulSoup as bs
from collections import OrderedDict
from datetime import datetime
import scraperwiki
import urlparse
import requests
import logging
import time

from database import create_db


class BratislavaScraper(object):
    # main domain
    DOMAIN = 'http://www.bratislava.sk'

    # list of documents, sorted by date
    LIST_TPL = '/register/vismo/zobraz_dok.asp?id_org=700026&stranka={page}&tzv=1&pocet={limit}&sz=zmena_formalni&sz=nazev&sz=strvlastnik&sort=zmena_formalni&sc=DESC'

    # path template to page with personal details
    PEOPLE_TPL = '/register/vismo/o_osoba.asp?id_org=700026&id_o={}'

    # path to entry details
    DETAILS_PATH = '/register/vismo/dokumenty2.asp'

    # path to document
    DOCUMENT_PATH = '/register/VismoOnline_ActionScripts/File.ashx'


    LISTING_AMOUNT = 10  # max 100
    HTTP_OK_CODES = [200]


    def __init__(self, sleep=1/2):
        self.sleep = sleep

    @staticmethod
    def get_url_params(href):
        url = urlparse.urlparse(href)
        return urlparse.parse_qs(url.query)

    @staticmethod
    def parse_person_email(html):
        soup = bs(html)

        dl = soup.find('div', {'id': 'osobnost'}).dl
        for dd in dl.find_all('dd'):
            if dd.a:
                return dd.a.text


    def get_content(self, path):
        url = urlparse.urljoin(self.DOMAIN, path)
        logging.info('Requesting content from url: "{}"'.format(url))
        response = requests.get(url)

        if response.status_code not in self.HTTP_OK_CODES:
            logging.error('Could not load category list from "{}" (CODE: {})'.format(url, response.status_code))
            return None

        return response.text


    def scrape(self):
        page = 1
        content = self.get_content(self.LIST_TPL.format(limit=self.LISTING_AMOUNT, page=page))
        if content:
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

            # name/desc/category
            details = self.parse_description(cells[1])

            # TODO
            # if pdf, done
            # else load MORE details and categories?


            # person
            if cells[2].find('a'):
                row['responsible_person'] = self.scrape_person(cells[2].a)
            else:
                # missing responsible person or one without personal page
                row['responsible_person'] = None


    def parse_description(self, node):
        details = {}

        if node.strong and node.strong.a:
            target = node.strong.a['href']
            details['title'] = node.strong.a.text
        elif node.strong:
            target = None
            details['title'] = node.strong.text
        else:
            target = None
            details['title'] = None

        if target:
            url = urlparse.urlparse(target)
            params = self.get_url_params(target)

            if url.path.startswith(self.DETAILS_PATH):
                # this entry has separate page
                if 'id' in params:
                    details['id'] = params['id'][0]

            elif url.path.startswith(self.DOCUMENT_PATH):
                # direct link to (PDF) document
                if 'dokument_id' in params:
                    details['pdf_ids'] = [params['dokument_id'][0]]
                    details['pdf_urls'] = [urlparse.urljoin(self.DOMAIN, target)]
            else:
                # unknow url format
                pass

        # not an url we can continue to, use details provided in the table
        if 'id' not in details:
            details['id'] = None

        # fill in more details in case we can't get them later
        if node.div and node.div.br:
            print node.div.br.previous_sibling
            details['description'] = node.div.br.previous_sibling.text
        elif node.div:
            details['description'] = node.div.text
        else:
            details['description'] = node.text
        
        category = node.find('div', {'class': 'ktg'})
        if category and category.a:
            try:
                params = self.get_url_params(category.a['href'])
                details['category_id'] = params['id_ktg'][0]
            except KeyError:
                details['category_id'] = None
                pass
        else:
            details['category_id'] = None

        return details


    def scrape_person(self, a):
        params = self.get_url_params(a['href'])
        id_o = int(params['id_o'][0])
        
        # check whether the person is already in db
        person = scraperwiki.sqlite.select('id FROM people WHERE id=?', data=[id_o])
        if person:
            return id_o

        person = {}
        person['id'] = id_o
        person['name'] = a.text

        content = self.get_content(self.PEOPLE_TPL.format(id_o))
        if content:
            person['email'] = self.parse_person_email(content)
        else:
            person['email'] = None
        
        logging.info('Inserting person "{}" into database'.format(person['name']))
        scraperwiki.sqlite.save(['id'], person, table_name='people')


if __name__ == '__main__':
    # create tables explicitly
    create_db()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.ERROR)
    scraper = BratislavaScraper(sleep=1/10)
    scraper.scrape()
