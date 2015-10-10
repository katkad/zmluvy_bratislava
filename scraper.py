#!/bin/env python2
from bs4 import BeautifulSoup as bs
from datetime import datetime
import scraperwiki
import urlparse
import requests
import logging
import json

from database import create_db


class BratislavaScraper(object):
    # main domain
    DOMAIN = 'http://www.bratislava.sk'

    # list of documents, sorted by date
    LIST_TPL = '/register/vismo/zobraz_dok.asp?id_org=700026&stranka={page}&tzv=1&pocet={limit}&sz=zmena_formalni&sz=nazev&sz=strvlastnik&sort=zmena_formalni&sc=DESC'

    # path template to page with personal details
    PEOPLE_TPL = '/register/vismo/o_osoba.asp?id_org=700026&id_o={}'

    # entry details page
    DETAILS_PATH = '/register/vismo/dokumenty2.asp'
    DETAILS_TPL = '/register/vismo/dokumenty2.asp?id_org=700026&id={}&p1=15331'

    # path to document
    DOCUMENT_PATH = '/register/VismoOnline_ActionScripts/File.ashx'
    DOCUMENT_TPL = '/register/VismoOnline_ActionScripts/File.ashx?id_org=700026&id_dokumenty={}'


    LISTING_AMOUNT = 100  # max 100
    MAX_PAGES = 1
    HTTP_OK_CODES = [200]


    def __init__(self, sleep=1/2):
        self.sleep = sleep


    @staticmethod
    def get_url_params(href):
        url = urlparse.urlparse(href)
        return urlparse.parse_qs(url.query)


    def doc_url_to_id(self, url):
        params = self.get_url_params(url)
        if 'id_dokumenty' in params:
            return params['id_dokumenty'][0]
        else:
            return None


    def parse_person_email(self, html):
        soup = bs(html, "html.parser")

        dl = soup.find('div', {'id': 'osobnost'}).dl
        for dd in dl.find_all('dd'):
            if dd.a:
                return dd.a.text


    def get_content(self, path):
        '''
        Download webpage.
        '''
        url = urlparse.urljoin(self.DOMAIN, path)
        logging.info('Requesting content from url: "{}"'.format(url))
        response = requests.get(url)

        if response.status_code not in self.HTTP_OK_CODES:
            logging.error('Could not load category list from "{}" (CODE: {})'.format(url, response.status_code))
            return None

        return response.text


    def scrape(self):
        '''
        Main entry point
        '''
        # TODO check pages from actual page OR check for unknow page result
        for page in xrange(1, self.MAX_PAGES + 1):
            content = self.get_content(self.LIST_TPL.format(limit=self.LISTING_AMOUNT, page=page))
            if not content:
                break
             
            if self.parse_list(content) is None:
                break
        

    def parse_list(self, html):
        '''
        Parse results table = get date, details, person and possibly additional documents
        '''
        soup = bs(html, "html.parser")

        table = soup.find('div', {'id': 'kategorie'}).find('table', {'class': 'seznam'})
        
        for table_row in table.tbody.find_all('tr'):
            cells = table_row.find_all('td')

            # name/desc/category
            row = self.parse_description(cells[1])

            if row['html_id']:
                # we have link for details page
                row['document_urls'] = self.scrape_details(row['html_id'])

            # load document ids from document urls
            row['document_ids'] = map(self.doc_url_to_id, row['document_urls']) if row['document_urls'] else None

            # date
            try:
                row['date'] = datetime.strptime(cells[0].text, '%d.%m.%Y')
            except ValueError:
                row['date'] = cells[0].text

            # person
            if cells[2].find('a'):
                row['responsible_person'] = self.scrape_person(cells[2].a)
            else:
                # missing responsible person or one without personal page
                row['responsible_person'] = None

            # explicitly convert documents (urls and ids) to json
            row['document_urls'] = json.dumps(row['document_urls'])
            row['document_ids'] = json.dumps(row['document_ids'])

            # update db and decide what to do next
            # we wither have html_id (higher priority) or list of document ids; if not, we don't save the entry
            if row['html_id']:
                html_id = scraperwiki.sqlite.get_var('html_id')
                if html_id == row['html_id']:
                    logging.info('Reached known result (html_id): "{}"'.format(html_id))
                    break
                scraperwiki.sqlite.save_var('html_id', row['html_id'])

            elif row['document_ids']:
                doc_ids = scraperwiki.sqlite.get_var('doc_ids')
                if doc_ids == row['document_ids']:
                    logging.info('Reached known result (doc_ids): "{}"'.format(doc_ids))
                    break
                scraperwiki.sqlite.save_var('doc_ids', row['document_ids'])

            else:
                logging.error('Not enough data to save this entry: {}: "{}"'.format(row['date']. row['title']))
                continue

            try:
                scraperwiki.sqlite.save(['html_id', 'document_ids'], row, table_name='data')
            except:
                print row
                raise

        else:
            # for ended without break
            return True


    def scrape_details(self, page_id):
        '''
        For given page id, return list of documents + process categories.
        '''
        content = self.get_content(self.DETAILS_TPL.format(page_id))
        soup = bs(content, "html.parser")
        links = soup.find('div', {'class': 'odkazy'})

        document_urls = []
        for li in links.find_all('li'):
            if not li.a:
                continue

            if li.a['href'].startswith(self.DOCUMENT_PATH):
                document_urls.append(urlparse.urljoin(self.DOCUMENT_PATH, li.a['href']))

        # TODO process categories while we have the document

        return document_urls


    def parse_description(self, node):
        '''
        Parse "Nazov" column from list of documents and return available details.

        - title, short description, link either to details page or document, category
        '''
        # default None values
        data = {'title': None,
                'document_urls': None,
                'html_id': None
                }

        # title
        if node.strong and node.strong.a:
            target = node.strong.a['href']
            data['title'] = node.strong.a.text
        elif node.strong:
            target = None
            data['title'] = node.strong.text
        else:
            target = None

        # either document or details url
        if target:
            url = urlparse.urlparse(target)
            params = self.get_url_params(target)

            if url.path.startswith(self.DETAILS_PATH):
                # this entry has separate page
                if 'id' in params:
                    data['html_id'] = params['id'][0]

            elif url.path.startswith(self.DOCUMENT_PATH):
                # direct link to (PDF) document
                if 'dokument_id' in params:
                    data['document_urls'] = [urlparse.urljoin(self.DOMAIN, target)]
            else:
                # other url formats
                pass

        # fill in description
        if node.div and node.div.br:
            data['description'] = unicode(node.div.br.previous_sibling)
        elif node.div:
            data['description'] = node.div.text
        else:
            data['description'] = node.text
        
        # category
        category = node.find('div', {'class': 'ktg'})
        if category and category.a:
            try:
                params = self.get_url_params(category.a['href'])
                data['category_id'] = params['id_ktg'][0]
            except KeyError:
                data['category_id'] = None
                pass
        else:
            data['category_id'] = None

        return data


    def scrape_person(self, a):
        '''
        Download person info and save it to db if it's not already there.
        '''

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
        
        # logging.info('Inserting person "{}" into database'.format(person['name']))
        scraperwiki.sqlite.save(['id'], person, table_name='people')

        return id_o


if __name__ == '__main__':
    # create tables explicitly
    create_db()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    scraper = BratislavaScraper(sleep=1/10)
    scraper.scrape()
