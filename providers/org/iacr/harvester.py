import datetime
import logging

from furl import furl
from lxml import etree
import requests
from share import Harvester
from io import StringIO, BytesIO

logger = logging.getLogger(__name__)

class IACRHarvester(Harvester):

    namespaces = {
        'ns0': 'http://www.w3.org/2005/Atom'
    }

    url = 'http://eprint.iacr.org/rss/rss.xml'

    def do_harvest(self, start_date, end_date):
        # IACR has previous-search, you can only go from some past day to today
        return self.fetch_records(self.url, start_date)

    def fetch_records(self, url, start_date):
        records = self.fetch_page(url)

        for index, record in enumerate(records):
            yield (
                record['link'],
                record
            )

    def fetch_page(self, url):
        logger.info('Making request to {}'.format(url))
        data = []
        resp = self.requests.get(url, verify=False)
        parsed = etree.fromstring(resp.content)
        records = parsed.xpath('//item', namespaces=self.namespaces)

        logger.info('Found {} records.'.format(len(records)))
        for record in records:
            title, authors = record[1].text.split(', by')
            data.append({'link':record[0].text, 'description': record[2].text, 'title':title.strip(),'authors':[a.strip(' ') for a in authors.split('and')] })
        return data
