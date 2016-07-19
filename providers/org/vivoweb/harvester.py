"""
A VIVO harvester for the SHARE project
This harvester makes several SPARQL queries to a VIVO SPARQL endpoint,
the information to access the VIVO endpoint must be provided in the local.py file.
There is also a Map to the SPARQL queries made to harvest documents
from the VIVO endpoint in the sparql_mapping.py file.
"""
from share.harvest.harvester import Harvester

import sparql_mapping as mapping
from SPARQLWrapper import SPARQLWrapper, JSON
from document import RawDocument
from datetime import date, timedelta
import json
import logging

VIVO_ACCESS = {
    'url': 'http://dev.vivo.ufl.edu/',
    'query_endpoint': 'http://dev.vivo.ufl.edu/api/sparqlQuery',
    'username': 'fake_user@ufl.edu',
    'password': 'fakepassword'
}

logger = logging.getLogger(__name__)

def build_properties(*args):
    ret = []
    for arg in args:
        name, expr = arg[0], arg[1]
        kwargs = arg[2] if len(arg) > 2 else {}
        description, uri = kwargs.get('description'), kwargs.get('uri')
        ret.append(build_property(name, expr, description=description, uri=uri))
    return ret


def build_property(name, expr, description=None, uri=None):
    property = {
        'name': CONSTANT(name),
        'properties': {
            name: expr
        },
    }
    if description:
        property['description'] = CONSTANT(description)
    if uri:
        property['uri'] = CONSTANT(uri)
    return property

def datetime_formatter(datetime_string):
    '''Takes an arbitrary date/time string and parses it, adds time
    zone information and returns a valid ISO-8601 datetime string
    '''
    date_time = parser.parse(datetime_string)
    if not date_time.tzinfo:
        date_time = date_time.replace(tzinfo=pytz.UTC)
    return date_time.isoformat()

class VivoHarvester(Harvester):
    namespaces = {
        'ns0': 'http://www.w3.org/2005/Atom'
    }

    short_name = 'vivo'
    long_name = 'VIVO'
    url = VIVO_ACCESS['url']
    base_url = VIVO_ACCESS['query_endpoint']
    sparql_wrapper = SPARQLWrapper(base_url)
    sparql_wrapper.setReturnFormat(JSON)
    sparql_wrapper.addParameter("email", VIVO_ACCESS['username'])
    sparql_wrapper.addParameter("password", VIVO_ACCESS['password'])
    sparql_wrapper.method = 'GET'

    def __init__(self, app_config):
        super().__init__(app_config)
        self.start_page_num = 0

    def get_records(self, uris, sparql_mapping):
        records = []
        for uri in uris:
            record = {}
            record['uri'] = uri
            for sparql_map in sparql_mapping:
                if sparql_map['type'] == 'string':
                    record[sparql_map['name']] = self.get_string(uri, sparql_map)
                if sparql_map['type'] == 'array':
                    record[sparql_map['name']] = self.get_array(uri, sparql_map)
                if sparql_map['type'] == 'dict':
                    record[sparql_map['name']] = self.get_dict(uri, sparql_map)
            record['authors'] = self.complete_authors(record['authors'])
            records.append(record)
        return records

    def get_total(self, start_date, end_date):
        query_str = self.GET_TOTAL_QUERY.format(start_date.isoformat(), end_date.isoformat())
        self.sparql_wrapper.setQuery(query_str)
        result = self.sparql_wrapper.query()
        result = result.convert()
        return int(result['results']['bindings'][0]['total']['value'])

    def get_uris(self, start_date, end_date, limit, offset):
        query_str = self.GET_URIS_QUERY.format(start_date.isoformat(), end_date.isoformat(), limit, offset)
        self.sparql_wrapper.setQuery(query_str)
        results = self.sparql_wrapper.query()
        results = results.convert()
        return [result['uri']['value'] for result in results['results']['bindings']]

    def do_harvest(self, start_date, end_date):
        # Arxiv does not have filter dates; can sort by last updated
        start_date = start_date or date.today() - timedelta(2)
        end_date = end_date or date.today()
        # Fetch records is a separate function for readability
        # Ends up returning a list of tuples with provider given id and the document itself
        return self.fetch_records(self.url, start_date)

    def fetch_records(self, url, start_date):
        doc_list = []
        for i in xrange(0, total, 1000):
            uris = self.get_uris(start_date, end_date, 1000, i)
            records = self.get_records(uris, mapping.DOCUMENT_MAPPING)
            logger.info('Harvested {} documents'.format(i + len(records)))

            for record in records:
                if 'doi' in record:
                    doc_id = record['doi']
                else:
                    doc_id = record['uri']
                doc_list.append(RawDocument({
                    'doc': json.dumps(record),
                    'source': self.short_name,
                    'docID': doc_id,
                    'filetype': 'json'
                }))

        return doc_list
