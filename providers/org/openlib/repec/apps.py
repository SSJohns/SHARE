from share.provider import OAIProviderAppConfig


class AppConfig(OAIProviderAppConfig):
    name = 'providers.org.openlib.repec'
    version = '0.0.1'
    title = 'repec'
    long_title = 'Research Papers in Economics'
    home_page = 'http://repec.org/'
    url = 'http://oai.repec.openlib.org'
    time_granularity = False
    emitted_type = 'preprint'