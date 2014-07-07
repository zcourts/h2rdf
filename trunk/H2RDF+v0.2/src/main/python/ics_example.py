import json
import zookeeper_itf
import ics


test_ics = {
    'apiCrawlSpec': {
        'crawlPeriod': 6.0,
        'keywords': ['barack', 'obama', 'usa']},
    'campaignIdentifier': 'http://crawlspec.arcomem.eu/test_campaign_0',
    'crawlEndDate': '2012-11-11',
    'crawlSpecificationIdentifier':
        'http://crawlspec.arcomem.eu/test_campaign_0-ics-0',
    'crawlStartDate': '2012-12-01',
    'htmlCrawlSpec': {
        'languagePreferences':
            [{'languageIdentifier': 'en', 'weight': 1.0},
             {'languageIdentifier': '*', 'weight': 0.20000000000000001}],
        'seedURLs': [
            'http://news.bbc.co.uk/',
            'http://telegraph.co.uk/']},
    'offlineAnalysiConfig': {},
    'onlineAnalysisConfig': {
        'entityPreferences': [
            {'entityName': 'Barack Obama',
             'entityURI': 'http://dbpedia.org/resource/Barack_Obama',
             'weight': 1.0}],
        'sourceScopes': [
            {'sourceTag': {'CMS': 'wordpress',
                           'media': 'organization_business',
                           'technical': 'blog'},
             'weight': 0.40000000000000002},
            {'sourceTag': {'CMS': 'twitter',
                           'media': 'user_generated_content',
                           'technical': 'social_network'},
             'weight': 1.0}],
        'topicPreferences': [{'topicIdentifier': 'healthcare', 'weight': 1.0}],
        'urlPatterns': [{'pattern': '*news*', 'weight': 1.0}]}}


if __name__ == '__main__':
    rpcq = zookeeper_itf.ZooKeeperRpc("ia200127", 2181)
    ics.put_ics(rpcq, test_ics)
    print ics.ics(rpcq, test_ics['campaignIdentifier'])
