import requests
import xml.etree.ElementTree as ET
from enum import Enum
import pandas as pd


class Newspaper(Enum):
    Lefigaro = 1
    Liberation = 2


DIC_NEWSPAPER_URLSITEMAPACTU = {
    Newspaper.Lefigaro: "http://www.lefigaro.fr/sitemap_actu.xml"
}


class SitemapActu:

    def __build_df(self):
        list_columns = ["article_id",
                        "title",
                        "publication_date",
                        "last_modification",
                        "newspaper",
                        "url",
                        "priority",
                        "keywords",
                        "language"]
        self._df = pd.DataFrame(columns=list_columns)

    def __init__(self, newspaper: Newspaper):
        self.data = []
        self._newspaper = newspaper
        self._df = None
        self.__build_df()

    def download_sitemapactu(self):
        site_map_actu_xml = requests.get(DIC_NEWSPAPER_URLSITEMAPACTU[self._newspaper])
        root = ET.fromstring(site_map_actu_xml.text)
        namespaces = {'news': 'http://www.google.com/schemas/sitemap-news/0.9'}
        idx = 1
        for url_node in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
            url = url_node[0].text
            last_modification = url_node[1].text
            priority = url_node[3].text
            title = url_node.find("*/news:title", namespaces).text
            newspaper = url_node.find("*/news:publication/news:name", namespaces).text
            keywords = url_node.find("*/news:keywords", namespaces).text
            publication_date = url_node.find("*/news:publication_date", namespaces).text
            language = url_node.find("*/news:publication/news:language", namespaces).text
            article_id = url.split("/")[-1].split("-")[1]
            self._df.loc[idx] = [article_id,
                                 title,
                                 publication_date,
                                 last_modification,
                                 newspaper,
                                 url,
                                 priority,
                                 keywords,
                                 language]
            idx = idx + 1

    def print_df(self):
        print(self._df)


parser = SitemapActu(newspaper=Newspaper.Lefigaro)

parser.download_sitemapactu()
parser.print_df()
