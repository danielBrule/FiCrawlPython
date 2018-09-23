import requests
import xml.etree.ElementTree as ET
from enum import Enum
import pandas as pd
from nltk.stem.snowball import FrenchStemmer
from UpdateSitemapActu.Helper import *


class Newspaper(Enum):
    Lefigaro = 1
    Liberation = 2


DIC_STEMMERS = {"fr": FrenchStemmer()}

DIC_NEWSPAPER_URLSITEMAPACTU = {
    Newspaper.Lefigaro: "http://www.lefigaro.fr/sitemap_actu.xml",
    Newspaper.Liberation: "https://www.liberation.fr/sitemap_news.xml"
}

LIST_COLUMNS_SITEMAPACTU = ["article_id",
                            "title",
                            "publication_date",
                            "last_modification",
                            "newspaper",
                            "url",
                            "priority",
                            "keywords",
                            "language"]


class SitemapActu:
    def __init__(self, newspaper: Newspaper, password: str):
        self._newspaper = newspaper
        self._df = None
        self._df = pd.DataFrame(columns=LIST_COLUMNS_SITEMAPACTU)
        self._password = password

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

            publication_date = date_to_datetime_sql_server(publication_date)
            last_modification = date_to_datetime_sql_server(last_modification)
            if article_id is None or len(article_id) == 0:
                print("Article does not have an ID:{}".format(url))
            else:
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

    def _get_df_keywords(self) -> set:
        keywords_df = self._df.copy()
        all_keyword = keywords_df[["keywords", "language"]]
        all_keyword = split_data_frame_list(all_keyword, "keywords", ",")
        all_keyword['keywords'] = all_keyword.apply(lambda row: clean_keyword(keyword=row['keywords'],
                                                                              language=row['language']),
                                                    axis=1)
        all_keyword = all_keyword[all_keyword['keywords'].map(len) != 0]
        all_keyword['stem_keyword'] = all_keyword['keywords']
        all_keyword['stem_keyword'] = all_keyword.apply(lambda row: clean_keyword(keyword=row['stem_keyword'],
                                                                                  language=row['language'],
                                                                                  stemmers=DIC_STEMMERS),
                                                        axis=1)
        all_keyword = all_keyword.drop_duplicates(subset='stem_keyword')
        return all_keyword

    def _db_update_keywords(self):
        print("Update Keywords")
        df_keywords = self._get_df_keywords()
        keywords_in_db = db_execute_select_query(password=self._password, query="select KeywordID from Ficrawl.keywords")
        keywords_in_db = [x[0] for x in keywords_in_db]
        if len(keywords_in_db) != 0:
            df_keywords = df_keywords.query('stem_keyword not in @keywords_in_db')
        if len(df_keywords) == 0:
            return
        query = "insert into Ficrawl.keywords (KeywordID, FullKeyword) values ('{}','{}')"
        queries = df_keywords.apply(lambda row: build_query(query=query, param=[row['stem_keyword'], row['keywords']]),
                                    axis=1)
        db_execute_insert_update_queries(password=self._password, queries=queries)

    def _db_update_articles(self, articles: pd.DataFrame):
        print("Update Articles")
        if len(articles) == 0:
            return
        query = "Update FiCrawl.articles set HasBeenParsed = 0, LastModificationDate = '{}' where ArticleID='{}'"
        queries = articles.apply(lambda row: build_query(query=query,
                                                           param=[row[LIST_COLUMNS_SITEMAPACTU[3]],
                                                                  row[LIST_COLUMNS_SITEMAPACTU[0]]]),
                                 axis=1)
        db_execute_insert_update_queries(password=self._password, queries=queries)

    def _db_insert_new_articles(self, articles: pd.DataFrame):
        print("Insert new articles")
        if len(articles) == 0:
            return
        query_articles_table = "insert into FiCrawl.articles (ArticleID, Title, PublicationDate, " \
                               "LastModificationDate, NewsPaper, URL, Priority, Language, HasBeenParsed)" \
                               " values('{}','{}','{}','{}','{}','{}','{}','{}','0')"
        queries = articles.apply(lambda row: build_query(query=query_articles_table,
                                                         param=[row[LIST_COLUMNS_SITEMAPACTU[0]],
                                                                row[LIST_COLUMNS_SITEMAPACTU[1]],
                                                                row[LIST_COLUMNS_SITEMAPACTU[2]],
                                                                row[LIST_COLUMNS_SITEMAPACTU[3]],
                                                                row[LIST_COLUMNS_SITEMAPACTU[4]],
                                                                row[LIST_COLUMNS_SITEMAPACTU[5]],
                                                                row[LIST_COLUMNS_SITEMAPACTU[6]],
                                                                row[LIST_COLUMNS_SITEMAPACTU[8]]]),
                                 axis=1)
        db_execute_insert_update_queries(password=self._password, queries=queries)

        queries = []
        query_article_keywords_table = "insert into FiCrawl.ArticleKeywords(ArticleID, keywordid)  values('{}','{}')"
        for idx, row in articles.iterrows():
            list_keywords = keywords_to_list(language=row[LIST_COLUMNS_SITEMAPACTU[8]],
                                             keywords=row[LIST_COLUMNS_SITEMAPACTU[7]],
                                             stemmers=DIC_STEMMERS)
            queries = queries + ([build_query(query=query_article_keywords_table,
                                              param=[row[LIST_COLUMNS_SITEMAPACTU[0]], x])
                                  for x in list_keywords])
        db_execute_insert_update_queries(password=self._password, queries=queries)

    def update_db(self):
        self._db_update_keywords()
        articles_in_db = db_execute_select_query(password=self._password,
                                                 query="select ArticleID from Ficrawl.Articles")
        articles_in_db = [x[0] for x in articles_in_db]

        if len(articles_in_db) != 0:
            articles_to_add = self._df.query('article_id not in @articles_in_db')
            articles_to_update = self._df.query('article_id in @articles_in_db')
            self._db_update_articles(articles=articles_to_update)
            self._db_insert_new_articles(articles=articles_to_add)
        else:
            self._db_insert_new_articles(articles=self._df)


parser = SitemapActu(newspaper=Newspaper.Lefigaro, password="")

parser.download_sitemapactu()
parser.update_db()
