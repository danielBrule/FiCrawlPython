import pandas as pd
from nltk.stem.snowball import _StandardStemmer
import pyodbc
from datetime import datetime


STR_CONNECTION_STRING = "Driver={{ODBC Driver 13 for SQL Server}};" \
                        "Server=tcp:ficrawl-server-db.database.windows.net,1433;" \
                        "Database=FiCrawl;" \
                        "Uid=Ben@ficrawl-server-db;" \
                        "Pwd={};" \
                        "Encrypt=yes;" \
                        "TrustServerCertificate=no;" \
                        "Connection Timeout=30;"


def split_data_frame_list(df: pd.DataFrame, target_column: str, separator: str) -> pd.DataFrame:
    """ df = dataframe to split,
    target_column = the column containing the values to split
    separator = the symbol used to perform the split
    returns: a dataframe with each entry for the target column separated, with each element moved into a new row.
    The values in the other columns are duplicated across the newly divided rows.
    """

    def split_list_to_rows(row, row_accumulator, target_column, separator):
        split_row = row[target_column].split(separator)
        for s in split_row:
            new_row = row.to_dict()
            new_row[target_column] = s
            row_accumulator.append(new_row)

    new_rows = []
    df.apply(split_list_to_rows, axis=1, args=(new_rows, target_column, separator))
    new_df = pd.DataFrame(new_rows)
    return new_df


def date_to_datetime_sql_server(data: str) -> str:
    data = datetime.strptime(data[:-6], '%Y-%m-%dT%H:%M:%S')
    data = data.strftime("%Y%m%d %H:%M:%S")
    return data


def stem_one_keyword(word: str, language: str, stemmers: {_StandardStemmer}) -> str:
    if language in stemmers:
        word = stemmers[language].stem(word)
        return (word)
    else:
        msg = "language '{}' is not supported"
        raise Exception(msg.format(language))


def clean_keyword(language: str, keyword: str, stemmers: {_StandardStemmer} = None) -> str:
    keyword = keyword.lower()
    keyword = keyword.strip()
    if stemmers is not None:
        if language in stemmers:
            keyword = stemmers[language].stem(keyword)
        else:
            msg = "language '{}' is not supported"
            raise Exception(msg.format(language))
    return keyword


def keywords_to_list(language: str, keywords: str, stemmers: {_StandardStemmer}) -> [str]:
    keywords = keywords.split(",")
    keywords = [clean_keyword(language=language, keyword=x, stemmers=stemmers) for x in keywords]
    keywords = [x for x in keywords if len(x) != 0]
    keywords = set(keywords)
    return keywords


def build_query(query: str, param: [str]):
    param = [x.replace("'", "''") for x in param]
    query = query.format(*param)
    return query


def db_execute_select_query(password: str, query: str):
    connection_str = STR_CONNECTION_STRING.format(password)
    connection = pyodbc.connect(connection_str)
    cursor = connection.cursor()
    output = cursor.execute(query).fetchall()
    cursor.close()
    connection.close()
    return output


def db_execute_insert_update_queries(password: str, queries: [str]):
    connection = pyodbc.connect(STR_CONNECTION_STRING.format(password))
    cursor = connection.cursor()

    [cursor.execute(query) for query in queries]

    cursor.commit()
    cursor.close()
    connection.close()
