import csv
import pathlib
import re
from datetime import datetime

import luigi
import pandas as pd
import pymongo
import requests
from luigi.format import Nop
from raven import Client

from headers import EVENT_FILE_HEADERS, EVENT_MENTION_FILE_HEADERS, GKG_FILE_HEADERS

sentry = Client(
    "https://8efb97a3904e4d2a9db38d1d21fcba04@o81812.ingest.sentry.io/6190398"
)

PIPELINE = "luigi-etl"
LOCAL_DIR = pathlib.Path(__file__).parent
DATA_DIR = pathlib.Path(LOCAL_DIR).parent.joinpath(f"data/{PIPELINE}")
MASTER_DIR = pathlib.Path(LOCAL_DIR).parent

VERSION = 1

NOTIFICATION_USERS = ["alias1@telecom-paris.com"]

# MongoDB
HOST = 'localhost'
PORT = 27018
INDEX = 'gdelt'
COLLECTION = 'events'

FILE_HEADER = {
    'export': EVENT_FILE_HEADERS,
    'mentions': EVENT_MENTION_FILE_HEADERS,
    'gkg': GKG_FILE_HEADERS,
}

SELECTED_COLUMNS = {
    'export':['GLOBALEVENTID', 'SQLDATE',"Actor2CountryCode", "Actor1CountryCode"
                                   ,'ActionGeo_CountryCode',"AvgTone", "NumArticles"],
    'mentions': ['GLOBALEVENTID', 'MentionDocTranslationInfo'],
    'gkg':["GKGRECORDID", "DATE", "SourceCommonName", "Themes", "Locations", "Persons", "V2Tone", "Amounts"],
}

COLLECTIONS = {
    'export': 'events',
    'mentions': 'eventmentions',
    'gkg': 'gkg',
}


class ArchiveDownloadTask(luigi.Task):
    url: str = luigi.Parameter()
    archive_file_name = luigi.Parameter()
    retry_count = 5

    def output(self):
        lt = luigi.LocalTarget(
            DATA_DIR.joinpath(
                f"{VERSION}/datafiles/{self.archive_file_name}"
            ),
            format=Nop
        )
        return lt

    def run(self):
        try:

            with requests.get(self.url) as r, self.output().open("wb") as f:
                r.raise_for_status()
                f.write(r.content)
        except Exception:
            sentry.captureException()
            raise


class ProcessCSVTask(luigi.Task):
    url: str = luigi.Parameter()
    archive_file_name: str = luigi.Parameter()
    json_file_name = luigi.Parameter(default=None)

    def __init__(self, *args, **kwargs):
        super(ProcessCSVTask, self).__init__(*args, **kwargs)
        self.json_file_name = '.'.join(self.archive_file_name.split('.')[:-1]) + '.json'

    def requires(self):
        return ArchiveDownloadTask(url=self.url, archive_file_name=self.archive_file_name)

    def output(self):
        return luigi.LocalTarget(
            DATA_DIR.joinpath(
                f"{VERSION}/datafiles/{self.json_file_name}"
            ),
        )

    def run(self):
        try:
            matches = re.search(r'(?<=\.)\w+(?=\.)', self.input().path)
            file_type = matches[0].lower()
            header = FILE_HEADER[file_type]

            df = pd.read_csv(self.input().path, names=header, usecols=SELECTED_COLUMNS[file_type],
                             delimiter='	', index_col=False, encoding='latin1')

            mongo_client = pymongo.MongoClient(HOST, PORT)
            collection = COLLECTIONS[file_type]
            mongo_client[INDEX][collection].insert_many(df.to_dict('records'))
            with self.output().open("wb") as f:
                f.write("1")

        except Exception:
            sentry.captureException()
            raise


class RunFileProcessingTask(luigi.WrapperTask):
    batch_dt: datetime = luigi.DateParameter(default=datetime.today())
    url: str = luigi.Parameter()

    def requires(self):
        archive_file_name: str = self.url.split('/')[-1]
        yield ProcessCSVTask(url=self.url, archive_file_name=archive_file_name)


class RunProcessTask(luigi.WrapperTask):
    def requires(self):
        with open(MASTER_DIR / 'masterfilelist-urls.txt') as f:
            csv_reader = csv.reader(f)
            rows = list(csv_reader)
        return [RunFileProcessingTask(url=row[0]) for row in rows[:1000]]



