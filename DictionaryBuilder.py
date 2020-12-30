from datetime import datetime
from pyspark import SparkConf
from pyspark.ml.feature import StringIndexer
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
import logging


class DictionaryBuilder:

    def __init__(self):
        self.start_time = self._log_init()
        self._spark = self._create_spark_session()

    @staticmethod
    def _log_init():
        start_time = datetime.now()
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s-%(filename)s.%(funcName)s'
                                   '-%(levelname)s-%(message)s')
        logging.info('-------------------------------------------------------')
        logging.info('----------------- DictionaryBuilder -------------------')
        logging.info('-------------------------------------------------------')
        logging.info('starting...')

        return start_time

    def _log_footer(self):
        logging.info('-------------------------------------------------------')
        logging.info('start: {}'.format(self.start_time))
        logging.info('  end: {}'.format(datetime.now()))

    @staticmethod
    def _create_spark_session():
        logging.info('retrieving SparkSession')
        conf = SparkConf().setAppName('GutenbergDictionaryBuilder')

        return SparkSession.builder.config(conf=conf).getOrCreate()

    def generate_dic(self):
        logging.info('generate_dic() start')

        logging.info('retrieving files content')
        lines_rdd = \
            self._spark \
                .sparkContext \
                .textFile('hdfs:///user/maria_dev/project22/dataset/*')

        logging.info('flattening lines into words')
        words_rdd = lines_rdd.flatMap(lambda line: line.split())

        logging.info('creating words DataFrame')
        words_df = self._spark.createDataFrame(words_rdd, 'string')
        words_df = words_df.withColumnRenamed('value', 'word')

        logging.info('generating ids for the words indexed by frequency')
        indexer_model = StringIndexer(inputCol='word', outputCol='word_id')
        words_dic_df = indexer_model.fit(words_df).transform(words_df)

        logging.info('deduplicating')
        words_dic_df = words_dic_df.distinct()

        logging.info('sorting')
        words_dic_df = words_dic_df.sort('word_id')

        logging.info('casting word_id column from double to string')
        words_dic_df = \
            words_dic_df.withColumn('word_id',
                                    words_dic_df['word_id']
                                    .cast(IntegerType())
                                    .cast(StringType()))
        words_dic_df.printSchema()
        words_dic_df.show(10)
        # logging.debug('words_dic_df.count(): {}'
        #               .format(words_dic_df.count()))

        logging.info('writing dictionary to hdfs')
        words_dic_df\
            .coalesce(1)\
            .write \
            .option('sep', '\t') \
            .mode('overwrite') \
            .csv('hdfs:///user/maria_dev/project22/output/words_dictionary')

        logging.info('closing SparkSession')
        self._spark.stop()
        self._log_footer()


if __name__ == '__main__':
    dic_builder = DictionaryBuilder()

    dic_builder.generate_dic()
