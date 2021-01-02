from datetime import datetime
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
import logging
import re


class ReverseIndexBuilder:
    FILE_NAME_PATTERN = r'\/(\d+)$'
    DATASET_PATH = 'hdfs:///user/maria_dev/project22/dataset/*'
    DICTIONARY_PATH = \
        'hdfs:///user/maria_dev/project22/output/words_dictionary/*'
    IDX_PATH = 'hdfs:///user/maria_dev/project22/output/word_reverse_idx'
    _LOG_SEP_SIZE = 55

    def __init__(self):
        self._builder_name = 'ReverseIndexBuilder'
        self._start_time = self._log_init(self._builder_name)
        self._spark = self._create_spark_session(self._builder_name)

    @staticmethod
    def _log_init(builder_name):
        start_time = datetime.now()
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s-%(filename)s.%(funcName)s'
                                   '-%(levelname)s-%(message)s')
        logging.info('-' * ReverseIndexBuilder._LOG_SEP_SIZE)
        log_title_sep = \
            '-' * ((ReverseIndexBuilder._LOG_SEP_SIZE - 2 - len(builder_name))
                   / 2)
        logging.info('{} {} {}{}'
                     .format(log_title_sep,
                             builder_name,
                             log_title_sep,
                             '-' if len(builder_name) % 2 == 0 else ''))
        logging.info('-' * ReverseIndexBuilder._LOG_SEP_SIZE)
        logging.info('starting...')

        return start_time

    def _log_footer(self):
        logging.info('-------------------------------------------------------')
        logging.info('start: {}'.format(self._start_time))
        logging.info('  end: {}'.format(datetime.now()))

    @staticmethod
    def _create_spark_session(app_namne):
        logging.info('retrieving SparkSession')
        conf = SparkConf().setAppName(app_namne)

        return SparkSession.builder.config(conf=conf).getOrCreate()

    def get_files_rdd(self):
        logging.info('retrieving files content')

        return self._spark.sparkContext\
            .wholeTextFiles(ReverseIndexBuilder.DATASET_PATH)

    @staticmethod
    def word_file_id_flat_map(rdd_line):
        file_path, file_content = rdd_line
        file_id = re.search(ReverseIndexBuilder.FILE_NAME_PATTERN, file_path)\
            .group(1)

        word_file_list = [(word, file_id) for word in file_content.split()]

        return word_file_list

    def flat_rdd(self, rdd, function):
        logging.info('flattening file_content_rdd')
        logging.info('from: (file_path, file_content)')
        logging.info('  to: (word, doc_id)')

        return rdd.flatMap(function)
    
    def get_dictionary_df(self):
        logging.info('getting dictionary rdd')

        schema = StructType([StructField("word", StringType(), True),
                             StructField("word_id", IntegerType(), True)])
        
        return self._spark.read.csv(ReverseIndexBuilder.DICTIONARY_PATH,
                                    sep='\t',
                                    schema=schema)

    def convert_word_file_rdd_to_df(self, rdd):
        logging.info('converting word_file_rdd from RDD to DataFrame')

        df = self._spark.createDataFrame(rdd, schema=['word', 'doc_id'])

        logging.info('casting column doc_id from string to int')
        df = df.withColumn('doc_id', df['doc_id'].cast(IntegerType()))

        return df

    @staticmethod
    def convert_word_in_word_id(word_file_df, dic_df):
        logging.info('converting word in word_id')

        return word_file_df.join(dic_df, on='word').select('word_id', 'doc_id')

    @staticmethod
    def remove_duplicates(df):
        logging.info('removing ducplicates')

        return df.dropDuplicates()

    @staticmethod
    def group_rdd_by_key(rdd):
        logging.info('grouping doc_id by word_id')

        return rdd.groupByKey()

    @staticmethod
    def sort_rdd(rdd):
        logging.info('sorting word_id and doc_id')

        return rdd.mapValues(sorted).sortByKey()

    def save_rdd_to_hdfs(self, rdd):
        logging.info('converting values array into a string')
        rdd = rdd.mapValues(lambda doc_ids:
                            ', '.join(str(doc_id) for doc_id in doc_ids))

        logging.info('converting word_file_rdd from RDD to DataFrame')
        df = self._spark.createDataFrame(rdd, schema=['word_id', 'doc_ids'])

        logging.info('writing index to hdfs')
        df.coalesce(1)\
            .write \
            .option('sep', '\t') \
            .mode('overwrite') \
            .csv(ReverseIndexBuilder.IDX_PATH)

    @staticmethod
    def print_df_info(df):
        df.printSchema()
        df.show(4)
        print('df.count(): {}'.format(df.count()))

    @staticmethod
    def print_rdd_inf(rdd):
        for element in rdd.take(5):
            print('rdd element: {}, {}'.format(element[0], element[1]))

    @staticmethod
    def print_rdd_with_list_inf(rdd):
        for element in rdd.take(5):
            doc_str = ''
            for doc in element[1]:
                doc_str += str(doc) + ','

            print('rdd element: {}, {}'.format(element[0], doc_str[:-1]))

    def generate_index(self):
        logging.info('method start')

        file_content_rdd = self.get_files_rdd()

        word_file_rdd = self.flat_rdd(file_content_rdd,
                                      self.word_file_id_flat_map)

        word_file_df = self.convert_word_file_rdd_to_df(word_file_rdd)

        dic_df = self.get_dictionary_df()

        word_file_df = self.convert_word_in_word_id(word_file_df, dic_df)

        word_file_df = self.remove_duplicates(word_file_df)
        # self.print_df_info(word_file_df)

        word_file_rdd = self.group_rdd_by_key(word_file_df.rdd)

        word_file_rdd = self.sort_rdd(word_file_rdd)
        # self.print_rdd_with_list_inf(word_file_rdd)

        self.save_rdd_to_hdfs(word_file_rdd)

        self._log_footer()


if __name__ == '__main__':
    revIdxBuilder = ReverseIndexBuilder()
    revIdxBuilder.generate_index()
