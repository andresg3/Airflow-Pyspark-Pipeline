from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType
from pyspark.sql.functions import udf
from time import strptime
from datetime import datetime


def create_spark_session():
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3").getOrCreate()
    return spark


# A Simple udf to remove extra spaces from text
remove_extra_spaces = udf(lambda x: ' '.join(x.split()), StringType())


# A udf to transform timestamp format
@udf(TimestampType())
def stringtodatetime(datestring):
    x = datestring.split()
    day, month, year = int(x[2]), strptime(x[1], '%b').tm_mon, int(x[5])
    hour, minute, second = [int(val) for val in x[3].split(":")]
    return datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)


def process_author_data(source_path, dest_path, spark):
    author_df = spark.read.csv(source_path + '/author.csv', header=True, mode='PERMISSIVE', inferSchema=True)

    author_lookup_df = author_df.groupBy('author_id').agg(
        F.max('record_create_timestamp').alias('record_create_timestamp'))
    author_lookup_df.persist()
    F.broadcast(author_lookup_df)

    uniques_author = author_df.join(author_lookup_df, ['author_id', 'record_create_timestamp'], how='inner').select(
        author_df.columns).withColumn('name', remove_extra_spaces('name'))

    uniques_author.repartition(10).write.csv(dest_path + '/authors/', sep='|', mode='overwrite', compression='gzip',
                                             header=True, timestampFormat='yyyy-MM-dd HH:mm:ss.SSS', quote='"',
                                             escape='"')


def process_reviews_data(source_path, dest_path, spark):
    reviews_df = spark.read.csv(source_path + '/reviews.csv', header=True, mode='PERMISSIVE', inferSchema=True,
                                quote="\"", escape="\"")

    reviews_lookup_df = reviews_df.groupBy('review_id').agg(
        F.max('record_create_timestamp').alias('record_create_timestamp'))

    reviews_lookup_df.persist()
    F.broadcast(reviews_lookup_df)

    unique_reviews_df = reviews_df.join(reviews_lookup_df, ['review_id', 'record_create_timestamp'],
                                        how='inner').select(reviews_df.columns)

    unique_reviews_df = unique_reviews_df.withColumn('review_added_date',
                                                     stringtodatetime('review_added_date')).withColumn(
        'review_updated_date', stringtodatetime('review_updated_date'))

    unique_reviews_df.repartition(10).write.csv(path=dest_path + '/reviews/', sep='|', mode='overwrite',
                                                compression='gzip', header=True,
                                                timestampFormat='yyyy-MM-dd HH:mm:ss.SSS', quote='"', escape='"')


def process_book_data(source_path, dest_path, spark):
    books_df = spark.read.csv(source_path + '/book.csv', header=True, mode='PERMISIVE', inferSchema=True, quote="\"",
                              escape="\"")

    books_lookup_df = books_df.groupBy('book_id').agg(F.max('record_create_timestamp').alias('record_create_timestamp'))

    books_lookup_df.persist()
    F.broadcast(books_lookup_df)

    unique_books_df = books_df.join(books_lookup_df, ['book_id', 'record_create_timestamp'], how='inner').select(
        books_df.columns)

    unique_books_df.repartition(10).write.csv(path=dest_path + '/books/', sep='|', mode='overwrite', compression='gzip',
                                              header=True, timestampFormat='yyyy-MM-dd HH:mm:ss.SSS', quote='"',
                                              escape='"')


def process_user_data(source_path, dest_path, spark):
    users_df = spark.read.csv(source_path + '/user.csv', header=True, mode='PERMISSIVE', inferSchema=True, quote="\"",
                              escape="\"")

    users_lookup_df = users_df.groupBy('user_id').agg(F.max('record_create_timestamp').alias('record_create_timestamp'))

    users_lookup_df.persist()
    F.broadcast(users_lookup_df)

    unique_users_df = users_df.join(users_lookup_df, ['user_id', 'record_create_timestamp'], how='inner').select(
        users_df.columns)

    unique_users_df.repartition(10).write.csv(path=dest_path + '/users/', sep='|', mode='overwrite', compression='gzip',
                                              header=True, timestampFormat='yyyy-MM-dd HH:mm:ss.SSS', quote='"',
                                              escape='"')


def main():
    source_path = './data_files'
    dest_path = './processed_files'
    spark = create_spark_session()

    process_author_data(source_path, dest_path, spark)
    process_reviews_data(source_path, dest_path, spark)
    process_book_data(source_path, dest_path, spark)
    process_user_data(source_path, dest_path, spark)


if __name__ == '__main__':
    main()
