import boto3
import io
import logging
import os
import sys
import awswrangler as wr
from awsglue.utils import getResolvedOptions



class RawProcessing():
    def __init__(self) -> None:
        args = getResolvedOptions(
            sys.argv,
            ['aws_bucket_source',
            'aws_bucket_target',
            'database',
            'table',
            'crawler_name'])
        self.s3_resource = boto3.resource('s3')
        self.glue_client = boto3.client('glue')
        self.aws_bucket_source = args['aws_bucket_source']
        self.aws_bucket_target = args['aws_bucket_target']
        self.crawler_name = args['crawler_name']
        self.boto_session = boto3.session.Session(region_name='us-east-1')
        self.db = args['database']
        self.tb = args['table']

    def df_to_parquet(self, dataframe, partition_cols, aws_bucket, db, tb):
        """
        Save a Parquet file in S3.
        :param dataframe: 
        :param s3: s3 resouce
        :param aws_bucket: bucket where information is putting
        :param filename: name used for file
        """
        wr.s3.to_parquet(
            df=dataframe,
            path=f's3://{aws_bucket}',
            index=False,
            compression='snappy',
            boto3_session=self.boto_session,
            dataset=True,
            partition_cols=partition_cols,
            concurrent_partitioning=True,
            catalog_versioning=True,
            mode='overwrite_partitions',
            database=db,
            table=tb,
            table_type='EXTERNAL_TABLE'
            )

    def read_csv_s3(self, aws_bucket, filename, sep=','):
        df = wr.s3.read_csv(
            path=f's3://{aws_bucket}/{filename}', 
            sep=sep, 
            header=0, 
            index_col=False
            )
        return df
    
    def start_crawler(self, name):
        self.glue_client.start_crawler(
            Name=name
)

    def processing_data(self):
        """
        Save a Parquet file in S3.
        :param dataframe: 
        :param s3: s3 resouce
        :param aws_bucket: bucket where information is putting
        :param filename: name used for file
        """
        for obj in self.s3_resource.Bucket(self.aws_bucket_source).objects.all():
            file_name = obj.key
            name = os.path.splitext(file_name)[:-1][0]
            partition_city = name.split('-')[1]

            #Read csv into dataframe
            df = self.read_csv_s3(self.aws_bucket_source, file_name)
            df['city'] = partition_city

            # Identify day, month and year for star range date
            date_range = df['Date Range'].unique()
            for date in date_range:
                month, day, year = date.split('-')[0].split('/')
                df.loc[df['Date Range'] == date, 'month'] = month
                df.loc[df['Date Range'] == date, 'day'] = day
                df.loc[df['Date Range'] == date, 'year'] = year

            #Change data types
            df['Origin Movement ID'] = df['Origin Movement ID'].astype('object')
            df['Destination Movement ID'] = df['Destination Movement ID'].astype('object')
            df['Mean Travel Time (Seconds)'] = df['Mean Travel Time (Seconds)'].astype('float')
            df['Range - Lower Bound Travel Time (Seconds)'] = df['Range - Lower Bound Travel Time (Seconds)'].astype('float')
            df['Range - Upper Bound Travel Time (Seconds)'] = df['Range - Upper Bound Travel Time (Seconds)'].astype('float')
            partition_cols = ['city', 'month', 'day', 'year']

            self.df_to_parquet(df, partition_cols, self.aws_bucket_target, self.db, self.tb)
            # self.start_crawler(self.crawler_name)

if __name__ == '__main__':
    process = RawProcessing()
    process.processing_data()
