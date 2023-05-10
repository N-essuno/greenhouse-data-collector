from typing import Optional, Iterable, Union

from influxdb_client import InfluxDBClient, Bucket, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from collector.config import CONFIG_PATH

try:
    # >3.2
    from configparser import ConfigParser
except ImportError:
    # python27
    # Refer to the older SafeConfigParser as ConfigParser
    from configparser import SafeConfigParser as ConfigParser


class InfluxController:
    # https://influxdb-client.readthedocs.io/en/latest/

    def __init__(self) -> None:
        self._client: InfluxDBClient = InfluxDBClient.from_config_file(CONFIG_PATH)

    def create_bucket(self, bucket_name: str) -> Bucket:
        """
        Create a new bucket in InfluxDB with name bucket_name
        :return: the new bucket if created, the existing bucket if it already exists
        """
        bucket = self.get_bucket(bucket_name)
        if bucket is None:
            return self._client.buckets_api().create_bucket(bucket_name=bucket_name)
        else:
            return bucket

    def delete_bucket(self, bucket_name: str) -> bool:
        """
        Delete a bucket from InfluxDB by name
        :return: True if bucket was deleted, False if bucket does not exist
        """
        bucket = self.get_bucket(bucket_name)
        if bucket is None:
            return False
        else:
            self._client.buckets_api().delete_bucket(bucket)
            return True

    def get_bucket(self, bucket_name: str) -> Optional[Bucket]:
        """
        Get a bucket from InfluxDB by name
        :return: bucket if found, None otherwise
        """
        if self._client.buckets_api().find_bucket_by_name(bucket_name) is None:
            self.create_bucket(bucket_name)

        return self._client.buckets_api().find_bucket_by_name(bucket_name)

    def write_point(self, point: Union[Point, Iterable[Point]], bucket: Bucket) -> bool:
        """
        Write a Point or Iterable of Points to bucket
        :param point: measurement to write
        :param bucket: bucket to write to
        :return: True if write was successful, False otherwise
        """
        if self._client.write_api(write_options=SYNCHRONOUS).write(
                bucket=bucket.name, org=self._client.org, record=point
        ) is None:
            return True
        else:
            return False
