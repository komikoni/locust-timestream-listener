import atexit
import gevent
import logging
import sys
import traceback
from datetime import datetime

import boto3
from botocore.config import Config
from locust.exception import InterruptTaskSet
from requests.exceptions import HTTPError
import locust.env
import pprint

log = logging.getLogger('locust_timestream')


class TimestreamSettings:
    """
    Store timestream settings
    """

    def __init__(
        self,
        region_name: str = 'us-east-1',
        # Recommended Timestream write client SDK configuration:
        #  - Set SDK retry count to 10.
        #  - Use SDK DEFAULT_BACKOFF_STRATEGY
        #  - Set RequestTimeout to 20 seconds .
        #  - Set max connections to 5000 or higher.
        read_timeout: int = 20,
        max_pool_connections: int = 5000,
        max_attempts: int = 10,
        database: str = 'default',
        ht_ttl_hours: int = 24,
        ct_ttl_days: int = 7,
        events_table: str = 'locust_events',
        requests_table: str = 'locust_requests',
        exceptions_table: str = 'locust_exceptions',
        interval_ms: int = 1000
    ):
        self.region_name = region_name
        self.read_timeout = read_timeout
        self.max_pool_connections = max_pool_connections
        self.max_attempts = max_attempts
        self.database = database
        self.ht_ttl_hours = ht_ttl_hours
        self.ct_ttl_days = ct_ttl_days
        self.events_table = events_table
        self.requests_table = requests_table
        self.exceptions_table = exceptions_table
        self.interval_ms = interval_ms


class TimestreamListener:
    """
    Events listener that writes locust events to the given timestream connection
    """

    def __init__(
        self,
        env: locust.env.Environment,
        timestreamSettings: TimestreamSettings
    ):

        # flush related attributes
        self.cache = {}
        self.cache[timestreamSettings.events_table] = []
        self.cache[timestreamSettings.requests_table] = []
        self.cache[timestreamSettings.exceptions_table] = []
        self.stop_flag = False
        self.interval_ms = timestreamSettings.interval_ms
        # timestream settings
        self.timestreamSettings = timestreamSettings
        try:
            self.session = boto3.Session(
                region_name=timestreamSettings.region_name)
            self.timestream_client = self.session.client('timestream-write',
                                                         config=Config(read_timeout=timestreamSettings.read_timeout,
                                                                       max_pool_connections=timestreamSettings.max_pool_connections,
                                                                       retries={'max_attempts': timestreamSettings.max_attempts}))
        except:
            logging.exception('Could not connect to timestream')
            return

        # Create Database
        try:
            self.timestream_client.create_database(
                DatabaseName=timestreamSettings.database)
            logging.exception(
                "Database [%s] created successfully." % timestreamSettings.database)
        except self.timestream_client.exceptions.ConflictException:
            logging.exception(
                "Database [%s] exists. Skipping database creation" % timestreamSettings.database)
        except Exception as err:
            logging.exception("Create database failed:", err)

        retention_properties = {
            'MemoryStoreRetentionPeriodInHours': timestreamSettings.ht_ttl_hours,
            'MagneticStoreRetentionPeriodInDays': timestreamSettings.ct_ttl_days
        }
        # Create Events Tables
        try:
            self.timestream_client.create_table(DatabaseName=timestreamSettings.database,
                                                TableName=timestreamSettings.events_table,
                                                RetentionProperties=retention_properties)
            print("Table [%s] successfully created." %
                  timestreamSettings.events_table)
        except self.timestream_client.exceptions.ConflictException:
            print("Table [%s] exists on database [%s]. Skipping table creation" % (
                timestreamSettings.events_table, timestreamSettings.database))
        except Exception as err:
            print("Create table failed:", err)

        # Create Requests Tables
        try:
            self.timestream_client.create_table(DatabaseName=timestreamSettings.database,
                                                TableName=timestreamSettings.requests_table,
                                                RetentionProperties=retention_properties)
            print("Table [%s] successfully created." %
                  timestreamSettings.requests_table)
        except self.timestream_client.exceptions.ConflictException:
            print("Table [%s] exists on database [%s]. Skipping table creation" % (
                timestreamSettings.requests_table, timestreamSettings.database))
        except Exception as err:
            print("Create table failed:", err)

        # Create Exceptions Tables
        try:
            self.timestream_client.create_table(DatabaseName=timestreamSettings.database,
                                                TableName=timestreamSettings.exceptions_table,
                                                RetentionProperties=retention_properties)
            print("Table [%s] successfully created." %
                  timestreamSettings.exceptions_table)
        except self.timestream_client.exceptions.ConflictException:
            print("Table [%s] exists on database [%s]. Skipping table creation" % (
                timestreamSettings.exceptions_table, timestreamSettings.database))
        except Exception as err:
            print("Create table failed:", err)

        # determine if worker or master
        self.node_id = 'local'
        if '--master' in sys.argv:
            self.node_id = 'master'
        if '--worker' in sys.argv:
            # TODO: Get real ID of slaves form locust somehow
            self.node_id = 'worker'

        # start background event to push data to timestream
        self.flush_worker = gevent.spawn(self.__flush_cached_points_worker)
        self.test_start(0)

        events = env.events

        # requests
        events.request_success.add_listener(self.request_success)
        events.request_failure.add_listener(self.request_failure)
        # events
        events.test_stop.add_listener(self.test_stop)
        events.user_error.add_listener(self.user_error)
        events.spawning_complete.add_listener(self.spawning_complete)
        events.quitting.add_listener(self.quitting)
        # complete
        atexit.register(self.quitting)

    def request_success(self, request_type, name, response_time, response_length, **_kwargs) -> None:
        self.__listen_for_requests_events(
            self.node_id, request_type, name, response_time, response_length, True, None)

    def request_failure(self, request_type, name, response_time, response_length, exception, **_kwargs) -> None:
        self.__listen_for_requests_events(
            self.node_id, request_type, name, response_time, response_length, False, exception)

    def spawning_complete(self, user_count) -> None:
        self.__register_event(self.node_id, user_count, 'spawning_complete')
        return True

    def test_start(self, user_count) -> None:
        self.__register_event(self.node_id, 0, 'test_started')

    def test_stop(self, user_count) -> None:
        print('test started')
        self.__register_event(self.node_id, 0, 'test_stopped')

    def test_stop(self, user_count) -> None:
        self.__register_event(self.node_id, 0, 'test_stopped')

    def user_error(self, user_instance, exception, tb, **_kwargs) -> None:
        self.__listen_for_locust_errors(
            self.node_id, user_instance, exception, tb)

    def quitting(self, **_kwargs) -> None:
        self.__register_event(self.node_id, 0, 'quitting')
        self.last_flush_on_quitting()

    def __register_event(self, node_id: str, user_count: int, event: str, **_kwargs) -> None:
        """
        Persist locust event such as hatching started or stopped to timestream.
        Append user_count in case that it exists

        :param node_id: The id of the node reporting the event.
        :param event: The event name or description.
        """

        time = datetime.utcnow()
        tags = {
        }
        fields = {
            'node_id': node_id,
            'event': event,
            'user_count': user_count
        }

        point = self.__make_data_point(tags, fields, time)
        # self.cache[self.timestreamSettings.events_table].append(point) #TODO WIP Debuging

    # def __listen_for_requests_events(self, node_id, measurement, request_type, name, response_time, response_length, success, exception) -> None:
    def __listen_for_requests_events(self, node_id, request_type, name, response_time, response_length, success, exception) -> None:
        """
        Persist request information to timestream.

        :param node_id: The id of the node reporting the event.
        :param measurement: The measurement where to save this point.
        :param success: Flag the info to as successful request or not
        """

        time = datetime.utcnow()
        tags = {
            'node_id': node_id,
            'request_type': request_type,
            'name': name,
            'success': success,
            'exception': repr(exception),
        }

        if isinstance(exception, HTTPError):
            tags['code'] = exception.response.status_code

        fields = {
            'response_time': response_time,
            'response_length': response_length,
            'counter': 1,  # TODO: Review the need of this field
        }
        point = self.__make_data_point(tags, fields, time)
        self.cache[self.timestreamSettings.requests_table].append(point)

    def __listen_for_locust_errors(self, node_id, user_instance, exception: Exception = None, tb=None) -> None:
        """
        Persist locust errors to Timestream.

        :param node_id: The id of the node reporting the error.
        :return: None
        """

        time = datetime.utcnow()
        tags = {
            'exception_tag': repr(exception)
        }
        fields = {
            'node_id': node_id,
            'user_instance': repr(user_instance),
            'exception': repr(exception),
            'traceback': "".join(traceback.format_tb(tb)),
        }
        point = self.__make_data_point(tags, fields, time)
        self.cache[self.timestreamSettings.exceptions_table].append(point)

    def __flush_cached_points_worker(self) -> None:
        """
        Background job that puts the points into the cache to be flushed according tot he interval defined.

        :param timestream_client:
        :param interval:
        :return: None
        """
        log.info('Flush worker started.')
        while not self.stop_flag:
            self.__flush_points(self.timestream_client)
            gevent.sleep(self.interval_ms / 1000)

    # def __make_data_point(self, measurement: str, tags: dict, fields: dict, time: datetime) -> dict:
    def __make_data_point(self, tags: dict, fields: dict, time: datetime) -> dict:
        """
        Create a list with a single point to be saved to timestream.

        :param measurement: The measurement where to save this point.
        :param tags: Dictionary of tags to be saved in the measurement.
        :param fields: Dictionary of field to be saved to measurement.
        :param time: The time os this point.
        """
        # return {"measurement": measurement, "tags": tags, "time": time, "fields": fields}

        current_time = str(int(round(time.timestamp()*1000)))
        # current_time = str(int(round(time.time() * 1000)))
        version = int(current_time)

        dimensions = [{'Name': k, 'Value': str(v)} for k, v in tags.items()]

        common_attributes = {
            'Dimensions': dimensions,
            'MeasureValueType': 'DOUBLE',
            'Time': current_time,
            'Version': version
        }

        records = [{'MeasureName': k, 'MeasureValue': str(v)}
                   for k, v in fields.items()]

        return {"common_attributes": common_attributes, "records": records}

    def last_flush_on_quitting(self):
        self.stop_flag = True
        self.flush_worker.join()
        self.__flush_points(self.timestream_client)

    def __flush_points(self, timestream_client) -> None:
        """
        Write the cached data points to timestream

        :param timestream_client: An instance of TimestreamClient
        :return: None
        """
        log.debug(f'Flushing points {len(self.cache)}')
        to_be_flushed = self.cache
        self.cache = {}
        self.cache[self.timestreamSettings.events_table] = []
        self.cache[self.timestreamSettings.requests_table] = []
        self.cache[self.timestreamSettings.exceptions_table] = []

        # pprint.pprint(to_be_flushed)
        for table_name in to_be_flushed.keys():
            for points in to_be_flushed[table_name]:
                records = points["records"]
                common_attributes = points["common_attributes"]
                try:
                    result = self.timestream_client.write_records(DatabaseName=self.timestreamSettings.database,
                                                                  TableName=table_name,
                                                                  Records=records,
                                                                  CommonAttributes=common_attributes)
                except Exception as err:
                    log.error('Failed to write records to timestream.')
                    pprint.pprint(points)
                    log.error("Error:", err)
                    # If failed for any reason put back into the beginning of cache
                    # self.cache[table_name].insert(0, to_be_flushed[table_name])
                    self.cache[table_name].append(points)
