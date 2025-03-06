#!/opt/apsystems-ez1-logger/venv/bin/python3

import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
import json
from random import random, randint
import asyncio
import aiofiles
import pandas as pd
from suntime import Sun
import paho.mqtt.client as mqtt
from APsystemsEZ1 import APsystemsEZ1M

DEFAULT_COLUMNS_LIST = (
    "date", "timestamp_ms", "power_1", "power_2",
    "daily_production_1", "daily_production_2"
)

MQTT_DEFAULT_CONFIG = {
    "host": "localhost",
    "ca_certs": None,
    "port": 1883,
    "user": None,
    "password": None,
    "client_id": f'ez1-get-data-mqtt-{randint(0, 1000)}'
    }

INVERTER_DEFAULT_CONFIG = {
    "ip_address": "192.168.1.25",
    "port": 8050,
    "timeout": 15,
    "max_power": 800,
    "min_power": 30
}

# Initialize logging
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)


class EZ1Logger:
    def __init__(self, mqtt_config, inverter_config, log_file, poll_period, coordinates, mqtt_topic_base="solar/ez1",
                 assume_inverter_offline_after_seconds=300, test_mode=False, log_columns_list=()):
        self.test_mode = test_mode

        logger.info("Initializing EZ1Logger.")
        logger.info(f"Logging to file: {log_file}")
        logger.info(f"Poll period time: {poll_period} s")

        # create log file if it does not exist
        self.log_columns_list = log_columns_list or DEFAULT_COLUMNS_LIST
        self.log_file = Path(log_file)
        if not self.log_file.exists():
            self.data_log_init()

        self.poll_period = poll_period

        self.mqtt_config = MQTT_DEFAULT_CONFIG.copy()
        self.mqtt_config.update(mqtt_config)

        self.inverter_config = INVERTER_DEFAULT_CONFIG.copy()
        self.inverter_config.update(inverter_config)

        self.assume_inverter_offline_after_seconds = assume_inverter_offline_after_seconds
        self.coordinates = coordinates

        self.inverter = APsystemsEZ1M(**self.inverter_config)

        self.mqtt_client = self.get_mqtt_client(**self.mqtt_config)
        self.mqtt_topic_base = mqtt_topic_base

    @staticmethod
    def get_mqtt_client(client_id: str, host: str, port: str, user: str = None,
                        password: str = None, ca_certs: str = None, insecure: bool = True):
        """Initialize and return MQTT Client object.

        :param client_id: client id
        :param host: hostname or IP address
        :param port: the port of the MQTT server
        :param user: username, defaults to None
        :param password: user password, defaults to None
        :param ca_certs: CA certificate file path, defaults to None
        :param insecure: If TLS is used: verify certificates, defaults to True
        """
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                logger.info("Connected to MQTT Broker!")
            else:
                logger.warning(f"Failed to connect with result code {rc}")

        def on_disconnect(client, userdata, rc):
            if rc != 0:
                logger.debug(f"Unexpected disconnection from MQTT server at {client.host}. Reconnecting.")

        mqtt_client = mqtt.Client(client_id=client_id)
        mqtt_client.on_connect = on_connect
        mqtt_client.on_disconnect = on_disconnect
        if ca_certs:
            mqtt_client.tls_set(ca_certs=ca_certs)
            if insecure:
                mqtt_client.tls_insecure_set(True)
        if user and password:
            mqtt_client.username_pw_set(user, password)
        mqtt_client.connect(host, port, 60)
        #mqtt_client.loop_start()
        return mqtt_client

    def data_log_init(self):
        # create folder and file for logging
        self.log_file.parent.mkdir(exist_ok=True)
        if not self.log_file.exists():
            logger.info(f"Creating file {self.log_file}")
            with open(self.log_file, mode='w') as log_file:
                log_file.write(f"{';'.join(self.log_columns_list)}\n")

    @staticmethod
    def get_seconds_until_daylight(coordinates):
        # Get current local time (system time, which is already in Berlin time)
        now_local = datetime.now()

        lat = coordinates[0]
        lon = coordinates[1]

        # Create a Sun object for Berlin (coordinates 52.52, 13.4050)
        sun = Sun(lat, lon)

        # Get today's sunrise time in local time (Berlin time)
        today_midnight = datetime.combine(now_local.date(), datetime.min.time())
        sunrise_naive = sun.get_sunrise_time(today_midnight)
        sunset_naive = sun.get_sunset_time(today_midnight)

        # Make sure sunrise and sunset times are timezone-aware (convert to Berlin time)
        sunrise = sunrise_naive.replace(tzinfo=now_local.tzinfo)
        sunset = sunset_naive.replace(tzinfo=now_local.tzinfo)

        # If it's already after sunset today, get the next day's sunrise time
        if now_local > sunset:
            # Get the next day's sunrise time
            next_day = datetime.combine(now_local.date() + timedelta(days=1), datetime.min.time())
            sunrise_naive = sun.get_sunrise_time(next_day)
            sunrise = sunrise_naive.replace(tzinfo=now_local.tzinfo)

        # If it's daylight (between sunrise and sunset), return 0 seconds
        if sunrise <= now_local <= sunset:
            return 0  # It's daylight, no need to wait for daylight

        # Otherwise, calculate time difference in seconds until the next sunrise
        time_diff = sunrise - now_local
        return time_diff.total_seconds()

    @staticmethod
    def calc_lifetime_production(df):
        series_sum = df[["daily_production_1", "daily_production_2"]]\
            .rename(columns={'daily_production_1': 'lifetime_production_1',
                             'daily_production_2': 'lifetime_production_2'}).sum()
        return series_sum.map('{:,.2f}'.format)

    @staticmethod
    def calc_daily_max(df: pd.DataFrame) -> pd.DataFrame:
        """Group entries by date and calculate the maximal values for every day.

        :param df: input pandas DataFrame
        """
        # Convert 'date' column to datetime format
        df['date'] = pd.to_datetime(df['date'])

        # Extract date only (without time) to group by day
        df['date_only'] = df['date'].dt.date

        # Group by the date (ignoring time) and calculate the max values for each column
        df_max = df.groupby('date_only').agg({
            'power_1': 'max',
            'power_2': 'max',
            'daily_production_1': 'max',
            'daily_production_2': 'max'
        }).reset_index()

        # add timestamp
        df_max['timestamp_ms'] = pd.to_datetime(df_max['date_only']).astype(int) / 10**6
        del df_max["date_only"]

        return df_max

    async def get_data_from_inverter(self) -> dict:
        """Query data from EZ1 inverter and return the data as dictionary.
        If self.test_mode is set, return random data.
        """
        now = datetime.now()
        if not self.test_mode:
            response = await self.inverter.get_output_data()
            return {
                "date": now.strftime("%Y-%m-%d %H:%M:%S"),
                "power_1": response.p1,
                "power_2": response.p2,
                "daily_production_1": response.e1,
                "daily_production_2": response.e2
            }
        else:
            num = random()
            return {
                "date": now.strftime("%Y-%m-%d %H:%M:%S"),
                "power_1": num,
                "power_2": -num,
                "daily_production_1": num + 1,
                "daily_production_2": num - 1
            }

    async def log_to_file(self, data_row: dict):
        """Write row to data log."""
        # add timestamp if not present
        if "timestamp_ms" not in data_row:
            data_row["timestamp_ms"] = int(datetime.now().timestamp() * 1000)

        # assume that data_row has identical keys to log_columns_list
        data_row = {key: data_row[key] for key in self.log_columns_list}

        # Writing to a CSV file asynchronously with aiofiles
        async with aiofiles.open(self.log_file, mode="a") as log_file:
            row_str = ";".join(map(str, data_row.values()))  # Join values with ';'
            await log_file.write(f"{row_str}\n")
            logger.debug(f"Logged data: {row_str}")

    async def publish_to_mqtt(self, topic_name: str, payload_dict: dict | list[dict]):
        """Publish given payload data dictionary via MQTT

        :param topic_name: the last part of the topic
        :param payload_dict: dictionary with data to publish
        """
        if self.mqtt_client.is_connected():
            if isinstance(payload_dict, dict):
                if "timestamp_ms" not in payload_dict:
                    payload_dict["timestamp_ms"] = int(datetime.now().timestamp() * 1000)
            topic = f"{self.mqtt_topic_base}/{topic_name}"
            payload_json_str = json.dumps(payload_dict)
            msg_info = self.mqtt_client.publish(topic, payload_json_str)

            logger.info(f"Published data to MQTT at topic {topic}: {payload_json_str}")
            return msg_info
        else:
            logger.warning("Could not connect to MQjTT server!")
            return None

    async def calc_publish_statistics(self):
        """Read log file data, calculate several things such as daily maxima and lifetime production
        and publish statistical data via MQTT."""
        # read log file
        df = pd.read_csv(self.log_file, sep=";")

        # get daily max values as dict
        df_max = self.calc_daily_max(df)

        # Publish daily maximal values via MQTT as JSON
        payload_dict = df_max.to_dict(orient="records")
        await self.publish_to_mqtt("max", payload_dict)

        # Publish total lifetime production in kWh via MQTT as JSON
        series_lifetime_production = self.calc_lifetime_production(df_max)
        payload_dict = series_lifetime_production.to_dict()
        await self.publish_to_mqtt("lifetime_production", payload_dict)

    async def main_loop_get_publish_data_during_daylight(self):
        """Main loop querying the EZ1 inverter publishing the data, logging to file,
        calculating statistical values from the log file and publish those as well.
        """
        inverter_unresponsive_seconds = 0
        while True:
            seconds_until_daylight = self.get_seconds_until_daylight(self.coordinates)
            if seconds_until_daylight == 0:
                try:
                    self.mqtt_client.loop_start()
                    await asyncio.sleep(3)

                    if not self.mqtt_client.is_connected():
                        logger.warning("Could not connect to MQTT server!")
                    else:

                        # get data from the inverter
                        payload_dict = await self.get_data_from_inverter()

                        # Log data to cv file
                        await self.log_to_file(payload_dict)

                        # Publish the same data to MQTT as JSON
                        await self.publish_to_mqtt("data", payload_dict)

                        # calculate statistics from log file and publish data
                        await self.calc_publish_statistics()

                    self.mqtt_client.loop_stop()

                except KeyError as e:
                    # exception: inverter is unresponsive
                    logger.debug(f"{type(e).__name__}: {e}. Inverter not reachable.")
                    await self.mqtt_set_inverter_online_state(state=False)

                    if -1 < inverter_unresponsive_seconds <= self.assume_inverter_offline_after_seconds:
                        inverter_unresponsive_seconds += self.poll_period
                    else:
                        await self.log_to_file({"power_1": 0, "power_2": 0})
                        await self.publish_to_mqtt("data", {"power_1": 0, "power_2": 0})
                        inverter_unresponsive_seconds = -1
                else:
                    # no exception: inverter online. publish online state of inverter
                    await self.mqtt_set_inverter_online_state(state=True)
                    inverter_unresponsive_seconds = 0

                finally:
                    self.mqtt_client.loop_stop()
                    await asyncio.sleep(self.poll_period)
                    self.mqtt_client.loop_start()
                    await asyncio.sleep(5)

            else:
                self.mqtt_client.loop_stop()
                await asyncio.sleep(seconds_until_daylight)
                self.mqtt_client.loop_start()
                await asyncio.sleep(5)

    async def mqtt_set_inverter_online_state(self, state=True):
        timestamp_ms = int(datetime.now().timestamp() * 1000)
        await self.publish_to_mqtt("online", {"timestamp": timestamp_ms, "value": state})

    def run(self):
        """Run main loop."""
        asyncio.run(self.main_loop_get_publish_data_during_daylight())


if __name__ == "__main__":

    # test only, do not actually read from inverter
    test_mode = False

    log_file = "/var/log/apsystems-ez1/ez1.csv"
    poll_period_seconds = 300
    mqtt_topic_base = "solar/ez1"
    coordinates = (51.31667, 9.5)
    assume_inverter_offline_after_seconds = 600

    # this could be read from config files
    mqtt_config = {
        "host": "192.168.1.100",
        "ca_certs": "/etc/mosquitto/certs/ca.crt",
        "port": 8883,
        "user": "mqtt_client",
        "password": "secret",
        "client_id": f'ez1-get-data-mqtt-{randint(0, 1000)}',
    }

    inverter_config = {
        "ip_address": "192.168.1.25",
        "port": 8050,
        "timeout": 20,
        "max_power": 800,
        "min_power": 30
    }

    if test_mode:
        log_file = './ez1.csv'
        mqtt_config["ca_certs"] = None

    ez1_logger = EZ1Logger(mqtt_config, inverter_config,
                           log_file=log_file,
                           coordinates=coordinates,
                           mqtt_topic_base=mqtt_topic_base,
                           poll_period=poll_period_seconds,
                           assume_inverter_offline_after_seconds=assume_inverter_offline_after_seconds)
    ez1_logger.run()
