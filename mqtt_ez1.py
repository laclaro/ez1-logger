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

# test only, do not actually read from inverter
TEST = False

# time in seconds (min: 60)
POLL_PERIOD_SECONDS = 300

# log file
LOG_FILE = '/var/log/apsystems-ez1/ez1.csv'

# MQTT Configuration
MQTT_IP = "192.168.1.100"  # Replace with your broker's address
CA_CERT = "/etc/mosquitto/certs/ca.crt"
MQTT_PORT = 8883  # Broker port (default for unencrypted MQTT)
MQTT_USER = "mqtt_client"
MQTT_PASSWORD = "secret"
MQTT_CLIENT_ID = f'ez1-get-data-mqtt-{randint(0, 1000)}'
MQTT_TOPIC_BASE = "solar/ez1"

# Initialize the inverter with the specified IP address and port number.
INVERTER_IP = "192.168.1.25"
INVERTER_PORT = 8050
ASSUME_INVERTER_OFFLINE_AFTER_SECONDS = 600

COORDINATES = (51.31667, 9.5)

if TEST:
    pass
LOG_FILE = 'ez1.csv'
CA_CERT = "ca.crt"

# Initialize logging
logger = logging.getLogger()
logging.basicConfig(level=logging.DEBUG)


def get_seconds_until_daylight(coordinates: tuple = (52.52, 13.4050)):
    """"""
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


def get_lifetime_production(df):
    series_sum = df[["daily_production_1", "daily_production_2"]]\
        .rename(columns={'daily_production_1': 'lifetime_production_1',
                         'daily_production_2': 'lifetime_production_2'}).sum()
    return series_sum.map('{:,.2f}'.format)


def get_daily_max(df):

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


async def get_data_from_inverter(inverter, test=TEST):
    now = datetime.now()
    if not test:
        response = await inverter.get_output_data()
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
            "daily_production_1": num+1,
            "daily_production_2": num-1
        }


async def log_to_file(row_list: dict):
    # add timestamp if not present
    if "timestamp_ms" not in row_list:
        row_list["timestamp_ms"] = int(datetime.now().timestamp() * 1000)

    # Writing to a CSV file asynchronously with aiofiles
    async with aiofiles.open(LOG_FILE, mode="a") as log_file:
        row_str = ";".join(map(str, row_list.values()))  # Join values with ';'
        await log_file.write(f"{row_str}\n")
        logger.debug(f"Logged data: {row_str}")


async def publish_to_mqtt(mqtt_client: mqtt.Client, topic: str, payload_dict: dict):
    if "timestamp_ms" not in payload_dict:
        payload_dict["timestamp_ms"] = int(datetime.now().timestamp() * 1000)
    payload_json_str = json.dumps(payload_dict)
    mqtt_client.publish(topic, payload_json_str)
    logger.info(f"Published data to MQTT at topic {topic}: {payload_json_str}")


async def calc_publish_statistics(mqtt_client: mqtt.Client, log_file: str | Path,
                                  topic_base: str):
    # read log file
    df = pd.read_csv(log_file, sep=";")

    # get daily max values as dict
    df_max = get_daily_max(df)

    # Publish daily maximal values via MQTT as JSON
    payload_dict = df_max.to_dict(orient="records")
    await publish_to_mqtt(mqtt_client, f"{topic_base}/max", payload_dict)

    # Publish total lifetime production in kWh via MQTT as JSON
    series_lifetime_production = get_lifetime_production(df_max)
    payload_dict = series_lifetime_production.to_dict()
    await publish_to_mqtt(mqtt_client, f"{topic_base}/lifetime_production", payload_dict)


async def get_publish_data_during_daylight(mqtt_client: mqtt.Client, inverter: APsystemsEZ1M,
                                           topic_base: str, log_file: str | Path,
                                           coordinates: tuple):
    """Main loop querying the EZ1 inverter publishing the data, logging to file,
    calculating statistical values from the log file and publish those as well.

    :param mqtt_client: paho mqtt client object
    :param inverter: ez1 inverter object
    :param topic_base: mqtt topic base path
    :param log_file: log file
    :param coordinates: coordinates tuple
    """
    inverter_unresponsive_seconds = 0
    while True:
        seconds_until_daylight = get_seconds_until_daylight(coordinates)
        if seconds_until_daylight == 0:
            try:
                # get data from the inverter
                payload_dict = await get_data_from_inverter(inverter)

                # Log data to cv file
                await log_to_file(payload_dict)

                # Publish the same data to MQTT as JSON
                await publish_to_mqtt(mqtt_client, f"{topic_base}/data", payload_dict)

                # calculate statistics from log file and publish data
                await calc_publish_statistics(mqtt_client, log_file, topic_base=topic_base)

            except Exception as e:
                # exception: inverter is unresponsive
                logger.debug(f"Error: {e}. Inverter not reachable.")
                await mqtt_set_inverter_online_state(mqtt_client, topic_base, state=0)

                if -1 < inverter_unresponsive_seconds <= ASSUME_INVERTER_OFFLINE_AFTER_SECONDS:
                    inverter_unresponsive_seconds += POLL_PERIOD_SECONDS
                else:
                    await log_to_file({"power_1": 0, "power_2": 0})
                    await publish_to_mqtt(mqtt_client, f"{topic_base}/data", {"power_1": 0, "power_2": 0})
                    inverter_unresponsive_seconds = -1             

            else:
                # no exception: inverter online. publish online state of inverter
                await mqtt_set_inverter_online_state(mqtt_client, topic_base, state=1)
                inverter_unresponsive_seconds = 0

            finally:
                await asyncio.sleep(POLL_PERIOD_SECONDS)

        else:
            await asyncio.sleep(seconds_until_daylight)


async def mqtt_set_inverter_online_state(mqtt_client, topic_base, state=0):
    timestamp_ms = int(datetime.now().timestamp() * 1000)
    await publish_to_mqtt(mqtt_client, f"{topic_base}/online",
                          {"timestamp": timestamp_ms, "value": state})


def initialize_mqtt_client(client_id=MQTT_CLIENT_ID, host=MQTT_IP, port=MQTT_PORT,
                           ca_certs=CA_CERT, user=None, password=None):

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logger.info("Connected to MQTT Broker!")
        else:
            logger.warning(f"Failed to connect with result code {rc}")

    def on_disconnect(client, userdata, rc):
        if rc != 0:
            logger.warning("Unexpected MQTT disconnection. Will auto-reconnect")

    mqtt_client = mqtt.Client(client_id=client_id)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    if ca_certs:
        mqtt_client.tls_set(ca_certs=ca_certs)
        mqtt_client.tls_insecure_set(True)
    if user and password:
        mqtt_client.username_pw_set(user, password)
    mqtt_client.connect(host, port, 60)
    mqtt_client.loop_start()  # Start the loop for MQTT client to handle messaging
    return mqtt_client


def initialize_log_file(log_file, header):
    # create folder and file for logging
    if not Path(LOG_FILE).parent.exists():
        Path(LOG_FILE).parent.mkdir(exist_ok=True)
    if not Path(LOG_FILE).exists():
        logger.info(f"Creating file {LOG_FILE}")
        with open(LOG_FILE, mode='w') as log_file:
            log_file.write(f"{header}\n")


if __name__ == "__main__":

    logger.info(f"Logging to file: {LOG_FILE}")
    logger.info(f"Poll period time: {POLL_PERIOD_SECONDS} s")

    csv_header = "date;timestamp_ms;power_1;power_2;"\
        "daily_production_1;daily_production_2"
    initialize_log_file(log_file=LOG_FILE, header=csv_header)

    # initialize inverter object
    ez1_inverter = APsystemsEZ1M(INVERTER_IP, INVERTER_PORT)

    # initialize mqtt client
    mqtt_client = initialize_mqtt_client(client_id=MQTT_CLIENT_ID, ca_certs=CA_CERT,
                                         host=MQTT_IP, port=MQTT_PORT,
                                         user=MQTT_USER, password=MQTT_PASSWORD)

    # start the event loop
    asyncio.run(get_publish_data_during_daylight(mqtt_client=mqtt_client, inverter=ez1_inverter,
                                                 topic_base=MQTT_TOPIC_BASE, log_file=LOG_FILE,
                                                 coordinates=COORDINATES))
