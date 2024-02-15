"""
Starts the data collection process on the Raspberry Pi. Different threads
are assigned to different sensors and periodically collect the data from them
and send it to the InfluxDB database.

Should be run from the root of the project as: python3 -m collector
"""
import json
import logging
import os
import signal
import sys
import traceback
import stomp # for message broker
import socket # for hostname retrieval
import re # to split the hostname in letters and numbers

from sys import argv
from threading import Thread
from time import sleep
from typing import Dict, List, Tuple

import board
from adafruit_dht import DHT22

from collector.assets import utils
from collector.assets.asset import Asset
from collector.config import CONFIG_PATH
from collector.demo.demo_influx import demo
from collector.influx.influx_controller import InfluxController
from collector.sensors.humidity import Humidity
from collector.sensors.temperature import Temperature

try:
    # >3.2
    from configparser import ConfigParser
except ImportError:
    # python27
    # Refer to the older SafeConfigParser as ConfigParser
    from configparser import SafeConfigParser as ConfigParser

from collector.assets.greenhouse_asset import GreenhouseAsset
from collector.assets.plant_asset import PlantAsset
from collector.assets.pot_asset import PotAsset
from collector.assets.shelf_asset import ShelfAsset
from collector.sensors.light_level import LightLevel
from collector.sensors.mcp3008 import MCP3008
from collector.sensors.moisture import Moisture
from collector.sensors.ndvi import NDVI
from collector.queue.subscriber import Subscriber

# We need to use board instead of initializing the pins manually like 'Pin(12)'
# because in this way we have a wrapper that works on every Raspberry Pi model
pinlist = [getattr(board, f"D{i}") for i in range(26)]

# Used to read the .ini configuration file
conf: ConfigParser = ConfigParser()
conf.read(CONFIG_PATH)

# Initialize Analog to Digital Converter (ADC) used to convert analog signal from moisture sensors
mcp3008 = MCP3008()


def main():
    """
    Initialize and starts the threads that will read the data from the sensors and send it to the database.
    The parameters of the sensors and assets are read from the configuration file as specified in the README.
    Plus, an extra thread will be used for the Message Broker.
    """
    thread = Thread(target=__wait_message)
    thread.start()

    utils.setup_logging()
    utils.create_stop_script()

    signal.signal(signal.SIGINT, signal_handler)

    asset_list = init_threads()
    logging.info("Threads started")

    sync_config_file(asset_list)


def load_env_file(env_file_path=".env"):
    try:
        with open(env_file_path, "r") as file:
            for line in file:
                # Ignore lines that are empty or start with '#'
                if not line.strip() or line.startswith("#"):
                    continue

                # Split the line at the first '=' character
                key, value = line.strip().split("=", 1)

                # Set the environment variable
                os.environ[key] = value

    except FileNotFoundError:
        print(f"{env_file_path} not found. Make sure to create a .env file with your environment variables.")


def __wait_message():
    """
    Wait for a message from the broker
    """
    load_env_file()
    url = os.getenv("URL")
    username = os.getenv("USER")
    password = os.getenv("PASS")

    # Get hostname for the queue
    hostname = socket.gethostname()
    r = re.compile("([a-zA-Z]+)([0-9]+)")
    m = r.match(hostname)
    # Match the tuple <T.i.A>
    queue_destination = m.group(1) + "." + m.group(2) + ".config.change"

    conn = stomp.Connection(host_and_ports=[(url, 61613)])
    conn.set_listener('', Subscriber(conn, conf, CONFIG_PATH))
    conn.connect(username, password, wait=True)
    conn.subscribe(destination=queue_destination, id=1, ack='auto')

    while 1:
        sleep(10)


def signal_handler(signal, frame):
    print("Terminating data-collector and threads...")
    logging.info("Terminating data-collector and threads...")
    sys.exit(0)


def sync_config_file(thread_list: List[Tuple[Asset, Thread]]):
    """
    Checks if the configuration file has been modified and if so, it stops the threads and restarts them.
    They will read the new parameters from the configuration file.
    :param thread_list: list of tuples (asset, thread)
    """
    last_edited: float = os.path.getmtime(CONFIG_PATH)
    while True:
        newLastEdited = os.path.getmtime(CONFIG_PATH)
        if newLastEdited > last_edited:
            print("Config file changed, restarting threads...")
            logging.info("Config file changed, restarting threads...")
            last_edited = newLastEdited

            for asset, thread in thread_list:
                asset.stop_thread()
                thread.join()
            thread_list = init_threads()
            print("Threads restarted")
            logging.info("Threads restarted")
        sleep(20)


def init_threads() -> List[Tuple[Asset, Thread]]:
    """
    Initializes the threads that will read the data from the sensors and send it to the database.
    For each asset, a thread is created and a related tuple (asset, thread) is added to a list and returned.
    :return: list of tuples (asset, thread)
    """

    # List of tuple asset, thread that will be started
    asset_list: List[Tuple[Asset, Thread]] = []

    # Gets switch from config file to enable/disable sensors
    use_infrared_sensor = conf.getboolean("sensor_switches", "use_infrared_sensor")
    use_light_sensor = conf.getboolean("sensor_switches", "use_light_sensor")

    # Initialize InfluxController singleton
    try:
        influx_controller = InfluxController()
    except Exception as e:
        print("Error creating InfluxController: " + str(e))
        print("Traceback:\n" + traceback.format_exc())
        logging.error("Error creating InfluxController: " + str(e))
        logging.error("Traceback:\n" + traceback.format_exc())
        raise e

    try:
        influx_controller.create_bucket("greenhouse")
    except Exception as e:
        print("Error creating bucket: " + str(e))
        print("Traceback:\n" + traceback.format_exc())
        logging.error("Error creating bucket: " + str(e))
        logging.error("Traceback:\n" + traceback.format_exc())
        raise e

    # Initialize the threads that will read the data from the pots' sensors
    print("Initializing threads...")
    pot_threads = init_pots_threads()
    shelf_threads = init_shelf_thread()

    greenhouse_threads = []
    if use_light_sensor:
        greenhouse_threads = init_greenhouse_thread()

    plant_threads = []
    if use_infrared_sensor:
        plant_threads = init_plants_threads()

    # Add all the threads to a common list in order to be controlled
    asset_list.extend(pot_threads)
    asset_list.extend(shelf_threads)
    asset_list.extend(greenhouse_threads)
    asset_list.extend(plant_threads)

    for asset, thread in asset_list:
        asset.reset_stop_flag()
        thread.daemon = True
        thread.start()

    return asset_list


def init_plants_threads() -> List[Tuple[Asset, Thread]]:
    """
    Initializes the threads that will read the data from the plants' sensors.
    :return: list of tuples (plant, thread)
    """
    # List of tuples (plant, thread) that will be started
    plant_list = []

    plants = conf.items("plants")
    # A shared NDVI sensor is used for all the plants in the shelf.
    ndvi = NDVI()
    for _, plant_parameters in plants:
        plant_dict = json.loads(plant_parameters)
        plant_id = plant_dict["plant_id"]
        plant = PlantAsset(plant_id, ndvi)
        thread_plant = Thread(target=plant.read_sensor_data)
        plant_list.append((plant, thread_plant))

    return plant_list


def init_shelf_thread() -> List[Tuple[ShelfAsset, Thread]]:
    """
    Initializes the threads that will read the data from the shelf's sensors.
    :return: list of tuples (shelf, thread)
    """
    # List of tuples (shelf, thread) that will be started
    shelf_list = []

    shelves = conf.items("shelves")
    # for now we only have one data-collector per shelf
    shelf_dict: Dict = json.loads(shelves[0][1])

    # In this case temperature and humidity sensors are on the same pin,
    # we can use either of the temperature or humidity gpio_pin on the raspberry
    dht22_gpio_pin = shelf_dict["temperature_gpio_pin"]
    # The DHT22 sensor is used for both temperature and humidity
    # It is initialized with the pin number taken from the configuration file and
    # the pinlist initialized above in order to have a wrapper that works on every Raspberry Pi model
    humidity_temperature_sensor = DHT22(pinlist[int(dht22_gpio_pin)])
    humidity_sensor = Humidity(humidity_temperature_sensor)
    temperature_sensor = Temperature(humidity_temperature_sensor)

    shelf_floor = shelf_dict["shelf_floor"]
    shelf = ShelfAsset(shelf_floor, humidity_sensor, temperature_sensor)
    shelf.set_sensor_read_interval(10)

    thread_shelf = Thread(target=shelf.read_sensor_data)

    shelf_list.append((shelf, thread_shelf))
    return shelf_list


def init_pots_threads():
    """
    Initializes the threads that will read the data from the pots' sensors.
    The moisture sensors are read using the MCP3008 Analog to Digital Converter.
    :return: list of tuples (pot, thread)
    """
    # List of tuples (pot, thread) that will be started
    pot_list = []

    pots = conf.items("pots")
    for _, pot_parameters in pots:
        pot_dict = json.loads(pot_parameters)
        shelf_floor = pot_dict["shelf_floor"]
        group_position = pot_dict["group_position"]
        pot_position = pot_dict["pot_position"]
        plant_id = pot_dict["plant_id"]
        # The moisture sensors data is converted using a shared MCP3008 Analog to Digital Converter.
        moisture_sensor = Moisture(mcp3008, pot_dict["moisture_adc_channel"])
        pot = PotAsset(
            shelf_floor, group_position, pot_position, plant_id, moisture_sensor
        )
        thread_pot = Thread(target=pot.read_sensor_data)
        pot_list.append((pot, thread_pot))

    return pot_list


def init_greenhouse_thread() -> List[Tuple[GreenhouseAsset, Thread]]:
    """
    Initializes the thread that will read the data from the greenhouse's sensors.
    It will only read the light level.
    :return: list of tuples (greenhouse, thread)
    """
    # List of tuples (greenhouse, thread) that will be started
    greenhouse_list = []

    light_level = LightLevel()
    greenhouse = GreenhouseAsset(light_level)
    thread_greenhouse = Thread(target=greenhouse.read_sensor_data)
    greenhouse_list.append((greenhouse, thread_greenhouse))
    return greenhouse_list


if __name__ == "__main__":
    """
    Script to be run on the Raspberry Pi Data Collectors.
    It starts the data collection process if no parameters are passed.
    If the --demo parameter is passed it will run the demo.
    """
    # if argv has a value check it
    if len(argv) > 1:
        # if the first parameter is --demo run the demo
        if argv[1] == "--demo":
            demo()
        else:
            print("Wrong parameter: " + argv[1])
            print("Usage: python3 -m collector [--demo]")
    else:
        main()
