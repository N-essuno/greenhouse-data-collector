# Data Collector python script

The scripts in this project are used to collect data from the sensors of a Greenhouse and send them to an InfluxDB instance.

The script should run on Raspberry Pi devices (**Data Collectors**), which are physically connected to the sensors and virtually connected to the InfluxDB server.

The sensors retrieve data related to:

- **Light** intensity of the **greenhouse** (in lux)
- **Temperature** of the **shelves** in the greenhouse (in °C)
- **Humidity** of the **shelves** in the greenhouse (in %)
- **Moisture** level of the **pots** in the greenhouse (in %)
- **NDVI** of the **plants** in the shelf (between -1 and 1)

## Dependencies

To run the code needs the following packages to be installed (used for the NoIR camera):

```bash
sudo apt install -y python3-pyqt5 python3-opengl build-essential libcap-dev libcamera-dev
```

To install the python dependencies, run the following command from the root of the project:

```bash
pip install -r requirements.txt
```

## Configuration

Modify the [config.ini.example](config.ini.example) file to set the URL of the InfluxDB server, the personal access token and the organization ID. Save the file as `config.ini` in the [collector](collector) folder.
For other optional parameters, see the [documentation](https://github.com/influxdata/influxdb-client-python).

You can set the raw voltages reading of the sensors, so they can be converted to meaningful values. The array can be of arbitrary length and will be mapped to a range of 0 to 100

## How to Run

To run the main script, run the following command from the root of the project:

```bash
python3 -m collector
```

## Demo

To execute a demo, run the following command from the root of the project:

```bash
python3 -m collector --demo
```

The demo will create a database named `demo` and will populate it with pot measurements with decreasing moisture, simulating a real life scenario which triggers the actuator to water the pot.

The measurements refer to a pot with

- shelf_floor = 1
- group_position = left
- pot_position = right
- plant_id = 1

The incoming modifications to the configuration file are made using the message broker, for which the `.env` file must be configured with the correct credentials.
