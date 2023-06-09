from dataclasses import dataclass
from datetime import datetime

from influxdb_client import Point

from collector.assets.asset import Asset
from collector.assets.measurement_type import MeasurementType
from collector.sensors.water_level import WaterLevel


@dataclass
class PumpAsset(Asset):
    """
    Class representing the Pump asset.

    Attributes:
        shelf_floor (int): floor of the shelf in which the pump is, can be 1 or 2
        group_position (str): position of the pot group watered by the pump, can be 'left' or 'right'
        water_level_sensor (WaterLevel): water level of the pump at a specific time
        surface_area (float): surface area of the container of the pump
        water_level (float): water level of the pump at a specific time
    """
    shelf_floor: int
    group_position: str
    water_level_sensor: WaterLevel
    surface_area: float  # FIXME: if we want to calculate the water pumped we need this

    def __post_init__(self):
        if self.shelf_floor != 1 and self.shelf_floor != 2:
            raise ValueError("shelf_floor must be 1 or 2")

        if self.group_position != "left" and self.group_position != "right":
            raise ValueError("group_position must be 'left' or 'right'")

    def calculate_pumped_water(self) -> float:
        """Calculate the water pumped by the pump in liters since the last measurement."""

        old_water_level = self.water_level
        new_water_level = self.water_level = self.water_level_sensor.read()

        return (old_water_level - new_water_level) * self.surface_area

    def to_point(self) -> Point:
        return (
            Point(MeasurementType.PUMP.get_measurement_name())
            .tag("shelf_floor", self.shelf_floor)
            .tag("group_position", self.group_position)
            .field("pumped_water", self.calculate_pumped_water())
            .time(datetime.now())
        )
