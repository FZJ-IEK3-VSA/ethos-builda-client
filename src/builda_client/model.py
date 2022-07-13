from dataclasses import dataclass
import dataclasses
import json
from typing import Dict

@dataclass
class Building:
    id: str
    area: float
    height: float
    residential: bool
    household_count: int
    heating_commodity: str
    cooling_commodity: str
    water_heating_commodity: str
    cooking_commodity: str

@dataclass
class NutsEntry:
    id: int
    nuts_code: str
    nuts_name: str
    level: int
    parent: int | None
    geometry: str

@dataclass
class BuildingStockEntry:
    footprint: str
    centroid: str
    nuts3: str
    nuts2: str
    nuts1: str
    nuts0: str

@dataclass
class Info:
    building_id: str
    source: str

@dataclass
class ResidentialInfo(Info):
    value: bool

@dataclass
class HouseholdInfo(Info):
    value: int

@dataclass
class HeatingCommodityInfo(Info):
    value: str

@dataclass
class CoolingCommodityInfo(Info):
    value: str

@dataclass
class WaterHeatingCommodityInfo(Info):
    value: str

@dataclass
class CookingCommodityInfo(Info):
    value: str

@dataclass
class EnergyConsumption(Info):
    commodity: str
    value: str

@dataclass
class SectorEnergyConsumptionStatistics:
    energy_consumption: float
    commodities: Dict[str, float]

@dataclass
class EnergyConsumptionStatistics:
    nuts_code: str
    energy_consumption: float
    residential: SectorEnergyConsumptionStatistics

class EnhancedJSONEncoder(json.JSONEncoder):
        def default(self, o):
            if dataclasses.is_dataclass(o):
                return dataclasses.asdict(o)
            return super().default(o)