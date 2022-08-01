from dataclasses import dataclass
import dataclasses
import json
from typing import Dict
from shapely.geometry import Polygon, MultiPolygon

@dataclass
class Building:
    id: str
    area: float
    height: float
    type: str
    household_count: int
    heating_commodity: str
    cooling_commodity: str
    water_heating_commodity: str
    cooking_commodity: str

@dataclass
class NutsRegion:
    code: str
    name: str
    level: int
    parent: str | None
    geometry: MultiPolygon

@dataclass
class BuildingStockEntry:
    footprint: str
    centroid: str
    nuts3: str
    nuts2: str
    nuts1: str
    nuts0: str
    building_id: str | None = None

@dataclass
class Info:
    building_id: str
    source: str

@dataclass
class TypeInfo(Info):
    value: str

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
class HeatDemandInfo(Info):
    value: float

@dataclass
class BuildingStatistics:
    nuts_code: str
    building_count_total: int
    building_count_residential: int
    building_count_non_residential: int
    building_count_irrelevant: int
    building_count_undefined: int

@dataclass
class CommodityCount:
    heating_commodity_count: int
    cooling_commodity_count: int
    water_heating_commodity_count: int
    cooking_commodity_count: int

@dataclass
class BuildingCommodityStatistics:
    nuts_code: str
    commodity_name: str
    building_count: CommodityCount

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
            if isinstance(o, Polygon) or isinstance(o, MultiPolygon):
                return str(o)
            if dataclasses.is_dataclass(o):
                return dataclasses.asdict(o)
            return super().default(o)