import dataclasses
import json
from dataclasses import dataclass
from typing import Dict, Optional
from uuid import UUID

from shapely.geometry import MultiPolygon, Point, Polygon
from uuid import UUID
from shapely.geometry import Polygon, MultiPolygon

@dataclass
class Address:
    street: str
    house_number: str
    postcode: str
    city: str

@dataclass
class Parcel:
    id: UUID
    shape: Polygon
    source: str

@dataclass
class ParcelMinimalDto:
    id: UUID
    shape: Polygon


@dataclass
class Building:
    id: str
    address: Address
    footprint_area: float
    height: float
    type: str
    use: str
    heat_demand: float
    pv_generation: float
    household_count: int
    heating_commodity: str
    cooling_commodity: str
    water_heating_commodity: str
    cooking_commodity: str


@dataclass
class BuildingBase:
    id: str
    footprint: MultiPolygon
    centroid: Point
    type: str


@dataclass
class BuildingHouseholds:
    id: UUID
    household_count: int


@dataclass
class BuildingParcel:
    id: UUID
    footprint: MultiPolygon
    centroid: Point
    type: str
    parcel: Optional[ParcelMinimalDto]


@dataclass
class BuildingEnergyCharacteristics:
    id: UUID
    type: str
    heating_commodity: str
    cooling_commodity: str
    water_heating_commodity: str
    cooking_commodity: str
    heat_demand: float
    pv_generation: float

@dataclass
class NutsRegion:
    code: str
    name: str
    level: int
    parent: Optional[str]
    geometry: MultiPolygon

@dataclass
class BuildingStockEntry:
    footprint: Polygon
    centroid: Point
    footprint_area: float
    nuts3: str
    nuts2: str
    nuts1: str
    nuts0: str
    lau: str
    building_id: Optional[UUID] = None

@dataclass
class Info:
    building_id: UUID
    source: str

@dataclass
class AddressInfo(Info):
    street: str
    house_number: str
    postcode: str
    city: str
    priority: int

@dataclass
class TypeInfo(Info):
    value: str
    priority: int # TODO use metadata table reference instead

@dataclass
class UseInfo(Info):
    value: str
    priority: int # TODO use metadata table reference instead

@dataclass
class HeightInfo(Info):
    value: float

@dataclass
class ParcelInfo(Info):
    value: UUID

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
    priority: int

@dataclass
class HeatDemandInfo(Info):
    value: float


@dataclass
class PvGenerationInfo(Info):
    value: float
    

@dataclass
class BuildingStatistics:
    nuts_code: str
    building_count_total: int
    building_count_residential: int
    building_count_non_residential: int
    building_count_mixed: int
    building_count_undefined: int

@dataclass
class BuildingUseStatistics:
    nuts_code: str
    type: str
    use: str
    building_count: int

@dataclass
class FootprintAreaStatistics:
    nuts_code: str
    sum_footprint_area_total: float
    avg_footprint_area_total: float
    sum_footprint_area_residential: float
    avg_footprint_area_residential: float
    sum_footprint_area_non_residential: float
    avg_footprint_area_non_residential: float
    sum_footprint_area_irrelevant: float
    avg_footprint_area_irrelevant: float
    sum_footprint_area_undefined: float
    avg_footprint_area_undefined: float

@dataclass
class HeatDemandStatistics:
    nuts_code: str
    heat_demand: float

@dataclass
class CommodityCount:
    heating_commodity_count: int
    cooling_commodity_count: int
    water_heating_commodity_count: int
    cooking_commodity_count: int

@dataclass
class EnergyCommodityStatistics:
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
        if isinstance(o, UUID):
            return o.hex
        if isinstance(o, Polygon) or isinstance(o, MultiPolygon):
            return str(o)
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)