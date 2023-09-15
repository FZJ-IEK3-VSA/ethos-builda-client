import dataclasses
import json
from abc import ABC
from dataclasses import dataclass
from typing import Optional
from uuid import UUID

from shapely.geometry import MultiPolygon, Point, Polygon

@dataclass
class Metadata:
    key: str
    name: Optional[str]
    provider: Optional[str]
    download_url: Optional[str]
    referring_website: Optional[str]
    download_timestamp: Optional[str]
    extent: Optional[str]
    license: Optional[str]
    citation: Optional[str]

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
class Coordinates:
    latitude: float
    longitude: float
    
@dataclass
class MetadataResponseDto:
    name: str
    provider: str
    referring_website: str
    license: str
    citation: str

@dataclass
class DataSource:
    attribute: str
    source: MetadataResponseDto
    lineage: str

@dataclass
class RoofGeometry:
    centroid: Coordinates
    orientation: str
    tilt: float
    area: float

@dataclass
class Building:
    id: str
    coordinates: Coordinates
    address: Address
    footprint_area_m2: float
    height_m: float
    elevation_m: float
    roof_shape: str
    type: str
    pv_potential: str
    additional: str


@dataclass
class ResidentialBuilding(Building):
    size_class: str
    refurbishment_state: int
    construction_year: int
    tabula_type: str
    useful_area_m2: float
    conditioned_living_area_m2: float
    net_floor_area_m2: float
    housing_unit_count: int
    households: str
    energy_system: str
    heat_demand_mwh: float
    norm_heating_load_kw: float

@dataclass
class NonResidentialBuilding(Building):
    use: str
    electricity_consumption_mwh: float


@dataclass
class BuildingBase:
    id: str
    footprint: MultiPolygon
    centroid: Point
    type: str

@dataclass
class BuildingGeometry:
    id: str
    footprint: MultiPolygon
    centroid: Point
    height_m: float
    roof_shape: str
    roof_geometry: RoofGeometry
    type: str
    nuts3: str
    nuts2: str
    nuts1: str
    nuts0: str
    lau: str


@dataclass
class BuildingParcel:
    id: str
    footprint: MultiPolygon
    centroid: Point
    type: str
    parcel: Optional[ParcelMinimalDto]


@dataclass
class BuildingEnergyCharacteristics:
    id: str
    type: str
    heating_commodity: str
    cooling_commodity: str
    water_heating_commodity: str
    cooking_commodity: str
    heat_demand_mwh: float
    norm_heating_load_kw: float
    pv_generation_potential_kwh: float


@dataclass
class NutsRegion:
    code: str
    name: str
    level: int
    parent: Optional[str]
    geometry: MultiPolygon


@dataclass
class BuildingStockEntry:
    building_id: str
    footprint: Polygon
    centroid: Point
    footprint_area: float
    nuts3: str
    nuts2: str
    nuts1: str
    nuts0: str
    lau: str


@dataclass
class Info:
    building_id: str
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
    lineage: str
    priority: int  # TODO use metadata table reference instead


@dataclass
class UseInfo(Info):
    value: str
    value_raw: str
    lineage: str
    priority: int  # TODO use metadata table reference instead


@dataclass
class HeightInfo(Info):
    value: float
    lineage: str
    priority: int

@dataclass
class ElevationInfo(Info):
    value: float
    lineage: str
    priority: int

@dataclass
class ParcelInfo(Info):
    value: UUID


@dataclass
class OccupancyInfo(Info):
    housing_unit_count: int
    households: str
    priority: int
    lineage: str

@dataclass
class EnergySystemInfo(Info):
    value: str

@dataclass
class EnergyConsumption(Info):
    type: str
    commodity: str
    value: str
    priority: int


@dataclass
class HeatDemandInfo(Info):
    value: float


@dataclass
class NormHeatingLoadInfo(Info):
    value: float


@dataclass
class PvPotentialInfo(Info):
    value: str


@dataclass
class ConstructionYearInfo(Info):
    value: int


@dataclass
class RefurbishmentStateInfo(Info):
    value: str


@dataclass
class RoofCharacteristicsInfo(Info):
    shape: str
    geometry: str

@dataclass
class SizeClassInfo(Info):
    value: str


@dataclass
class TabulaTypeInfo(Info):
    value: str


@dataclass
class FloorAreasInfo(Info):
    useful_area_m2: float
    conditioned_living_area_m2: float
    net_floor_area_m2: float
    lineage: str
    priority: int


@dataclass
class AdditionalInfo(Info):
    attribute: str
    value: str
    lineage: str


@dataclass
class Statistics(ABC):
    nuts_code: str


@dataclass
class BuildingStatistics(Statistics):
    building_count_total: int
    building_count_residential: int
    building_count_non_residential: int
    building_count_mixed: int


@dataclass
class BuildingUseStatistics(Statistics):
    type: str
    use: str
    building_count: int


@dataclass
class SizeClassStatistics(Statistics):
    sfh_count: str
    th_count: str
    mfh_count: str
    ab_count: str


@dataclass
class ConstructionYearStatistics(Statistics):
    avg_construction_year: int


@dataclass
class FootprintAreaStatistics(Statistics):
    sum_footprint_area_total_m2: float
    avg_footprint_area_total_m2: float
    median_footprint_area_total_m2: float
    sum_footprint_area_residential_m2: float
    avg_footprint_area_residential_m2: float
    median_footprint_area_residential_m2: float
    sum_footprint_area_non_residential_m2: float
    avg_footprint_area_non_residential_m2: float
    median_footprint_area_non_residential_m2: float
    sum_footprint_area_mixed_m2: float
    avg_footprint_area_mixed_m2: float
    median_footprint_area_mixed_m2: float


@dataclass
class HeightStatistics(Statistics):
    avg_height_total_m: float
    median_height_total_m: float
    avg_height_residential_m: float
    median_height_residential_m: float
    avg_height_non_residential_m: float
    median_height_non_residential_m: float
    avg_height_mixed_m: float
    median_height_mixed_m: float


@dataclass
class RefurbishmentStateStatistics(Statistics):
    sum_1_refurbishment_state: int
    sum_2_refurbishment_state: int
    sum_3_refurbishment_state: int


@dataclass
class HeatDemandStatistics(Statistics):
    heat_demand_mwh: float

@dataclass
class HeatDemandStatisticsByBuildingCharacteristics():
    country: str
    size_class: str
    construction_year: int
    refurbishment_state: str
    heat_demand_mwh: float


@dataclass
class EnergyCommodityStatistics(Statistics):
    energy_system: str
    commodity_name: str
    commodity_count: int


@dataclass
class ResidentialEnergyConsumptionStatistics(Statistics):
    solids_consumption_mwh: float
    lpg_consumption_mwh: float
    gas_diesel_oil_consumption_mwh: float
    gas_consumption_mwh: float
    biomass_consumption_mwh: float
    geothermal_consumption_mwh: float
    derived_heat_consumption_mwh: float
    electricity_consumption_mwh: float


@dataclass
class NonResidentialEnergyConsumptionStatistics(Statistics):
    use: str
    electricity_consumption_mwh: float


@dataclass
class PvPotentialStatistics(Statistics):
    sum_pv_generation_potential_kwh: float
    avg_pv_generation_potential_residential_kwh: float
    median_pv_generation_potential_residential_kwh: float
    sum_pv_generation_potential_residential_kwh: float
    avg_pv_generation_potential_non_residential_kwh: float
    median_pv_generation_potential_non_residential_kwh: float
    sum_pv_generation_potential_non_residential_kwh: float
    avg_pv_generation_potential_mixed_kwh: float
    median_pv_generation_potential_mixed_kwh: float
    sum_pv_generation_potential_mixed_kwh: float


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, UUID):
            return o.hex
        if isinstance(o, (Polygon, MultiPolygon)):
            return str(o)
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)
