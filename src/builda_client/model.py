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
    refering_website_link: Optional[str]
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
    refering_website_link: str
    license: str
    citation: str

@dataclass
class DataSource:
    attribute: str
    source: MetadataResponseDto
    lineage: str


@dataclass
class Building:
    id: str
    coordinates: Coordinates
    address: Address
    footprint_area_m2: float
    height_m: float
    construction_year: int
    type: str
    use: str
    pv_generation_potential_kwh: float


@dataclass
class ResidentialBuilding(Building):
    size_class: str
    refurbishment_state: int
    tabula_type: str
    household_count: int
    heating_commodity: str
    cooling_commodity: str
    water_heating_commodity: str
    cooking_commodity: str
    heat_demand_mwh: float
    solids_consumption_mwh: float
    lpg_consumption_mwh: float
    gas_diesel_oil_consumption_mwh: float
    gas_consumption_mwh: float
    biomass_consumption_mwh: float
    geothermal_consumption_mwh: float
    derived_heat_consumption_mwh: float
    electricity_consumption_mwh: float


@dataclass
class NonResidentialBuilding(Building):
    electricity_consumption_mwh: float


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
    heat_demand_mwh: float
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
class RoofStockEntry:
    roof_area: float
    roof_height: float
    roof_orientation: float
    roof_tilt: float
    roof_type: str
    building_id: Optional[UUID] = None
    roof_id: Optional[UUID] = None


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
    priority: int  # TODO use metadata table reference instead


@dataclass
class UseInfo(Info):
    value: str
    priority: int  # TODO use metadata table reference instead


@dataclass
class HeightInfo(Info):
    value: float
    lineage: str
    priority: int

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
    type: str
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
class ConstructionYearInfo(Info):
    value: int


@dataclass
class RefurbishmentStateInfo(Info):
    value: str


@dataclass
class RoofHeightInfo(Info):
    roof_id: str
    value: float


@dataclass
class RoofTiltInfo(Info):
    roof_id: str
    value: float


@dataclass
class RoofAreaInfo(Info):
    roof_id: str
    value: float


@dataclass
class RoofOrientationInfo(Info):
    roof_id: str
    value: float


@dataclass
class RoofTypeInfo(Info):
    roof_id: str
    value: str


@dataclass
class BuildingClassInfo(Info):
    value: str


@dataclass
class TabulaTypeInfo(Info):
    value: str


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
class BuildingClassStatistics(Statistics):
    sfh_count: str
    th_count: str
    mfh_count: str
    ab_count: str


@dataclass
class ConstructionYearStatistics(Statistics):
    avg_construction_year: int
    avg_construction_year_residential: int
    avg_construction_year_non_residential: int
    avg_construction_year_mixed: int


@dataclass
class FootprintAreaStatistics(Statistics):
    sum_footprint_area_total_m2: float
    avg_footprint_area_total_m2: float
    median_footprint_area_total_m2: float
    avg_footprint_area_total_irrelevant_m2: float
    sum_footprint_area_total_irrelevant_m2: float
    median_footprint_area_total_irrelevant_m2: float
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
class CommodityCount:
    heating_commodity_count: int
    cooling_commodity_count: int
    water_heating_commodity_count: int
    cooking_commodity_count: int


@dataclass
class EnergyCommodityStatistics(Statistics):
    commodity_name: str
    building_count: CommodityCount


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
class PvGenerationPotentialStatistics(Statistics):
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
