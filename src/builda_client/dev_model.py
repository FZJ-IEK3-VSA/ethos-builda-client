import dataclasses
import json
from abc import ABC
from dataclasses import dataclass
from typing import Optional
from uuid import UUID

from shapely.geometry import MultiPolygon, Point, Polygon
from builda_client.model import AddressSource, CoordinatesSource, LineageResponseDto, SourceLineageResponseDto, SourceResponseDto, StringSource, IntSource, FloatSource


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
class Lineage:
    key: str
    description: str


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


@dataclass
class ParcelMinimalDto:
    id: UUID
    shape: Polygon


@dataclass
class Coordinates:
    latitude: float
    longitude: float
    


@dataclass
class RoofGeometry:
    centroid: Coordinates
    orientation: str
    tilt: float
    area: float


@dataclass
class PvPotential:
    capacity_kW: float
    generation_kWh: float

@dataclass
class PvPotentialSource(SourceLineageResponseDto):
    value: PvPotential
    
### Buildings without sources (for internal use only)
@dataclass
class Building:
    id: str
    coordinates: Coordinates
    address: Address
    footprint_area_m2: float
    height_m: float
    elevation_m: float
    facade_area_m2: float
    roof_shape: str
    type: str
    pv_potential: PvPotential | None
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
    yearly_heat_demand_mwh: float
    norm_heating_load_kw: float

@dataclass
class BuildingWithSourceDto:
    id: str
    coordinates: CoordinatesSource
    address: AddressSource
    footprint_area_m2: float
    height_m: FloatSource
    elevation_m: FloatSource
    facade_area_m2: FloatSource
    roof_shape: StringSource
    type: StringSource
    pv_potential: PvPotentialSource
    additional: StringSource

@dataclass
class ResidentialBuildingWithSourceDto(BuildingWithSourceDto):
    size_class: StringSource
    refurbishment_state: IntSource
    construction_year: IntSource
    tabula_type: StringSource
    useful_area_m2: FloatSource
    conditioned_living_area_m2: FloatSource
    net_floor_area_m2: FloatSource
    housing_unit_count: IntSource
    households: StringSource
    energy_system: StringSource
    yearly_heat_demand_mwh: FloatSource
    norm_heating_load_kw: FloatSource

@dataclass
class NonResidentialBuildingWithSourceDto(BuildingWithSourceDto):
    use: StringSource
    electricity_consumption_mwh: FloatSource


@dataclass
class NonResidentialBuildingResponseDto:
    buildings: list[NonResidentialBuildingWithSourceDto]
    sources: list[SourceResponseDto]
    lineages: list[LineageResponseDto]

@dataclass
class ResidentialBuildingResponseDto:
    buildings: list[ResidentialBuildingWithSourceDto]
    sources: list[SourceResponseDto]
    lineages: list[LineageResponseDto]

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
    yearly_heat_demand_mwh: float
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


### Info classes (for posting to DB during development)


@dataclass
class Info:
    building_id: str
    source: str
    lineage: str


@dataclass
class BuildingStockInfo(Info):
    footprint: Polygon
    centroid: Point
    footprint_area: float
    nuts3: str
    nuts2: str
    nuts1: str
    nuts0: str
    lau: str


@dataclass
class AddressInfo(Info):
    address: str


@dataclass
class TypeInfo(Info):
    value: str
    priority: int  # TODO use metadata table reference instead


@dataclass
class UseInfo(Info):
    value: str
    value_raw: str
    priority: int  # TODO use metadata table reference instead


@dataclass
class HeightInfo(Info):
    value: float
    priority: int


@dataclass
class ElevationInfo(Info):
    value: float
    priority: int

@dataclass
class FacadeAreaInfo(Info):
    value: float

@dataclass
class ParcelInfo(Info):
    value: UUID


@dataclass
class HousingUnitCountInfo(Info):
    value: int

@dataclass
class Household:
    id: UUID
    building_id: str
    cars: str
    income: str

@dataclass
class Person:
    id: UUID
    household_id: UUID
    age: str
    gender: str
    employment: str

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


### Statistics (for public and internal use)


@dataclass
class Statistics(ABC):
    nuts_code: str


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
