from abc import ABC
from dataclasses import dataclass


@dataclass
class Address:
    street: str
    house_number: str
    postcode: str
    city: str


@dataclass
class Coordinates:
    latitude: float
    longitude: float


@dataclass
class SourceResponseDto:
    key: str
    name: str
    provider: str
    referring_website: str
    license: str
    citation: str


@dataclass
class LineageResponseDto:
    key: str
    description: str


### Buildings with sources (for public use)
@dataclass
class SourceLineageResponseDto:
    source: str
    lineage: str


@dataclass
class AddressSource(SourceLineageResponseDto):
    value: Address


@dataclass
class FloatSource(SourceLineageResponseDto):
    value: float


@dataclass
class IntSource(SourceLineageResponseDto):
    value: int


@dataclass
class StringSource(SourceLineageResponseDto):
    value: str


@dataclass
class CoordinatesSource(SourceLineageResponseDto):
    value: Coordinates


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


@dataclass
class BuildingResponseDto:
    buildings: list[BuildingWithSourceDto]
    sources: list[SourceResponseDto]
    lineages: list[LineageResponseDto]


@dataclass
class ResidentialBuildingWithSourceDto(BuildingWithSourceDto):
    size_class: StringSource
    refurbishment_state: IntSource
    construction_year: IntSource
    tabula_type: StringSource
    useful_area_m2: FloatSource
    conditioned_living_area_m2: FloatSource
    net_floor_area_m2: FloatSource
    yearly_heat_demand_mwh: FloatSource


@dataclass
class ResidentialBuildingResponseDto:
    buildings: list[ResidentialBuildingWithSourceDto]
    sources: list[SourceResponseDto]
    lineages: list[LineageResponseDto]


@dataclass
class NonResidentialBuildingWithSourceDto(BuildingWithSourceDto):
    use: StringSource


@dataclass
class NonResidentialBuildingResponseDto:
    buildings: list[NonResidentialBuildingWithSourceDto]
    sources: list[SourceResponseDto]
    lineages: list[LineageResponseDto]


### Statistics (for public and internal use)
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
    yearly_heat_demand_mwh: float


@dataclass
class HeatDemandStatisticsByBuildingCharacteristics:
    country: str
    size_class: str
    construction_year: int
    refurbishment_state: str
    yearly_heat_demand_mwh: float
