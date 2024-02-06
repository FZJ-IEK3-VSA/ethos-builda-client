from typing import Any

import pandas as pd
import pytest
from shapely import wkt
from shapely.geometry import Polygon

from builda_client.client import BuildaClient
from builda_client.model import BuildingResponseDto, NonResidentialBuildingResponseDto, ResidentialBuildingResponseDto

__author__ = "k.dabrock"
__copyright__ = "k.dabrock"
__license__ = "MIT"


class TestBuildaClient:
    """Integration tests for API client for reading methods."""

    testee: BuildaClient
    OLDENBURG_LAU = '03403000'

    ### BUILDINGS ###
    def test_get_buildings(self):
        self.given_client()
        result: BuildingResponseDto = self.testee.get_buildings(
            nuts_code=self.OLDENBURG_LAU
        )
        assert isinstance(result, BuildingResponseDto)
        self.__then_result_list_min_length_returned(result.buildings, 1)

    def test_get_buildings_type_residential(self):
        self.given_client()
        result: BuildingResponseDto = self.testee.get_buildings(
            building_type="residential", nuts_code=self.OLDENBURG_LAU,
        )
        assert isinstance(result, BuildingResponseDto)
        self.__then_result_list_min_length_returned(result.buildings, 1)
        assert all(pd.DataFrame(result.buildings)["type"].apply(lambda x: x["value"] == "residential"))

    def test_get_buildings_type_non_residential(self):
        self.given_client()
        result: BuildingResponseDto = self.testee.get_buildings(
            building_type="non-residential", nuts_code=self.OLDENBURG_LAU,
        )
        assert isinstance(result, BuildingResponseDto)
        self.__then_result_list_min_length_returned(result.buildings, 1)
        assert all(pd.DataFrame(result.buildings)["type"].apply(lambda x: x["value"] == "non-residential"))

    def test_get_buildings_type_mixed(self):
        self.given_client()
        result: BuildingResponseDto = self.testee.get_buildings(
            building_type="mixed", nuts_code=self.OLDENBURG_LAU,
        )
        assert isinstance(result, BuildingResponseDto)
        self.__then_result_list_min_length_returned(result.buildings, 1)
        assert all(pd.DataFrame(result.buildings)["type"].apply(lambda x: x["value"] == "mixed"))

    def test_get_buildings_by_postcode(self):
        postcode = '26127'
        self.given_client()
        result: BuildingResponseDto = self.testee.get_buildings(
            postcode=postcode
        )
        assert isinstance(result, BuildingResponseDto)
        self.__then_result_list_min_length_returned(result.buildings, 1)
        assert all(pd.DataFrame(result.buildings)["address"].apply(lambda x: x["value"]["postcode"] == postcode))

    def test_get_buildings_by_city(self):
        city = 'Edewecht'
        self.given_client()
        result: BuildingResponseDto = self.testee.get_buildings(
            city=city
        )
        self.__then_result_list_min_length_returned(result.buildings, 1)
        assert all(pd.DataFrame(result.buildings)["address"].apply(lambda x: x["value"]["city"] == city))

    def test_get_buildings_by_street(self):
        street = 'Kuckucksweg'
        self.given_client()
        result: BuildingResponseDto = self.testee.get_buildings(
            street=street
        )
        assert isinstance(result, BuildingResponseDto)
        self.__then_result_list_min_length_returned(result.buildings, 1)
        assert all(pd.DataFrame(result.buildings)["address"].apply(lambda x: x["value"]["street"] == street))

    def test_get_buildings_by_address(self):
        street = 'Rotkehlchenweg'
        house_number = '11A'
        postcode = '26215'
        city = 'Wiefelstede'
        self.given_client()
        result: BuildingResponseDto = self.testee.get_buildings(
            street=street, housenumber=house_number, postcode=postcode, city=city
        )
        self.then_result_list_correct_length_returned(result.buildings, 1)
        assert all(pd.DataFrame(result.buildings)["address"].apply(lambda x: x["value"]["street"] == street))
        assert all(pd.DataFrame(result.buildings)["address"].apply(lambda x: x["value"]["house_number"] == house_number))
        assert all(pd.DataFrame(result.buildings)["address"].apply(lambda x: x["value"]["postcode"] == postcode))
        assert all(pd.DataFrame(result.buildings)["address"].apply(lambda x: x["value"]["city"] == city))

    def test_get_residential_buildings(self):
        self.given_client()
        result: ResidentialBuildingResponseDto = self.testee.get_residential_buildings(
            nuts_code=self.OLDENBURG_LAU, include_mixed = True
        )
        assert isinstance(result, ResidentialBuildingResponseDto)
        self.__then_result_list_min_length_returned(result.buildings, 1)
        assert all(pd.DataFrame(result.buildings)["type"].apply(lambda x: x["value"] in ['residential', "mixed"]))
        
    def test_get_residential_buildings_by_address(self):
        street = 'Rotkehlchenweg'
        house_number = '11A'
        postcode = '26215'
        city = 'Wiefelstede'
        self.given_client()
        result: ResidentialBuildingResponseDto = self.testee.get_residential_buildings(
            street=street, housenumber=house_number, postcode=postcode, city=city
        )
        assert isinstance(result, ResidentialBuildingResponseDto)
        self.then_result_list_correct_length_returned(result.buildings, 1)
        assert all(pd.DataFrame(result.buildings)["address"].apply(lambda x: x["value"]["street"] == street))
        assert all(pd.DataFrame(result.buildings)["address"].apply(lambda x: x["value"]["house_number"] == house_number))
        assert all(pd.DataFrame(result.buildings)["address"].apply(lambda x: x["value"]["postcode"] == postcode))
        assert all(pd.DataFrame(result.buildings)["address"].apply(lambda x: x["value"]["city"] == city))

    def test_get_residential_buildings_without_mixed(self):
        self.given_client()
        result: ResidentialBuildingResponseDto = self.testee.get_residential_buildings(
            nuts_code=self.OLDENBURG_LAU, include_mixed = False
        )
        assert isinstance(result, ResidentialBuildingResponseDto)
        self.__then_result_list_min_length_returned(result.buildings, 1)
        assert all(pd.DataFrame(result.buildings)["type"].apply(lambda x: x["value"] == "residential"))

    def test_get_non_residential_buildings(self):
        self.given_client()
        result: NonResidentialBuildingResponseDto = self.testee.get_non_residential_buildings(
            nuts_code=self.OLDENBURG_LAU, include_mixed = True
        )
        assert isinstance(result, NonResidentialBuildingResponseDto)
        self.__then_result_list_min_length_returned(result.buildings, 1)
        assert all(pd.DataFrame(result.buildings)["type"].apply(lambda x: x["value"] in ['non-residential', "mixed"]))

    def test_get_non_residential_buildings_by_address(self):
        street = 'Schulweg'
        house_number = '6B'
        postcode = '26215'
        city = 'Wiefelstede'
        self.given_client()
        result: NonResidentialBuildingResponseDto = self.testee.get_non_residential_buildings(
            street=street, housenumber=house_number, postcode=postcode, city=city
        )
        assert isinstance(result, NonResidentialBuildingResponseDto)
        self.then_result_list_correct_length_returned(result.buildings, 1)
        assert all(pd.DataFrame(result.buildings)["address"].apply(lambda x: x["value"]["street"] == street))
        assert all(pd.DataFrame(result.buildings)["address"].apply(lambda x: x["value"]["house_number"] == house_number))
        assert all(pd.DataFrame(result.buildings)["address"].apply(lambda x: x["value"]["postcode"] == postcode))
        assert all(pd.DataFrame(result.buildings)["address"].apply(lambda x: x["value"]["city"] == city))

    def test_get_non_residential_buildings_without_mixed(self):
        self.given_client()
        result: NonResidentialBuildingResponseDto = self.testee.get_non_residential_buildings(
            nuts_code=self.OLDENBURG_LAU, include_mixed = False
        )
        assert isinstance(result, NonResidentialBuildingResponseDto)
        self.__then_result_list_min_length_returned(result.buildings, 1)
        assert all(pd.DataFrame(result.buildings)["type"].apply(lambda x: x["value"] == "non-residential"))

    def test_get_non_residential_buildings_without_auxiliary(self):
        self.given_client()
        result: NonResidentialBuildingResponseDto = self.testee.get_non_residential_buildings(
            nuts_code=self.OLDENBURG_LAU, include_mixed = True, exclude_auxiliary=True
        )
        assert isinstance(result, NonResidentialBuildingResponseDto)

        self.__then_result_list_min_length_returned(result.buildings, 1)
        assert all(pd.DataFrame(result.buildings)["type"].apply(lambda x: x["value"] in ['non-residential', "mixed"]))
        assert all(pd.DataFrame(result.buildings)["use"].apply(lambda x: x["value"]["sector"] != "auxiliary"))
   

    ### GENERAL STATISTICS ###

    @pytest.mark.parametrize(
        "nuts_level,country,expected_count", [(0, "DE", 1), (1, "DE", 16)]
    )
    def test_get_building_type_statistics_succeeds(
        self, nuts_level, country, expected_count
    ):
        self.given_client()
        building_statistic: list = self.testee.get_building_type_statistics(
            nuts_level=nuts_level, country=country
        )
        self.then_result_list_correct_length_returned(
            building_statistic, expected_count
        )

    def test_get_building_type_statistics_custom_geom_succeeds(self):
        self.given_client()
        custom_geom = self.given_valid_custom_geom()
        building_statistic = self.testee.get_building_type_statistics(geom=custom_geom)
        self.then_result_list_correct_length_returned(building_statistic, 1)

    @pytest.mark.parametrize(
        "nuts_level,country,expected_count", [(0, "DE", 1), (1, "DE", 16)]
    )
    def test_get_residential_construction_year_statistics_succeeds(
        self, nuts_level, country, expected_count
    ):
        self.given_client()
        construction_year_statistic = self.testee.get_residential_construction_year_statistics(
            nuts_level=nuts_level, country=country
        )
        self.then_result_list_correct_length_returned(
            construction_year_statistic, expected_count
        )

    def test_get_residential_construction_year_statistics_for_lau_succeeds(self):
        self.given_client()
        construction_year_statistic = self.testee.get_residential_construction_year_statistics(
            country="DE", nuts_code=self.OLDENBURG_LAU
        )
        self.then_result_list_correct_length_returned(construction_year_statistic, 1)

    @pytest.mark.parametrize(
        "nuts_level,country,expected_count", [(0, "DE", 1), (1, "DE", 16)]
    )
    def test_get_footprint_area_statistics_succeeds(
        self, nuts_level, country, expected_count
    ):
        self.given_client()
        footprint_area_statistics = self.testee.get_footprint_area_statistics(
            nuts_level=nuts_level, country=country
        )
        self.then_result_list_correct_length_returned(
            footprint_area_statistics, expected_count
        )

    def test_get_footprint_area_statistics_custom_geom_succeeds(self):
        self.given_client()
        custom_geom = self.given_valid_custom_geom()
        footprint_area_statistics = self.testee.get_footprint_area_statistics(
            geom=custom_geom
        )
        self.then_result_list_correct_length_returned(footprint_area_statistics, 1)

    @pytest.mark.parametrize(
        "nuts_level,country,expected_count", [(0, "DE", 1), (1, "DE", 16)]
    )
    def test_get_height_statistics_succeeds(self, nuts_level, country, expected_count):
        self.given_client()
        height_statistics = self.testee.get_height_statistics(
            nuts_level=nuts_level, country=country
        )
        self.then_result_list_correct_length_returned(
            height_statistics, expected_count
        )

    def test_get_height_statistics_custom_geom_succeeds(self):
        self.given_client()
        custom_geom = self.given_valid_custom_geom()
        height_statistics = self.testee.get_height_statistics(geom=custom_geom)
        self.then_result_list_correct_length_returned(height_statistics, 1)

    @pytest.mark.parametrize(
        "nuts_level,country,expected_count", [(0, "DE", 1), (1, "DE", 16)]
    )
    def test_get_pv_potential_statistics_succeeds(self, nuts_level, country, expected_count):
        self.given_client()
        height_statistics = self.testee.get_pv_potential_statistics(
            nuts_level=nuts_level, country=country
        )
        self.then_result_list_correct_length_returned(
            height_statistics, expected_count
        )

    ### NON-RESIDENTIAL STATISTICS ###

    @pytest.mark.parametrize(
        "nuts_level,country,expected_count", [(0, "DE", 1), (1, "DE", 16)]
    )
    def test_get_non_residential_building_use_statistics_succeeds(
        self, nuts_level, country, expected_count
    ):
        self.given_client()
        building_use_statistics = (
            self.testee.get_non_residential_building_use_statistics(
                nuts_level=nuts_level, country=country
            )
        )
        self.__then_result_list_min_length_returned(
            building_use_statistics, expected_count
        )

    def test_get_non_residential_building_use_statistics_by_geom_succeeds(self):
        self.given_client()
        custom_geom = self.given_valid_custom_geom()
        building_use_statistic = (
            self.testee.get_non_residential_building_use_statistics(geom=custom_geom)
        )
        self.__then_result_list_min_length_returned(building_use_statistic, 1)

    @pytest.mark.parametrize(
        "nuts_level,country,expected_min_count",
        [
            (0, "DE", 1),
            (1, "DE", 16),
        ],
    )
    def test_get_non_residential_energy_consumption_statistics_succeeds(
        self, nuts_level, country, expected_min_count
    ):
        self.given_client()
        energy_consumption_statistics = (
            self.testee.get_non_residential_energy_consumption_statistics(
                nuts_level=nuts_level,
                country=country,
            )
        )
        self.__then_result_list_min_length_returned(
            energy_consumption_statistics, expected_min_count
        )

    def test_get_non_residential_energy_consumption_statistics_custom_geom_succeeds(
        self,
    ):
        self.given_client()
        custom_geom = self.given_valid_custom_geom()
        energy_consumption_statistics = (
            self.testee.get_non_residential_energy_consumption_statistics(
                geom=custom_geom
            )
        )
        self.__then_result_list_min_length_returned(energy_consumption_statistics, 1)

    ### RESIDENTIAL STATISTICS ###

    @pytest.mark.parametrize(
        "nuts_level,country,expected_count", [(0, "DE", 1), (1, "DE", 16)]
    )
    def test_get_residential_size_class_statistics_succeeds(
        self, nuts_level, country, expected_count
    ):
        self.given_client()
        building_class_statistic = self.testee.get_residential_size_class_statistics(
            country=country, nuts_level=nuts_level
        )
        self.then_result_list_correct_length_returned(
            building_class_statistic, expected_count
        )

    def test_get_residential_size_class_statistics_for_lau_succeeds(self):
        self.given_client()
        building_class_statistic = self.testee.get_residential_size_class_statistics(
            country="DE", nuts_code=self.OLDENBURG_LAU
        )
        self.then_result_list_correct_length_returned(building_class_statistic, 1)

    @pytest.mark.parametrize(
        "nuts_level,country,expected_count", [(0, "DE", 1), (1, "DE", 16)]
    )
    def test_get_residential_energy_commodity_statistics_succeeds(
        self, nuts_level, country, expected_count
    ):
        self.given_client()
        commodity_statistics = self.testee.get_residential_energy_commodity_statistics(
            nuts_level=nuts_level, country=country
        )
        self.__then_result_list_min_length_returned(
            commodity_statistics, expected_count
        )
        self.__then_statistics_for_correct_country_returned(
            commodity_statistics, country
        )

    @pytest.mark.parametrize(
        "nuts_level,country,expected_count", [(0, "DE", 1), (1, "DE", 16)]
    )
    def test_get_residential_heat_demand_statistics_succeeds(
        self, nuts_level, country, expected_count
    ):
        self.given_client()
        heat_demand_statistics = self.testee.get_residential_heat_demand_statistics(
            nuts_level=nuts_level, country=country
        )
        self.then_result_list_correct_length_returned(
            heat_demand_statistics, expected_count
        )

    @pytest.mark.parametrize(
        "country, construction_year, construction_year_after, construction_year_before, size_class, expected_min_count",
        [("DE", None, 1900, 2000, 'AB', 1)]
    )
    def test_get_residential_heat_demand_statistics_by_building_info_succeeds(
        self, country, construction_year, construction_year_after, construction_year_before, size_class, expected_min_count
    ):
        self.given_client()
        heat_demand_statistics = self.testee.get_residential_heat_demand_statistics_by_building_info(
            country=country,
            construction_year=construction_year,
            construction_year_after=construction_year_after, 
            construction_year_before=construction_year_before,
            size_class=size_class
        )
        self.__then_result_list_min_length_returned(
            heat_demand_statistics, expected_min_count
        )

    def test_get_refurbishment_state_statistics_succeeds(self):
        self.given_client()
        refurbishment_state_statistics = self.testee.get_refurbishment_state_statistics(
            nuts_code=self.OLDENBURG_LAU, country="DE"
        )
        self.then_result_list_correct_length_returned(refurbishment_state_statistics, 1)


    # GIVEN
    def given_client(self) -> None:
        self.testee = BuildaClient(phase='dev')

    def given_valid_custom_geom(self) -> Polygon:
        return Polygon(
            wkt.loads(
                """POLYGON((4031408.7239999995 2684074.9562,4031408.7239999995 
                3551421.7045,4672473.542199999 3551421.7045,4672473.542199999 
                2684074.9562,4031408.7239999995 2684074.9562))"""
            )
        )

    # THEN
    def then_result_list_correct_length_returned(
        self, statistics: list[Any], expected_length: int
    ):
        assert statistics
        assert len(statistics) == expected_length

    def __then_result_list_min_length_returned(
        self, statistics: list[Any], expected_min_length: int
    ):
        assert statistics
        assert len(statistics) >= expected_min_length

    def __then_statistics_for_correct_country_returned(
        self, result, expected_country_prefix
    ):
        result_df = pd.DataFrame(result)
        assert all(result_df["nuts_code"].str.startswith(expected_country_prefix))
