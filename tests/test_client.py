from typing import Any

import pandas as pd
import pytest
from shapely import wkt
from shapely.geometry import Polygon

from builda_client.client import BuildaClient

__author__ = "k.dabrock"
__copyright__ = "k.dabrock"
__license__ = "MIT"


class TestBuildaClient:
    """Integration tests for API client for reading methods."""

    testee: BuildaClient

    ### BUILDINGS ###

    def test_get_buildings(self):
        self.given_client()
        buildings = self.testee.get_buildings(
            building_type="residential", city="Arpsdorf", street="Dorfstraße"
        )
        self.__then_result_list_min_length_returned(buildings, 1)

    def test_get_residential_buildings(self):
        self.given_client()
        buildings = self.testee.get_residential_buildings(
            city="Arpsdorf", street="Dorfstraße"
        )
        self.__then_result_list_min_length_returned(buildings, 1)

    def test_get_non_residential_buildings(self):
        self.given_client()
        buildings = self.testee.get_non_residential_buildings(
            city="Arpsdorf", street="Dorfstraße"
        )
        self.__then_result_list_min_length_returned(buildings, 1)

    ### METADATA ###
    def test_get_building_sources(self):
        self.given_client()
        sources = self.testee.get_building_sources("DE1_1000010573")
        self.__then_result_list_min_length_returned(sources, 1)

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
    def test_get_construction_year_statistics_succeeds(
        self, nuts_level, country, expected_count
    ):
        self.given_client()
        construction_year_statistic = self.testee.get_construction_year_statistics(
            nuts_level=nuts_level, country=country
        )
        self.then_result_list_correct_length_returned(
            construction_year_statistic, expected_count
        )

    def test_get_construction_year_statistics_for_lau_succeeds(self):
        self.given_client()
        construction_year_statistic = self.testee.get_construction_year_statistics(
            country="DE", nuts_code="05958048"
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
    def test_get_pv_generation_potential_statistics_succeeds(self, nuts_level, country, expected_count):
        self.given_client()
        height_statistics = self.testee.get_pv_generation_potential_statistics(
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
        "nuts_level,country,use,expected_count",
        [
            (0, "DE", "1_crop_animal_production", 1),
            (1, "DE", "1_crop_animal_production", 16),
        ],
    )
    def test_get_non_residential_energy_consumption_statistics_succeeds(
        self, nuts_level, country, use, expected_count
    ):
        self.given_client()
        energy_consumption_statistics = (
            self.testee.get_non_residential_energy_consumption_statistics(
                nuts_level=nuts_level,
                country=country,
                use=use,
            )
        )
        self.then_result_list_correct_length_returned(
            energy_consumption_statistics, expected_count
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
            country="DE", nuts_code="05958048"
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
    def test_get_residential_energy_consumption_statistics_succeeds(
        self, nuts_level, country, expected_count
    ):
        self.given_client()
        energy_consumption_statistics = (
            self.testee.get_residential_energy_consumption_statistics(
                nuts_level=nuts_level,
                country=country,
            )
        )
        self.then_result_list_correct_length_returned(
            energy_consumption_statistics, expected_count
        )
        self.__then_statistics_for_correct_country_returned(
            energy_consumption_statistics, country
        )

    def test_get_residential_energy_consumption_statistics_by_geom_succeeds(self):
        self.given_client()
        custom_geom = self.given_valid_custom_geom()
        energy_consumption_statistics = (
            self.testee.get_residential_energy_consumption_statistics(geom=custom_geom)
        )
        self.then_result_list_correct_length_returned(energy_consumption_statistics, 1)

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
        "country, construction_year, construction_year_after, construction_year_before, size_class, expected_count",
        [("DE", 1860, None, None, 'AB', 1)]
    )
    def test_get_residential_heat_demand_statistics_by_building_characteristics_succeeds(
        self, country, construction_year, construction_year_after, construction_year_before, size_class, expected_count
    ):
        self.given_client()
        heat_demand_statistics = self.testee.get_residential_heat_demand_statistics_by_building_characteristics(
            country=country,
            construction_year=construction_year,
            construction_year_after=construction_year_after, 
            construction_year_before=construction_year_before,
            size_class=size_class
        )
        self.then_result_list_correct_length_returned(
            heat_demand_statistics, expected_count
        )

    def test_get_refurbishment_state_statistics_succeeds(self):
        self.given_client()
        refurbishment_state_statistics = self.testee.get_refurbishment_state_statistics(
            nuts_code="05958048", country="DE"
        )
        self.then_result_list_correct_length_returned(refurbishment_state_statistics, 1)


    # GIVEN
    def given_client(self) -> None:
        self.testee = BuildaClient()

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
