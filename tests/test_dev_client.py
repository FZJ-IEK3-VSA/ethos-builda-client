import os
from typing import Any

import pandas as pd
import pytest

from builda_client.dev_client import BuildaDevClient, Phase
from builda_client.dev_model import (BuildingParcel, NutsRegion)
from dotenv import load_dotenv

load_dotenv()

__author__ = "k.dabrock"
__copyright__ = "k.dabrock"
__license__ = "MIT"

class TestDevBuildaClient:
    """Integration tests for API client development methods.
    """

    testee: BuildaDevClient

    def test_get_building_ids(self):
        self.__given_client_authenticated()
        building_ids = self.testee.get_building_ids(nuts_code='01058007', type='residential')
        self.__then_result_list_min_length_returned(building_ids, 1)

    def test_get_buildings_type_residential(self):
        self.__given_client_authenticated()
        buildings = self.testee.get_buildings(
            building_type="residential", nuts_code='01058007'
        )
        self.__then_result_list_min_length_returned(buildings, 1)
        assert all(pd.DataFrame(buildings)["type"] == 'residential')

    def test_get_buildings_type_non_residential(self):
        self.__given_client_authenticated()
        buildings = self.testee.get_buildings(
            building_type="non-residential", nuts_code='01058007'
        )
        self.__then_result_list_min_length_returned(buildings, 1)
        assert all(pd.DataFrame(buildings)["type"] == 'non-residential')

    def test_get_buildings_type_mixed(self):
        self.__given_client_authenticated()
        buildings = self.testee.get_buildings(
            building_type="mixed", nuts_code='DE943'
        )
        self.__then_result_list_min_length_returned(buildings, 1)
        assert all(pd.DataFrame(buildings)["type"] == 'mixed')

    def test_get_residential_buildings(self):
        self.__given_client_authenticated()
        buildings = self.testee.get_residential_buildings(nuts_code='01058007')
        self.__then_result_list_min_length_returned(buildings, 1)

    
    def test_get_residential_buildings_with_sources(self):
        self.__given_client_authenticated()
        buildings = self.testee.get_residential_buildings_with_sources(nuts_code='01058007')
        self.__then_result_list_min_length_returned(buildings.buildings, 1)

    def test_get_non_residential_buildings_exclude_auxiliary(self):
        self.__given_client_authenticated()
        non_residential_buildings = self.testee.get_non_residential_buildings(exclude_auxiliary=True, nuts_code='01058007')
        assert (pd.json_normalize(pd.DataFrame(non_residential_buildings)['use'])['sector'] == 'auxiliary').sum() == 0

    def test_get_buildings_geometry_with_no_type(self):
        self.__given_client_authenticated()
        buildings = self.testee.get_buildings_geometry(building_type=None, nuts_code='01058007')
        assert all([b.type is None for b in buildings])

    def test_get_buildings_geometry_type_mixed(self):
        self.__given_client_authenticated()
        buildings = self.testee.get_buildings_geometry(building_type="mixed", nuts_code='01058007')
        assert all([b.type == "mixed" for b in buildings])

    def test_get_buildings_geometry_all_types(self):
        self.__given_client_authenticated()
        buildings = self.testee.get_buildings_geometry(building_type="", nuts_code='01058007')
        assert all([b.type in ["residential", "non-residential", "mixed", None] for b in buildings])

    def test_get_nuts_region(self):
        self.__given_client_authenticated()
        result = self.testee.get_nuts_region("01058007")
        self.__then_nuts_region_with_code_returned(result, "01058007")

    def test_get_building_parcel_succeeds(self):
        self.__given_client_authenticated()
        building_parcel = self.testee.get_buildings_parcel(nuts_code="01058007")
        self.__then_buildings_with_parcel_returned(building_parcel)

    def test_get_nuts_children_succeeds(self):
        self.__given_client_authenticated()
        nuts_regions = self.testee.get_children_nuts_codes("DE")
        self.then_result_list_correct_length_returned(nuts_regions, 16)

    def test_get_buildings_base_no_type(self):
        self.__given_client_authenticated()
        buildings = self.testee.get_buildings_base(nuts_code='01058007')
        self.__then_result_list_min_length_returned(buildings, 1)

    # Commented out so this will not unintentionally overwrite existing data
    # def test_update_mobility_preference(self):
    #     self.__given_client_authenticated()
    #     person_count = 10000
    #     person_ids = self.testee.execute_query(f"SELECT id::varchar from data.persons limit {person_count}")
    #     person_ids = [x for xs in person_ids for x in xs]
    #     data = []
    #     for p in person_ids:
    #         data.append((p, '{"pref": "train"}'))

    #     self.testee.update_mobility_preference(data)

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
        self.__given_client_authenticated()
        energy_consumption_statistics = (
            self.testee.get_non_residential_energy_consumption_statistics(
                nuts_level=nuts_level,
                country=country,
            )
        )
        self.__then_result_list_min_length_returned(
            energy_consumption_statistics, expected_min_count
        )


    @pytest.mark.parametrize(
        "nuts_level,country,expected_count", [(0, "DE", 1), (1, "DE", 16)]
    )
    def test_get_pv_potential_statistics_succeeds(self, nuts_level, country, expected_count):
        self.__given_client_authenticated()
        height_statistics = self.testee.get_pv_potential_statistics(
            nuts_level=nuts_level, country=country
        )
        self.then_result_list_correct_length_returned(
            height_statistics, expected_count
        )

    # GIVEN
    def __given_client_authenticated(self, proxy: bool = False) -> None:
        username = os.getenv('API_USERNAME_DEV')
        password = os.getenv('API_PASSWORD_DEV')
        self.testee = BuildaDevClient(proxy=proxy, username=username, password=password, phase=Phase.DEVELOPMENT)

    
    # THEN
    def __then_nuts_region_with_code_returned(self, result, code):
        assert isinstance(result, NutsRegion)
        assert result.code == code


    def __then_buildings_with_parcel_returned(self, result):
        assert len(result) > 0
        assert isinstance(result[0], BuildingParcel)

    def then_result_list_correct_length_returned(
        self, result_list: list[Any], expected_length: int
    ):
        assert result_list
        assert len(result_list) == expected_length

    def __then_result_list_min_length_returned(
        self, result_list: list[Any], expected_min_length: int
    ):
        assert result_list
        assert len(result_list) >= expected_min_length