from typing import Any
from uuid import uuid4
from shapely import wkt

import pandas as pd
import pytest
from shapely.geometry import Polygon

from builda_client.dev_client import BuildaDevClient
from builda_client.exceptions import MissingCredentialsException
from builda_client.model import (AddressInfo, Building,
                                 BuildingEnergyCharacteristics,
                                 BuildingHouseholds, BuildingParcel,
                                 BuildingStockEntry,
                                 ConstructionYearStatistics, NutsRegion,
                                 Parcel)

__author__ = "k.dabrock"
__copyright__ = "k.dabrock"
__license__ = "MIT"

class TestDevBuildaClient:
    """Integration tests for API client development methods.
    """

    testee: BuildaDevClient

    def test_get_building_ids(self):
        self.__given_client_unauthenticated()
        building_ids = self.testee.get_building_ids(nuts_code='DE5', type='residential')
        self.__then_result_list_min_length_returned(building_ids, 1)

    def test_get_building_ids_geom(self):
        self.__given_client_unauthenticated()
        geom = self.__given_valid_custom_geom()
        building_ids = self.testee.get_building_ids(geom=geom, type='residential', nuts_code='DE943')
        self.__then_result_list_min_length_returned(building_ids, 1)

    def test_get_building_energy_characteristics_succeeds(self):
        self.__given_client_unauthenticated()
        bu_energy = self.testee.get_residential_buildings_energy_characteristics(nuts_code="DE80N")
        self.__then_building_energy_characteristics_returned(bu_energy)

    def test_get_nuts_region(self):
        self.__given_client_unauthenticated()
        result = self.testee.get_nuts_region("DE")
        self.__then_nuts_region_with_code_returned(result, "DE")

    def test_get_buildings_households(self):
        self.__given_client_unauthenticated()
        buildings_households = self.testee.get_buildings_households(nuts_code="DE80N")
        self.__then_buildings_with_households_returned(buildings_households)

    def test_get_building_parcel_succeeds(self):
        self.__given_client_unauthenticated()
        building_parcel = self.testee.get_buildings_parcel(nuts_code="DE80N")
        self.__then_buildings_with_parcel_returned(building_parcel)

    def test_get_nuts_children_succeeds(self):
        self.__given_client_unauthenticated()
        nuts_regions = self.testee.get_children_nuts_codes("DE")
        self.then_result_list_correct_length_returned(nuts_regions, 16)

    def test_refresh_view_raises_missing_credentials_exception(self):
        self.__given_client_unauthenticated()
        with pytest.raises(MissingCredentialsException):
            self.testee.refresh_buildings('non_residential')

    # TODO comment in once test db is in place
    # def test_refresh_view_succeeds(self):
    #     self.__given_client_authenticated()
    #     self.testee.refresh_buildings('non_residential')

    # def test_post_building_stock_raises_missing_credentials_exception(self):
    #     self.__given_client_unauthenticated()
    #     with pytest.raises(MissingCredentialsException):
    #         self.__when_post_building_stock([])
        
    # def test_post_nuts_succeeds(self):
    #     self.__given_client_authenticated()
    #     nuts_regions: list[NutsEntry] = self.__given_nuts_regions()
    #     self.__when_post_nuts(nuts_regions)

    # def test_post_parcels_succeeds(self):
    #     self.__given_client_authenticated()
    #     parcels = self.__given_valid_parcels()
    #     self.__when_add_parcels(parcels)

    # def test_post_addresses_succeeds(self):
    #     self.__given_client_unauthenticated()
    #     address_infos = self.__given_valid_addresses()
    #     self.testee.post_addresses(address_infos)


    # GIVEN
    def __given_client_authenticated(self, proxy: bool = False) -> None:
        self.testee = BuildaDevClient(proxy=proxy, username='admin', password='admin', phase='dev')


    def __given_client_unauthenticated(self) -> None:
        self.testee = BuildaDevClient(phase='dev')


    def __given_valid_parcels(self) -> list[Parcel]:
        parcel1 = Parcel(
            id = uuid4(),
            shape = Polygon(((0., 0.), (1., 0.), (0., 1.), (0., 0.))),
            source = 'test'
        )
        parcel2 = Parcel(
            id = uuid4(),
            shape = Polygon(((0., 0.), (2., 0.), (0., 2.), (0., 0.))),
            source = 'test'
        )
        return [parcel1, parcel2]

    def __given_valid_addresses(self) -> list[AddressInfo]:
        address_info1 = AddressInfo(
            building_id = uuid4(),
            street = 'Wüllnerstraße',
            house_number = '9',
            postcode = '52062',
            city = 'Aachen',
            priority = 1,
            source = 'Test'
        )
        address_info2 = AddressInfo(
            building_id = uuid4(),
            street = 'Wüllnerstraße',
            house_number = '10',
            postcode = '52062',
            city = 'Aachen',
            priority = 1,
            source = 'Test'
        )
        return [address_info1, address_info2]

    def __given_valid_custom_geom(self) -> Polygon:
        return Polygon(
            wkt.loads(
                """POLYGON((4031408.7239999995 2684074.9562,4031408.7239999995 
                3551421.7045,4672473.542199999 3551421.7045,4672473.542199999 
                2684074.9562,4031408.7239999995 2684074.9562))"""
            )
        )
    
    # THEN
    def __then_construction_year_statistics_returned(
        self, result: list[ConstructionYearStatistics]
    ):
        result_df = pd.DataFrame(result)
        assert 0 <= result_df["avg_construction_year"].iat[0] <= 2022

    def __then_heating_type_buildings_returned(
        self, result: list[Building], heating_type: str, min_length: int
    ):
        assert len(result) >= min_length
        for b in result:
            assert b.heating_commodity == heating_type

    def __then_nuts_region_with_code_returned(self, result, code):
        assert isinstance(result, NutsRegion)
        assert result.code == code

    def __then_buildings_with_households_returned(self, result):
        assert len(result) > 0
        assert isinstance(result[0], BuildingHouseholds)

    def __then_buildings_with_parcel_returned(self, result):
        assert len(result) > 0
        assert isinstance(result[0], BuildingParcel)

    def __then_building_energy_characteristics_returned(self, result):
        assert len(result) > 0
        assert isinstance(result[0], BuildingEnergyCharacteristics)

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