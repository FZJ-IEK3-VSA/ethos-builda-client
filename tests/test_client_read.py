from builda_client.client import ApiClient
from builda_client.model import (Building, BuildingEnergyCharacteristics, BuildingParcel, NutsRegion, BuildingStatistics)

__author__ = "k.dabrock"
__copyright__ = "k.dabrock"
__license__ = "MIT"

class TestApiClientRead:
    """Integration tests for API client for reading methods.
    """

    testee: ApiClient

    def test_get_building_statistics_succeeds(self):
        self.__given_client_unauthenticated()
        building_statistic = self.__when_get_building_statistics(nuts_code='DE')
        self.__then_building_statistics_returned(building_statistic, 1)


    def test_get_buildings_residential(self):
        self.__given_client_unauthenticated()
        buildings = self.__when_get_buildings(type='residential', nuts_code = '12064340')
        self.__then_residential_buildings_returned(buildings)


    def test_get_buildings_heating_type_solids(self):
        self.__given_client_unauthenticated()
        buildings = self.__when_get_buildings(heating_type='SLD')
        self.__then_heating_type_buildings_returned(buildings, 'SLD', 0)


    def test_get_building_energy_statistics_succeeds(self):
        self.__given_client_unauthenticated()
        bu_energy = self.testee.get_building_energy_characteristics(nuts_code='DE80N')
        self.__then_building_energy_characteristics_returned(bu_energy)
        

    def test_get_nuts_region(self):
        self.__given_client_unauthenticated()
        result = self.__when_get_nuts_region('DE')
        self.__then_nuts_region_with_code_returned(result, 'DE')


    def test_get_building_parcel_succeeds(self):
        self.__given_client_unauthenticated()
        building_parcel = self.testee.get_buildings_parcel(nuts_code='DE80N')
        self.__then_buildings_with_parcel_returned(building_parcel)


    def test_get_nuts_children_succeeds(self):
        self.__given_client_unauthenticated()
        nuts_regions = self.testee.get_children_nuts_codes('DE')
        self.__then_correct_number_returned(nuts_regions, 16)


    # GIVEN
    def __given_client_unauthenticated(self) -> None:
        self.testee = ApiClient()


    # WHEN
    def __when_get_building_statistics(self, nuts_level: int | None = None, nuts_code: str | None = None) -> list[BuildingStatistics]:
        return self.testee.get_building_statistics(nuts_level=nuts_level, nuts_code=nuts_code)

    def __when_get_buildings(self, nuts_code: str = '', type: str = '', heating_type: str = ''):
        return self.testee.get_buildings(nuts_code=nuts_code, type=type, heating_type=heating_type, page_size=100000)

    def __when_get_nuts_region(self, code: str):
        return self.testee.get_nuts_region(code)


    # THEN
    def __then_building_statistics_returned(self, result: list[BuildingStatistics], count: int):
        assert result
        assert len(result) == count


    def __then_residential_buildings_returned(self, result: list[Building]):
        assert result
        assert len(result) > 0
        for b in result:
            assert b.type == 'residential'


    def __then_heating_type_buildings_returned(self, result: list[Building], heating_type: str, min_length: int):
        assert len(result) >= min_length
        for b in result:
            assert b.heating_commodity == heating_type

    
    def __then_correct_number_returned(self, result: list, expected_count: int):
        assert len(result) == expected_count


    def __then_nuts_region_with_code_returned(self, result, code):
        assert isinstance(result, NutsRegion)
        assert result.code == code


    def __then_buildings_with_parcel_returned(self, result):
        assert len(result) > 0
        assert isinstance(result[0], BuildingParcel)


    def __then_building_energy_characteristics_returned(self, result):
        assert len(result) > 0
        assert isinstance(result[0], BuildingEnergyCharacteristics)