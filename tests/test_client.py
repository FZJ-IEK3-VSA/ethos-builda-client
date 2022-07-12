from builda_client.exceptions import MissingCredentialsException
import pytest

from builda_client.client import ApiClient
from builda_client.model import BuildingStockEntry, EnergyConsumptionStatistics, Building
from shapely.geometry import Polygon

__author__ = "k.dabrock"
__copyright__ = "k.dabrock"
__license__ = "MIT"

class TestApiClient:

    testee: ApiClient

    def test_refresh_view_succeeds(self):
        self.__given_client_authenticated()
        self.__when_refresh_view()

    def test_refresh_view_raises_missing_credentials_exception(self):
        self.__given_client_unauthenticated()
        with pytest.raises(MissingCredentialsException):
            self.__when_refresh_view()

    def test_get_energy_statistics_succeeds(self):
        self.__given_client_unauthenticated()
        consumption_statistic = self.__when_get_energy_statistics(nuts_code='DE')
        self.__then_energy_statistics_germany_returned(consumption_statistic, 1)

    def test_get_energy_statistics_raises_value_error(self):
        self.__given_client_unauthenticated()
        with pytest.raises(ValueError):
            self.__when_get_energy_statistics(nuts_code='DE', nuts_level=1)

    def test_get_buildings(self):
        self.__given_client_unauthenticated()
        buildings = self.__when_get_buildings()
        self.__then_buildings_returned(buildings)

    def test_get_buildings_residential(self):
        self.__given_client_unauthenticated()
        buildings = self.__when_get_buildings(residential=True)
        self.__then_residential_buildings_returned(buildings)

    def test_get_buildings_heating_type_solids(self):
        self.__given_client_unauthenticated()
        buildings = self.__when_get_buildings(heating_type='SLD')
        self.__then_heating_type_buildings_returned(buildings, 'SLD', 0)

    def test_get_building_stock_raises_missing_credentials_exception(self):
        self.__given_client_unauthenticated()
        polygon = self.__given_polygon()
        with pytest.raises(MissingCredentialsException):
            self.__when_get_building_stock(polygon)

    def test_get_building_stock_succeeds_empty(self):
        self.__given_client_authenticated()
        polygon = self.__given_polygon()
        result = self.__when_get_building_stock(polygon)
        self.__then_empty_list_returned(result)

    def test_post_building_stock_raises_missing_credentials_exception(self):
        self.__given_client_unauthenticated()
        with pytest.raises(MissingCredentialsException):
            self.__when_post_building_stock([])

    # GIVEN
    def __given_client_authenticated(self) -> None:
        self.testee = ApiClient(username='admin', password='admin')

    def __given_client_unauthenticated(self) -> None:
        self.testee = ApiClient()

    def __given_polygon(self) -> Polygon:
        return Polygon()

    # WHEN
    def __when_refresh_view(self):
        self.testee.refresh_buildings()

    def __when_get_energy_statistics(self, nuts_level: int | None = None, nuts_code: str | None = None) -> list[EnergyConsumptionStatistics]:
        return self.testee.get_energy_consumption_statistics(nuts_level=nuts_level, nuts_code=nuts_code)

    def __when_get_buildings(self, nuts_code: str = '', residential: bool | None = None, heating_type: str = ''):
        return self.testee.get_buildings(nuts_code=nuts_code, residential=residential, heating_type=heating_type)

    def __when_get_building_stock(self, geom: Polygon):
        return self.testee.get_building_stock(geom)

    def __when_post_building_stock(self, buildings: list[BuildingStockEntry]):
        self.testee.post_building_stock(buildings)

    # THEN
    def __then_energy_statistics_germany_returned(self, result: list[EnergyConsumptionStatistics], count: int):
        assert result
        assert len(result) == count

    def __then_buildings_returned(self, result: list[Building]):
        assert result
        assert len(result) > 0

    def __then_residential_buildings_returned(self, result: list[Building]):
        assert result
        assert len(result) > 0
        for b in result:
            assert b.residential == True

    def __then_heating_type_buildings_returned(self, result: list[Building], heating_type: str, min_length: int):
        assert result
        assert len(result) > min_length
        for b in result:
            assert b.heating_commodity == heating_type

    def __then_empty_list_returned(self, result: list):
        assert len(result) == 0