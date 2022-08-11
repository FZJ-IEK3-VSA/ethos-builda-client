from uuid import UUID, uuid4
import pytest
from builda_client.client import ApiClient
from builda_client.exceptions import MissingCredentialsException
from builda_client.model import (Building, BuildingCommodityStatistics, BuildingStockEntry,
                                 EnergyConsumptionStatistics, NutsRegion, BuildingStatistics, Parcel)
from shapely.geometry import MultiPolygon, Polygon

__author__ = "k.dabrock"
__copyright__ = "k.dabrock"
__license__ = "MIT"

class TestApiClient:
    """Integration tests for API client
    """

    testee: ApiClient

    def test_refresh_view_succeeds(self):
        self.__given_client_authenticated()
        self.__when_refresh_view()

    def test_refresh_view_raises_missing_credentials_exception(self):
        self.__given_client_unauthenticated()
        with pytest.raises(MissingCredentialsException):
            self.__when_refresh_view()

    def test_get_building_statistics_succeeds(self):
        self.__given_client_unauthenticated()
        building_statistic = self.__when_get_building_statistics(nuts_code='DE')
        self.__then_building_statistics_germany_returned(building_statistic, 1)

    def test_get_energy_statistics_succeeds(self):
        self.__given_client_unauthenticated()
        consumption_statistic = self.__when_get_energy_statistics(nuts_code='DE')
        self.__then_energy_statistics_germany_returned(consumption_statistic, 1)

    def test_get_building_commodity_statistics_succeeds(self):
        self.__given_client_unauthenticated()
        commodity_statistic = self.__when_get_building_commodity_statistics(nuts_code='DE')
        self.__then_building_commodity_statistics_germany_returned(commodity_statistic)

    def test_get_building_commodity_statistics_commodity_filter_succeeds(self):
        self.__given_client_unauthenticated()
        commodity_statistic = self.__when_get_building_commodity_statistics(nuts_code='DE', commodity='EL')
        self.__then_building_commodity_statistics_germany_returned(commodity_statistic)

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
        buildings = self.__when_get_buildings(type='residential')
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

    def test_get_building_stock_geom_nuts_succeeds_empty(self):
        self.__given_client_authenticated()
        polygon = self.__given_polygon()
        result = self.__when_get_building_stock(polygon, 'DE')
        self.__then_empty_list_returned(result)

    def test_post_building_stock_raises_missing_credentials_exception(self):
        self.__given_client_unauthenticated()
        with pytest.raises(MissingCredentialsException):
            self.__when_post_building_stock([])

    # TODO comment in once test db is in place
    # def test_post_nuts_succeeds(self):
    #     self.__given_client_authenticated()
    #     nuts_regions: list[NutsEntry] = self.__given_nuts_regions()
    #     self.__when_post_nuts(nuts_regions)

    def test_get_nuts_region(self):
        self.__given_client_unauthenticated()
        result = self.__when_get_nuts_region('DE')
        self.__then_nuts_region_with_code_returned(result, 'DE')

    def test_get_parcels_succeeds(self):
        self.__given_client_authenticated()
        parcels = self.__when_get_parcels()
        self.__then_parcels_returned(parcels)

    def test_get_parcels_by_ids_succeeds(self):
        self.__given_client_authenticated()
        parcels = self.testee.get_parcels(ids=[UUID('064df3a3-aeaa-4bb2-b54c-3a7ccbd91009'), UUID('1ac02b27-949b-437d-8861-e55b252b1561')])
        parcels
        
    def test_post_parcels_succeeds(self):
        self.__given_client_authenticated()
        parcels = self.__given_valid_parcels()
        self.__when_add_parcels(parcels)

    def test_get_building_parcel_succeeds(self):
        self.__given_client_unauthenticated()
        building_parcel = self.testee.get_buildings_parcel(nuts_code='DE80N')
        building_parcel


    # GIVEN
    def __given_client_authenticated(self, proxy: bool = False) -> None:
        self.testee = ApiClient(proxy=proxy, username='admin', password='admin', phase='dev')

    def __given_client_unauthenticated(self) -> None:
        self.testee = ApiClient(phase='dev')

    def __given_polygon(self) -> Polygon:
        return Polygon()

    def __given_nuts_regions(self) -> list[NutsRegion]:
        nuts_entry_1 = NutsRegion(
            code='TEST',
            name='test',
            level=5,
            parent= None,
            geometry= MultiPolygon([(((0., 0.), (0., 0.), (0., 0.), (0., 0.)), [])])
        )

        nuts_entry_2 = NutsRegion(
            code='TEST2',
            name='test2',
            level=5,
            parent= None,
            geometry= MultiPolygon([(((0., 0.), (1., 0.), (0., 1.), (0., 0.)), [])])
        )

        return [nuts_entry_1, nuts_entry_2]

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

    # WHEN
    def __when_refresh_view(self):
        self.testee.refresh_buildings()

    def __when_get_building_statistics(self, nuts_level: int | None = None, nuts_code: str | None = None) -> list[BuildingStatistics]:
        return self.testee.get_building_statistics(nuts_level=nuts_level, nuts_code=nuts_code)

    def __when_get_energy_statistics(self, nuts_level: int | None = None, nuts_code: str | None = None) -> list[EnergyConsumptionStatistics]:
        return self.testee.get_energy_consumption_statistics(nuts_level=nuts_level, nuts_code=nuts_code)

    def __when_get_building_commodity_statistics(self, nuts_level: int | None = None, nuts_code: str | None = None, commodity: str='') -> list[BuildingCommodityStatistics]:
        return self.testee.get_building_commodity_statistics(nuts_level=nuts_level, nuts_code=nuts_code, commodity=commodity)

    def __when_get_buildings(self, nuts_code: str = '', type: str = '', heating_type: str = ''):
        return self.testee.get_buildings(nuts_code=nuts_code, type=type, heating_type=heating_type)

    def __when_get_building_stock(self, geom: Polygon, nuts_code: str = ''):
        return self.testee.get_building_stock(geom, nuts_code)

    def __when_post_building_stock(self, buildings: list[BuildingStockEntry]):
        self.testee.post_building_stock(buildings)

    def __when_post_nuts(self, nuts: list[NutsRegion]):
        self.testee.post_nuts(nuts)

    def __when_get_nuts_region(self, code: str):
        return self.testee.get_nuts_region(code)

    def __when_get_parcels(self):
        return self.testee.get_parcels()

    def __when_add_parcels(self, parcels: list[Parcel]):
        self.testee.add_parcels(parcels)

    # THEN
    def __then_building_statistics_germany_returned(self, result: list[BuildingStatistics], count: int):
        assert result
        assert len(result) == count

    def __then_energy_statistics_germany_returned(self, result: list[EnergyConsumptionStatistics], count: int):
        assert result
        assert len(result) == count
    
    def __then_building_commodity_statistics_germany_returned(self, result: list[BuildingCommodityStatistics]):
        assert result
        assert len(result) > 0

    def __then_buildings_returned(self, result: list[Building]):
        assert result
        assert len(result) > 0

    def __then_residential_buildings_returned(self, result: list[Building]):
        assert result
        assert len(result) > 0
        for b in result:
            assert b.type == 'residential'

    def __then_heating_type_buildings_returned(self, result: list[Building], heating_type: str, min_length: int):
        assert len(result) >= min_length
        for b in result:
            assert b.heating_commodity == heating_type

    def __then_empty_list_returned(self, result: list):
        assert len(result) == 0

    def __then_nuts_region_with_code_returned(self, result, code):
        assert isinstance(result, NutsRegion)
        assert result.code == code

    def __then_parcels_returned(self, result: list[Parcel]):
        assert len(result) > 0