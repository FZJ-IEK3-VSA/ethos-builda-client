from uuid import uuid4
import pytest
from builda_client.client import ApiClient
from builda_client.exceptions import MissingCredentialsException
from builda_client.model import (AddressInfo, BuildingStockEntry, NutsRegion, Parcel)
from shapely.geometry import Polygon

__author__ = "k.dabrock"
__copyright__ = "k.dabrock"
__license__ = "MIT"

class TestApiClientWrite:
    """Integration tests for API client writing methods.
    TODO: Add assertions
    """

    testee: ApiClient

    def test_refresh_view_succeeds(self):
        self.__given_client_authenticated()
        self.__when_refresh_view()


    def test_refresh_view_raises_missing_credentials_exception(self):
        self.__given_client_unauthenticated()
        with pytest.raises(MissingCredentialsException):
            self.__when_refresh_view()


    def test_post_building_stock_raises_missing_credentials_exception(self):
        self.__given_client_unauthenticated()
        with pytest.raises(MissingCredentialsException):
            self.__when_post_building_stock([])

        
    # TODO comment in once test db is in place
    # def test_post_nuts_succeeds(self):
    #     self.__given_client_authenticated()
    #     nuts_regions: list[NutsEntry] = self.__given_nuts_regions()
    #     self.__when_post_nuts(nuts_regions)


    def test_post_parcels_succeeds(self):
        self.__given_client_authenticated()
        parcels = self.__given_valid_parcels()
        self.__when_add_parcels(parcels)

    def test_post_addresses_succeeds(self):
        self.__given_client_unauthenticated()
        address_infos = self.__given_valid_addresses()
        self.testee.post_addresses(address_infos)



    # GIVEN
    def __given_client_authenticated(self, proxy: bool = False) -> None:
        self.testee = ApiClient(proxy=proxy, username='admin', password='admin', phase='dev')


    def __given_client_unauthenticated(self) -> None:
        self.testee = ApiClient(phase='dev')


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

    # WHEN
    def __when_refresh_view(self):
        self.testee.refresh_buildings()


    def __when_post_building_stock(self, buildings: list[BuildingStockEntry]):
        self.testee.post_building_stock(buildings)


    def __when_post_nuts(self, nuts: list[NutsRegion]):
        self.testee.post_nuts(nuts)


    def __when_add_parcels(self, parcels: list[Parcel]):
        self.testee.add_parcels(parcels)

    # THEN
