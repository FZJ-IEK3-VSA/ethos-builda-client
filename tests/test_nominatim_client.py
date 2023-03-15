from typing import Tuple
from builda_client.nominatim_client import NominatimClient
from builda_client.nominatim_client import GeocodeException
import pytest

__author__ = "k.dabrock"
__copyright__ = "k.dabrock"
__license__ = "MIT"

class TestNominatimClient:

    testee: NominatimClient

    @pytest.fixture(autouse=True)
    def initialize_testee(self):
        self.testee = NominatimClient()

    @pytest.mark.parametrize("city_test_case", ['Aachen', 'Arpsdorf'])
    def test_get_address_succeeds(self, city_test_case):
        lat, lon = self.__given_position(city_test_case)
        street, house_number, postcode, city = self.__when_get_address(lat, lon)
        self.__then_correct_address_returned(city_test_case, street, house_number, postcode, city)

    def test_get_address_raises_exception(self):
        lat = 47.4945827961842
        lon = 9.987236950002425
        with pytest.raises(GeocodeException):
            self.__when_get_address(lat, lon)

    def test_get_address_succeeds_with_scientific_notation(self):
        lat = 38.82547070990314
        lon = 2.911528113837302e-05
        address = self.__when_get_address(lat, lon)
        address

    def __given_position(self, city) -> Tuple[float, float]:
        if city == 'Aachen':
            return (50.767327101912475, 6.081489764857749)
        elif city == 'Arpsdorf':
            return (54.039227906492656, 9.855937971902875)

        else:
            raise Exception('City not allowed')

    def __when_get_address(self, lat: float, lon: float) -> Tuple[str, str, str, str]:
        return self.testee.get_address_from_location(lat, lon)

    def __then_correct_address_returned(self, city_test_case: str, street: str, house_number: str, postcode: str, city: str):
        if city_test_case == 'Aachen':
            assert street == 'Südstraße'
            assert house_number == '29'
            assert postcode == '52064'
            assert city == 'Aachen'
        elif city_test_case == 'Arpsdorf':
            assert street == 'Dorfstraße'
            assert house_number == '5'
            assert postcode == '24634'
            assert city == 'Arpsdorf'


