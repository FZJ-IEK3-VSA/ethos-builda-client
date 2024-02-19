import json
import logging
from typing import Dict, Tuple

import numpy as np
import requests

from builda_client.exceptions import (GeocodeException, ServerException,
                                      UnauthorizedException)
from builda_client.util import load_config


class NominatimClient:
    def __init__(self, proxy: bool = False):
        """Constructor.

        Args:
            proxy (bool, optional): Whether to use a proxy or not. Proxy should be used 
                when using client on cluster compute nodes. Defaults to False.
        """
        logging.basicConfig(level=logging.WARN)

        self.config = load_config()
        if proxy:
            host = self.config["proxy"]["host"]
            port = self.config["proxy"]["port"]
        else:
            host = self.config["nominatim"]["host"]
            port = self.config["nominatim"]["port"]

        self.address = f"""http://{host}:{port}"""

    def get_address_from_location(
        self, lat: float, lon: float
    ) -> Tuple[str, str, str, str]:
        logging.debug(f"NominatimClient: get_address_from_location")
        lat_str = np.format_float_positional(lat, trim='-')
        lon_str = np.format_float_positional(lon, trim='-')

        url: str = f"""{self.address}/reverse/?lat={lat_str}&lon={lon_str}&zoom=18&format=geocodejson"""
        try:
            response: requests.Response = requests.get(url)
            response.raise_for_status()
        except requests.HTTPError as e:
            if e.response.status_code == 403:
                raise UnauthorizedException(
                    "You are not authorized to perform this operation."
                )
            else:
                raise ServerException("An unexpected error occured.")

        response_content: Dict = json.loads(response.content)
        if "error" in response_content or not "features" in response_content:
            raise GeocodeException

        address_info = response_content["features"][0]["properties"]["geocoding"]

        house_number: str = (
            address_info["housenumber"] if "housenumber" in address_info else ""
        )
        street: str = address_info["street"] if "street" in address_info else ""
        postcode: str = address_info["postcode"] if "postcode" in address_info else ""
        city: str = address_info["city"] if "city" in address_info else ""

        return (street, house_number, postcode, city)
