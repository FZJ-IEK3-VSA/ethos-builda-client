import json
import logging
from pathlib import Path
from typing import Dict, Optional
from uuid import UUID
import requests
import yaml
from shapely.geometry import Polygon

from builda_client.exceptions import (ClientException,
                                      MissingCredentialsException,
                                      ServerException, UnauthorizedException)
from builda_client.model import (Building, BuildingBase, BuildingParcel, BuildingCommodityStatistics, BuildingStatistics, BuildingStockEntry, CommodityCount,
                                 CookingCommodityInfo, CoolingCommodityInfo,
                                 EnergyConsumption, BuildingHouseholds,
                                 EnergyConsumptionStatistics,
                                 EnhancedJSONEncoder, HeatDemandInfo, HeatDemandStatistics, HeatingCommodityInfo,
                                 HouseholdInfo, NutsRegion, PvGenerationInfo, Parcel, ParcelInfo, ParcelMinimalDto, TypeInfo,
                                 SectorEnergyConsumptionStatistics,
                                 WaterHeatingCommodityInfo, BuildingEnergyCharacteristics, HeightInfo)
from shapely import wkt
from shapely.geometry import shape
from http.client import HTTPConnection
from uuid import UUID   
import re

   
def ewkt_loads(x):
    try:
        wkt_str = x.split(';')[1]
        return wkt.loads(wkt_str)
    except Exception:
        return None

def determine_nuts_query_param(nuts_lau_code: str) -> str:
    """Determines the correct query parameter based on the given NUTS or LAU code.

    Args:
        nuts_lau_code (str): The code for which to query.

    Raises:
        ValueError: If the code is invalid.

    Returns:
        str: The appropriate query parameter for the given code.
    """
    pattern = re.compile("^[A-Z]{2}[A-Z0-9]*$")
    if pattern.match(nuts_lau_code):
        # Probably NUTS code
        if len(nuts_lau_code) == 2:
            return 'nuts0'
        elif len(nuts_lau_code) == 3:
            return 'nuts1'
        elif len(nuts_lau_code) == 4:
            return 'nuts2'
        elif len(nuts_lau_code) == 5:
            return 'nuts3'
        else:
            raise ValueError('NUTS region code too long.')
    else:
        # Maybe LAU code
        return 'lau'

class ApiClient:

    # For read-only users of database
    BUILDING_STATISTICS_URL = 'statistics/buildings'
    HEAT_DEMAND_STATISTICS_URL = 'statistics/heat-demand'
    BUILDING_COMMODITY_STATISTICS_URL = 'statistics/building-commodities'
    ENERGY_STATISTICS_URL = 'statistics/energy-consumption'

    # For developpers/ write users of database
    AUTH_URL = '/auth/api-token'
    BUILDINGS_URL = 'buildings'
    BUILDINGS_BASE_URL = 'buildings-base/'
    BUILDINGS_HOUSEHOLDS_URL = 'buildings-households/'
    BUILDINGS_PARCEL_URL = 'buildings-parcel/'
    BUILDINGS_ENERGY_CHARACTERISTICS_URL = 'buildings-energy-characteristics/'
    BUILDINGS_ID_URL = 'buildings-id/'
    VIEW_REFRESH_URL = 'buildings/refresh'
    BUILDING_STOCK_URL = 'building-stock'
    NUTS_URL = 'nuts'
    NUTS_CODES_URL = 'nuts-codes/'
    TYPE_URL = 'type'
    HOUSEHOLD_COUNT_URL = 'household-count'
    HEATING_COMMODITY_URL = 'heating-commodity'
    COOLING_COMMODITY_URL = 'cooling-commodity'
    WARM_WATER_COMMODITY_URL = 'water-heating-commodity'
    COOKING_COMMODITY_URL = 'cooking-commodity'
    ENERGY_CONSUMPTION_URL = 'energy-consumption'
    HEAT_DEMAND_URL = 'heat-demand'
    PV_GENERATION_URL = 'pv-generation/'
    TIMING_LOG_URL = 'admin/timing-log'
    NUTS_URL = 'nuts'
    PARCEL_URL = 'parcels'
    PARCEL_INFO_URL = 'parcel-info'
    base_url: str

    def __init__(self, proxy: bool = False, username: str | None = None, password: str | None = None, phase = 'staging'):
        """Constructor.

        Args:
            proxy (bool, optional): Whether to use a proxy or not. Proxy should be used when using client on cluster compute nodes. Defaults to False.
            username (str | None, optional): Username for authentication. Only required when using client for accessing endpoints that are not open. Defaults to None.
            password (str | None, optional): Password; see username. Defaults to None.
            dev (boolean, optional): The 'phase' the client is used in, i.e. which databse to access. Possible options: 'dev', 'staging'. Defaults to 'staging'.
        """
        logging.basicConfig(level=logging.WARN)

        HTTPConnection.debuglevel = 1
        requests_log = logging.getLogger("urllib3")
        requests_log.setLevel(logging.WARN)
        requests_log.propagate = True

        self.config = self.__load_config()
        if proxy:
            host = self.config['proxy']['host']
            port = self.config['proxy']['port']
        else:
            host = self.config[phase]['api']['host']
            port = self.config[phase]['api']['port']

        self.authentication_url = f"""http://{host}:{port}{self.AUTH_URL}"""
        self.base_url = f"""http://{host}:{port}{self.config['base_url']}"""
        self.username = username
        self.password = password
        self.api_token = self.__get_authentication_token()

    def __load_config(self) -> Dict:
        """Loads the config file.

        Returns:
            dict: The configuration.
        """
        project_dir = Path(__file__).resolve().parents[0]
        config_file_path = project_dir / 'config.yml'
        with open(str(config_file_path), "r") as config_file:
            return yaml.safe_load(config_file)

    def __get_authentication_token(self) -> str:
        """Retrieves the authentication token for the given username and password from the token endpoint.

        Raises:
            ClientException: If the API returns bad request, most likely because no api token can be retrieved for given combination of username and password.
            ServerException: If an error on the server side occurrs.

        Returns:
            str: The authentication token if username and password were successfully authenticated. 
            Empty string if username and/or password are empty.
        """        
        if self.username is None or self.password is None:
            logging.info('Username and/or password not provided. Proceeding in unauthenticated mode.')
            return ''
        url: str = f"""{self.authentication_url}"""
        try:
            response: requests.Response = requests.post(url, data={'username': self.username, 'password': self.password})
            response.raise_for_status()
            return json.loads(response.content)['token']
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 400:
                raise ClientException('Could not retrieve api token. Probably the provided username and password are incorrect.')
            else:
                raise ServerException('An unexpected error occurred.')

    def __construct_authorization_header(self, json=True) -> Dict[str, str]:
        """Constructs the header for authorization including the API token.

        Returns:
            Dict[str, str]: The authorization header.
        """
        if json==True:
            return {'Authorization': f'Token {self.api_token}',
            'Content-Type': 'application/json'}
        else:
            return {'Authorization': f'Token {self.api_token}'}

    def get_buildings(self, nuts_code: str = '', type: str = '', heating_type: str = '', page_size: int = 100) -> list[Building]:
        """Gets all buildings within the specified NUTS region that fall into the provided type category
        and are of the given heating type.

        Args:
            nuts_code (str | None, optional): The NUTS-code, e.g. 'DE' for Germany according to the 2021 NUTS code definitions. Defaults to None.
            type (str): The type of building ('residential', 'non-residential', 'irrelevant')
            heating_type (str): Heating type of buildings.

        Raises:
            ServerException: When the DB is inconsistent and more than one building with same ID is returned.

        Returns:
            list[Building]: A list of buildings.
        """
        logging.debug(f"ApiClient: get_buildings(nuts_code = {nuts_code})")
        nuts_query_param: str = determine_nuts_query_param(nuts_code)
        url: str = f"""{self.base_url}{self.BUILDINGS_URL}?{nuts_query_param}={nuts_code}&type={type}&heating_commodity={heating_type}&page_size={page_size}"""

        buildings = self.__get_paginated_results_buildings(url)
        ids: list[UUID] = [b.id for b in buildings]
        if len(ids) > len(set(ids)):
            raise ServerException('Multiple buildings with the same ID have been returned.')
        return buildings


    def __get_paginated_results_buildings(self, url: str, header: Dict | None = None) -> list[Building]:
        has_next = True
        buildings: list[Building] = []
        while has_next:
            try:
                response: requests.Response = requests.get(url, headers=header)
                response.raise_for_status()
            except requests.HTTPError as e:
                if e.response.status_code == 403:
                    raise UnauthorizedException('You are not authorized to perform this operation.')
                else:
                    raise ServerException('An unexpected error occured.')
                    
            response_content: Dict = json.loads(response.content)
            results: list = response_content['results']
            for result in results:
                parcel: Optional[ParcelMinimalDto] = None
                if result['parcel']:
                    parcel =  ParcelMinimalDto(
                        id = result['parcel']['id'],
                        shape = ewkt_loads(result['parcel']['shape']),
                    )

                building = Building(
                    id = result['id'],
                    footprint = ewkt_loads(result['footprint']),
                    centroid = ewkt_loads(result['centroid']),
                    footprint_area = result['footprint_area'],
                    height = result['height'],
                    type = result['type'],
                    heat_demand = result['heat_demand'],
                    pv_generation = result['pv_generation'],
                    household_count = result['household_count'],
                    heating_commodity = result['heating_commodity'],
                    cooling_commodity = result['heating_commodity'],
                    water_heating_commodity = result['heating_commodity'],
                    cooking_commodity = result['heating_commodity'],
                    parcel = parcel
                )
                buildings.append(building)
           
            if not response_content['next']:
                has_next = False
            else:
                url = url.split('?')[0] + '?' + response_content['next'].split('?')[-1]
        
        return buildings

    def get_buildings_base(self, nuts_code: str = '', type: str = '', geom: Optional[Polygon] = None) -> list[BuildingBase]:
        """Gets buildings with reduced parameter set within the specified NUTS region that fall into the provided type category.

        Args:
            nuts_code (str | None, optional): The NUTS-code, e.g. 'DE' for Germany according to the 2021 NUTS code definitions. Defaults to None.
            type (str): The type of building ('residential', 'non-residential', 'irrelevant')

        Raises:
            ServerException: When the DB is inconsistent and more than one building with same ID is returned.

        Returns:
            gpd.GeoDataFrame: A geodataframe with all buildings.
        """
        logging.debug(f"ApiClient: get_buildings_base(nuts_code = {nuts_code}, type = {type})")
        nuts_query_param: str = determine_nuts_query_param(nuts_code)
        url: str = f"""{self.base_url}{self.BUILDINGS_BASE_URL}?{nuts_query_param}={nuts_code}&type={type}"""
        if geom:
            url += f"&geom={geom}"

        try:
            response: requests.Response = requests.get(url)
            logging.debug('ApiClient: received response. Checking for errors.')
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException('An unexpected exception occurred.')

        logging.debug(f"ApiClient: received ok response, proceeding with deserialization.")
        buildings = self.__deserialize(response.content)
        return buildings

    def get_buildings_households(self, nuts_code: str = '', heating_type: str = '') -> list[BuildingHouseholds]:
        """Gets residential buildings with household data within the specified NUTS region that fall into the provided type category.

        Args:
            nuts_code (str | None, optional): The NUTS-code, e.g. 'DE' for Germany according to the 2021 NUTS code definitions. Defaults to None.

        Raises:
            ServerException: When the DB is inconsistent and more than one building with same ID is returned.

        Returns:
            gpd.GeoDataFrame: A geodataframe with all buildings.
        """
        logging.debug(f"ApiClient: get_buildings_households(nuts_code={nuts_code}, heating_type={heating_type})")
        nuts_query_param: str = determine_nuts_query_param(nuts_code)
        url: str = f"""{self.base_url}{self.BUILDINGS_HOUSEHOLDS_URL}?{nuts_query_param}={nuts_code}&type=residential&heating_commodity={heating_type}"""

        try:
            response: requests.Response = requests.get(url)
            logging.debug('ApiClient: received response. Checking for errors.')
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException('An unexpected exception occurred.')

        logging.debug(f"ApiClient: received ok response, proceeding with deserialization.")
        results: list[str] = json.loads(response.content)
        buildings_households: list[BuildingHouseholds] = []
        for res in results:
            building_households = BuildingHouseholds(
                    id = UUID(res['id']),
                    household_count = res['household_count']
                )
            buildings_households.append(building_households)
        return buildings_households

    def get_buildings_parcel(self, nuts_code: str = '', type: str = '', geom: Optional[Polygon] = None) -> list[BuildingParcel]:
        """Gets buildings with reduced parameter set including parcel within the specified NUTS region that fall into the provided type category.

        Args:
            nuts_code (str | None, optional): The NUTS-code, e.g. 'DE' for Germany according to the 2021 NUTS code definitions. Defaults to None.
            type (str): The type of building ('residential', 'non-residential', 'irrelevant')

        Raises:
            ServerException: When the DB is inconsistent and more than one building with same ID is returned.

        Returns:
            gpd.GeoDataFrame: A geodataframe with all buildings.
        """
        logging.debug(f"ApiClient: get_buildings_parcel(nuts_code = {nuts_code}, type = {type})")
        nuts_query_param: str = determine_nuts_query_param(nuts_code)
        url: str = f"""{self.base_url}{self.BUILDINGS_PARCEL_URL}?{nuts_query_param}={nuts_code}&type={type}"""
        if geom:
            url += f"&geom={geom}"

        try:
            response: requests.Response = requests.get(url)
            logging.debug('ApiClient: received response. Checking for errors.')
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException('An unexpected exception occurred.')

        logging.debug(f"ApiClient: received ok response, proceeding with deserialization.")
        buildings = self.__deserialize_buildings_parcel(response.content)
        return buildings


    def get_building_ids(self, nuts_code: str = '', type: str = '') -> list[UUID]:
        logging.debug(f"ApiClient: get_building_ids(nuts_code = {nuts_code}, type = {type})")
        nuts_query_param: str = determine_nuts_query_param(nuts_code)
        url: str = f"""{self.base_url}{self.BUILDINGS_ID_URL}?{nuts_query_param}={nuts_code}&type={type}"""

        try:
            response: requests.Response = requests.get(url)
            logging.debug('ApiClient: received response. Checking for errors.')
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException('An unexpected exception occurred.')

        logging.debug(f"ApiClient: received ok response, proceeding with deserialization.")
        building_ids: list[UUID] = json.loads(response.content)

        return building_ids

    def __deserialize(self, response_content):
        results: list[str] = json.loads(response_content)
        buildings: list[BuildingBase] = []
        for res_json in results:
            res = json.loads(res_json)
            building = BuildingBase(
                    id = res['id'],
                    footprint = shape(res['footprint']),
                    centroid = shape(res['centroid']),
                    type = res['type'],
                )
            buildings.append(building)
        return buildings

    def __deserialize_buildings_parcel(self, response_content):
        results: list[str] = json.loads(response_content)
        buildings: list[BuildingParcel] = []
        for res_json in results:
            res = json.loads(res_json)
            parcel: ParcelMinimalDto | None = None
            if res['parcel_id'] != 'None' and res['parcel_geom'] != 'None':
                parcel = ParcelMinimalDto(
                    id = UUID(res['parcel_id']),
                    shape = shape(res['parcel_geom'])
                )
            building = BuildingParcel(
                    id = UUID(res['id']),
                    footprint = shape(res['footprint']),
                    centroid = shape(res['centroid']),
                    type = res['type'],
                    parcel = parcel
                )
            buildings.append(building)
        return buildings

    def get_parcels(self, ids: Optional[list[UUID]] = None) -> list[Parcel]:
        """
        [REQUIRES AUTHENTICATION] Gets all parcels.

        Returns:
            list[Parcel]: A list of parcels.
        """
        logging.debug(f"ApiClient: get_parcels()")
        url: str = f"""{self.base_url}{self.PARCEL_URL}"""
        if ids:
            id_str = ','.join([str(id) for id in ids])
            url += f'?ids={id_str}'

        try:
            response: requests.Response = requests.get(url, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.HTTPError as e:
            if e.response.status_code == 403:
                raise UnauthorizedException('You are not authorized to perform this operation.')
            else:
                raise ServerException('An unexpected error occured.')
                
        results: Dict = json.loads(response.content)
        parcels: list[Parcel] = []

        for result in results:
            parcel = Parcel(
                id = UUID(result['id']),
                shape = ewkt_loads(result['shape']),
                source = 'test'
            )
            parcels.append(parcel)
        return parcels


    def post_parcel_infos(self, parcel_infos: list[ParcelInfo]):
        logging.debug("ApiClient: post_parcel_infos")
        if not self.api_token:
            raise MissingCredentialsException('This endpoint is private. You need to provide username and password when initializing the client.')

        url: str = f"""{self.base_url}{self.PARCEL_INFO_URL}"""

        parcel_infos_json = json.dumps(parcel_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(url, data=parcel_infos_json, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 403:
                raise UnauthorizedException('You are not authorized to perform this operation. Perhaps wrong username and password given?')
            elif err.response.status_code >= 400 and err.response.status_code >= 499:
                raise ClientException('A client side error occured', err)
            else:
                raise ServerException('An unexpected error occurred', err)


    def add_parcels(self, parcels: list[Parcel]):
        """
        [REQUIRES AUTHENTICATION] Adds parcels.

        Args:
            parcels (list[Parcel]): A list of parcels.
        """
        if not self.api_token:
            raise MissingCredentialsException('This endpoint is private. You need to provide username and password when initializing the client.')
        url: str = f"""{self.base_url}{self.PARCEL_URL}"""

        parcels_json = json.dumps(parcels, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(url, data=parcels_json, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 403:
                raise UnauthorizedException('You are not authorized to perform this operation. Perhaps wrong username and password given?')
            elif err.response.status_code >= 400 and err.response.status_code >= 499:
                raise ClientException('A client side error occured', err)
            else:
                raise ServerException('An unexpected error occurred', err)

    def modify_building(self, building_id: UUID, building_data: Dict):
        if not self.api_token:
            raise MissingCredentialsException('This endpoint is private. You need to provide username and password when initializing the client.')
        url: str = f"""{self.base_url}{self.BUILDING_STOCK_URL}/{building_id}"""
        building_json = json.dumps(building_data)
        try:
            response: requests.Response = requests.put(url, data=building_json, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 403:
                raise UnauthorizedException('You are not authorized to perform this operation. Perhaps wrong username and password given?')
            elif err.response.status_code >= 400 and err.response.status_code >= 499:
                raise ClientException('A client side error occured', err)
            else:
                raise ServerException('An unexpected error occurred', err)

    def get_building_energy_characteristics(self, nuts_code: str = '', type: str = '', geom: Optional[Polygon] = None, heating_type: str = '') -> list[BuildingEnergyCharacteristics]:
        """Get energy related building information (commodities, heat demand, pv generation) for each building that fulfills the query parameters.

        Args:
            nuts_code (str | None, optional): The NUTS or LAU code, e.g. 'DE' for Germany according to the 2021 NUTS code definitions. Defaults to None.
            type (str): The type of building e.g. 'residential'
        Raises:
            ServerException: If an unexpected error occurrs on the server side.

        Returns:
            list[BuildingEnergyCharacteristics]: A list of building objects with energy characteristics.
        """
        logging.debug(f"ApiClient: get_building_energy_characteristics(nuts_code={nuts_code}, type={type})")
        nuts_query_param: str = determine_nuts_query_param(nuts_code)

        url: str = f"""{self.base_url}{self.BUILDINGS_ENERGY_CHARACTERISTICS_URL}?{nuts_query_param}={nuts_code}&type={type}&heating_commodity={heating_type}"""
        if geom:
            url += f"&geom={geom}"

        try:
            response: requests.Response = requests.get(url)
            logging.debug('ApiClient: received response. Checking for errors.')
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException('An unexpected exception occurred.')

        logging.debug(f"ApiClient: received ok response, proceeding with deserialization.")
        
        results: list[Dict] = json.loads(response.content)
        buildings: list[BuildingEnergyCharacteristics] = []
        for res in results:
            building = BuildingEnergyCharacteristics(
                    id = UUID(res['id']),
                    type = res['type'],
                    heating_commodity = res['heating_commodity'],
                    cooling_commodity = res['cooling_commodity'],
                    water_heating_commodity = res['water_heating_commodity'],
                    cooking_commodity = res['cooking_commodity'],
                    heat_demand = res['heat_demand'],
                    pv_generation = res['pv_generation'],
                )
            buildings.append(building)

        return buildings

    def get_building_statistics(self, nuts_level: int | None = None, nuts_code: str | None = None) -> list[BuildingStatistics]:
        """Get the building statistics for the given nuts level or nuts code. Only one of nuts_level and nuts_code may be specified.

        Args:
            nuts_level (int | None, optional): The NUTS level. Defaults to None.
            nuts_code (str | None, optional): The NUTS code, e.g. 'DE' for Germany according to the 2021 NUTS code definitions. Defaults to None.

        Raises:
            ValueError: If both nuts_level and nuts_code are specified.
            ServerException: If an unexpected error occurrs on the server side.

        Returns:
            list[BuildingStatistics]: A list of objects per NUTS region with statistical info about buildings.
        """
        if nuts_level is not None and nuts_code is not None:
            raise ValueError('Either nuts_level or nuts_code can be specified, not both.')

        query_params = ""
        if nuts_level is not None:
            query_params = f"?nuts_level={nuts_level}"
        elif nuts_code is not None:
            query_params = f"?nuts_code={nuts_code}"

        url: str = f"""{self.base_url}{self.BUILDING_STATISTICS_URL}{query_params}"""
        try:
            response: requests.Response = requests.get(url)
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException('An unexpected exception occurred.')

        results: list = json.loads(response.content)
        statistics: list[BuildingStatistics] = []
        for res in results:
            statistic = BuildingStatistics(
                nuts_code=res['nuts_code'], 
                building_count_total=res['building_count_total'], 
                building_count_residential=res['building_count_residential'], 
                building_count_non_residential=res['building_count_non_residential'],
                building_count_irrelevant=res['building_count_irrelevant'],
                building_count_undefined=res['building_count_undefined']
                )
            statistics.append(statistic)
        return statistics

    def get_heat_demand_statistics(self, country: str = '', nuts_level: Optional[int] = None, nuts_code: Optional[str] = None) -> list[HeatDemandStatistics]:
        """Get the residential heat demand statistics in MWh for the given NUTS level or NUTS/LAU code. 
        Results can be limited to a certain country by setting the country parameter.
        Only one of nuts_level and nuts_code may be specified.

        Args:
            country (str | None, optional): The NUTS-0 code for the country, e.g. 'DE' for Germany. Defaults to None.
            nuts_level (int | None, optional): The NUTS level (0=NUTS-0, 1=NUTS-1, 2=NUTS-2, 3=NUTS-3, 4=LAU). Defaults to None.
            nuts_code (str | None, optional): The NUTS or LAU code, e.g. 'DEA' for NRW in Germany according to the 2021 NUTS code and 2019 LAU code definitions. Defaults to None.

        Raises:
            ValueError: If both nuts_level and nuts_code are specified or the nuts_level is invalid.
            ServerException: If an unexpected error occurrs on the server side.

        Returns:
            list[HeatDemandStatistics]: A list of objects per NUTS/LAU region with statistical info about heat demand [MWh].
        """
        if nuts_level is not None and nuts_code is not None:
            raise ValueError('Either nuts_level or nuts_code can be specified, not both.')
        if nuts_level is not None and nuts_level not in range(0, 5):
            raise ValueError('Invalid NUTS/LAU level provided; nuts_level must be in range [0,4].')

        query_params = f"?country={country}"
        if nuts_level is not None:
            query_params += f"&nuts_level={nuts_level}"
        elif nuts_code is not None:
            query_params += f"&nuts_code={nuts_code}"

        url: str = f"""{self.base_url}{self.HEAT_DEMAND_STATISTICS_URL}{query_params}"""
        try:
            response: requests.Response = requests.get(url)
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException('An unexpected exception occurred.')

        results: list = json.loads(response.content)
        statistics: list[HeatDemandStatistics] = []
        for res in results:
            statistic = HeatDemandStatistics(
                nuts_code=res['nuts_code'], 
                heat_demand=res['heat_demand_mwh'], 
                )
            statistics.append(statistic)
        return statistics

    def get_energy_consumption_statistics(self, nuts_level: int | None = None, nuts_code: str | None = None) -> list[EnergyConsumptionStatistics]:
        """Get the energy consumption statistics for the given nuts level or nuts code. Only one of nuts_level and nuts_code may be specified.

        Args:
            nuts_level (int | None, optional): The NUTS level for which to retrieve the statistics. Defaults to None.
            nuts_code (str | None, optional): The NUTS code of the region for which to retrieve the statistics according to the 2021 NUTS code definitions. Defaults to None.

        Raises:
            ValueError: If both nuts_level and nuts_code are given.
            ServerException: If an error occurrs on the server side.

        Returns:
            list[EnergyConsumptionStatistics]: A list of energy consumption statistics. If just one nuts_code is queried, the list will only contain one element.
        """        
        logging.debug(f"ApiClient: get_energy_consumption_statistics(nuts_level={nuts_level}, nuts_code={nuts_code}")
        
        if nuts_level is not None and nuts_code is not None:
            raise ValueError('Either nuts_level or nuts_code can be specified, not both.')
        
        query_params = ""
        if nuts_level is not None:
            query_params = f"?nuts_level={nuts_level}"
        elif nuts_code is not None:
            query_params = f"?nuts_code={nuts_code}"

        url: str = f"""{self.base_url}{self.ENERGY_STATISTICS_URL}{query_params}"""
        try:
            response: requests.Response = requests.get(url)
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException('An unexpected exception occurred.')

        results: list = json.loads(response.content)
        statistics: list[EnergyConsumptionStatistics] = []
        for res in results:
            res_nuts_code: str = res['nuts_code']
            energy_consumption: float = res['energy_consumption_kWh']

            energy_consumption_residential: float = res['residential']['energy_consumption_kWh']
            commodities_residential: Dict[str, float] = res['residential']['commodities']
            residential: SectorEnergyConsumptionStatistics = SectorEnergyConsumptionStatistics(energy_consumption_residential, commodities_residential)

            statistic = EnergyConsumptionStatistics(nuts_code=res_nuts_code, energy_consumption=energy_consumption, residential=residential)
            statistics.append(statistic)
        return statistics

    def get_building_commodity_statistics(self,  nuts_level: int | None = None, nuts_code: str | None = None, commodity: str = ''):
        """Get the building commodity statistics for the given nuts level or nuts code. Only one of nuts_level and nuts_code may be specified.

        Args:
            nuts_level (int | None, optional): The NUTS level for which to retrieve the statistics. Defaults to None.
            nuts_code (str | None, optional): The NUTS code of the region for which to retrieve the statistics according to the 2021 NUTS code definitions. Defaults to None.
            commodity (str, optional): The commodity for which to get statistics

        Raises:
            ValueError: If both nuts_level and nuts_code are given.
            ServerException: If an error occurrs on the server side.

        Returns:
            list[BuildingCommodityStatistics]: A list of building commodity statistics. If just one nuts_code is queried, the list will only contain one element.
        """        
        logging.debug(f"ApiClient: get_building_commodity_statistics(nuts_level={nuts_level}, nuts_code={nuts_code}, commodity={commodity}")
        
        if nuts_level is not None and nuts_code is not None:
            raise ValueError('Either nuts_level or nuts_code can be specified, not both.')
        
        query_params = "?"
        if nuts_level is not None:
            query_params = f"?nuts_level={nuts_level}"
        elif nuts_code is not None:
            query_params = f"?nuts_code={nuts_code}"

        if commodity:
            query_params += f"&commodity={commodity}"

        url: str = f"""{self.base_url}{self.BUILDING_COMMODITY_STATISTICS_URL}{query_params}"""
        try:
            response: requests.Response = requests.get(url)
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException('An unexpected exception occurred.')

        results: list = json.loads(response.content)
        statistics: list[BuildingCommodityStatistics] = []
        for res in results:
            res_nuts_code: str = res['nuts_code']
            res_commodity: str = res['commodity']
            res_heating_commodity_count: int = int(res['commodity_count']['heating_commodity_count'])
            res_cooling_commodity_count: int = int(res['commodity_count']['cooling_commodity_count'])
            res_water_heating_commodity_count: int = int(res['commodity_count']['water_heating_commodity_count'])
            res_cooking_commodity_count: int = int(res['commodity_count']['cooking_commodity_count'])

            statistic = BuildingCommodityStatistics(
                nuts_code=res_nuts_code,
                commodity_name=res_commodity,
                building_count = CommodityCount(
                    heating_commodity_count=res_heating_commodity_count,
                    cooling_commodity_count=res_cooling_commodity_count,
                    water_heating_commodity_count=res_water_heating_commodity_count,
                    cooking_commodity_count=res_cooking_commodity_count
                )
            )
            statistics.append(statistic)

        return statistics

    def refresh_buildings(self) -> None:
        """[REQUIRES AUTHENTICATION] Refreshes the materialized view 'buildings'.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
        """        
        logging.debug("ApiClient: refresh_buildings")
        if not self.api_token:
            raise MissingCredentialsException('This endpoint is private. You need to provide username and password when initializing the client.')

        url: str = f"""{self.base_url}{self.VIEW_REFRESH_URL}"""
        try:
            response: requests.Response = requests.post(url, headers=self.__construct_authorization_header(json=False))
            response.raise_for_status()
        except requests.HTTPError as e:
            if e.response.status_code == 403:
                raise UnauthorizedException('You are not authorized to perform this operation. Perhaps wrong username and password given?')
            else:
                raise ServerException('An unexpected error occured.')
            

    def get_building_stock(self, geom: Polygon | None = None, nuts_code: str = '') -> list[BuildingStockEntry]:
        """[REQUIRES AUTHENTICATION]  Gets all entries of the building stock within the specified geometry.

        Args:
            geom (Polygon, optional): The polygon for which to retrieve buildings.
            nuts_code (str, optional): The NUTS region to get buildings from.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.

        Returns:
            list[BuildingStockEntry]: All building stock entries that lie within the given polygon.
        """
        logging.debug(f'ApiClient: get_building_stock')

        if not self.api_token:
            raise MissingCredentialsException('This endpoint is private. You need to provide username and password when initializing the client.')

        query_params: str = ''
        if geom is not None and nuts_code:
            nuts_query_param = determine_nuts_query_param(nuts_code)
            query_params = f'?geom={geom}&{nuts_query_param}={nuts_code}'
        elif geom is not None:
            query_params = f'?geom={geom}'
        elif nuts_code:
            nuts_query_param = determine_nuts_query_param(nuts_code)
            query_params = f'?{nuts_query_param}={nuts_code}'

        url: str = f"""{self.base_url}{self.BUILDING_STOCK_URL}{query_params}"""

        try:
            response: requests.Response = requests.get(url, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.HTTPError as e:
            if e.response.status_code == 403:
                raise UnauthorizedException('You are not authorized to perform this operation.')
            else:
                raise ServerException('An unexpected error occured.')

        buildings: list[BuildingStockEntry] = []
        results: Dict = json.loads(response.content)
        for result in results:
            building = BuildingStockEntry(
                building_id = result['building_id'],
                footprint = ewkt_loads(result['footprint']),
                centroid = ewkt_loads(result['centroid']),
                nuts3 = result['nuts3'],
                nuts2 = result['nuts2'],
                nuts1 = result['nuts1'],
                nuts0 = result['nuts0'],
                lau = result['lau'],
            )
            buildings.append(building)

        return buildings


    def post_building_stock(self, buildings: list[BuildingStockEntry]) -> None:
        """[REQUIRES AUTHENTICATION]  Posts the building_stock data to the database.

        Args:
            buildings (list[BuildingStockEntry]): The building stock entries to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """

        if not self.api_token:
            raise MissingCredentialsException('This endpoint is private. You need to provide username and password when initializing the client.')
        url: str = f"""{self.base_url}{self.BUILDING_STOCK_URL}"""

        buildings_json = json.dumps(buildings, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(url, data=buildings_json, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 403:
                raise UnauthorizedException('You are not authorized to perform this operation. Perhaps wrong username and password given?')
            elif err.response.status_code >= 400 and err.response.status_code >= 499:
                raise ClientException('A client side error occured', err)
            else:
                raise ServerException('An unexpected error occurred', err)


    def post_nuts(self, nuts_regions: list[NutsRegion]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the nuts data to the database. Private endpoint: requires client to have credentials.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """        
        logging.debug("ApiClient: post_nuts")

        if not self.api_token:
            raise MissingCredentialsException('This endpoint is private. You need to provide username and password when initializing the client.')

        url: str = f"""{self.base_url}{self.NUTS_URL}"""

        nuts_regions_json = json.dumps(nuts_regions, cls=EnhancedJSONEncoder)

        try:
            response: requests.Response = requests.post(url, data=nuts_regions_json, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 403:
                raise UnauthorizedException('You are not authorized to perform this operation. Perhaps wrong username and password given?')
            elif err.response.status_code >= 400 and err.response.status_code >= 499:
                raise ClientException('A client side error occured', err)
            else:
                raise ServerException('An unexpected error occurred', err)
  

    def post_type_info(self, type_infos: list[TypeInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the type info data to the database.

        Args:
            type_infos (list[TypeInfo]): The type info data to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """

        logging.debug("ApiClient: post_type_info")
        if not self.api_token:
            raise MissingCredentialsException('This endpoint is private. You need to provide username and password when initializing the client.')

        url: str = f"""{self.base_url}{self.TYPE_URL}"""

        type_infos_json = json.dumps(type_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(url, data=type_infos_json, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 403:
                raise UnauthorizedException('You are not authorized to perform this operation. Perhaps wrong username and password given?')
            elif err.response.status_code >= 400 and err.response.status_code >= 499:
                raise ClientException('A client side error occured', err)
            else:
                raise ServerException('An unexpected error occurred', err)

    def post_height_info(self, height_infos: list[HeightInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the household count data to the database.

        Args:
            household_infos (list[HeightInfo]): The household count data to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """        
        logging.debug("ApiClient: post_height_info")
        if not self.api_token:
            raise MissingCredentialsException('This endpoint is private. You need to provide username and password when initializing the client.')

        url: str = f"""{self.base_url}{self.HOUSEHOLD_COUNT_URL}"""
        height_infos_json = json.dumps(height_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(url, data=height_infos_json, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 403:
                raise UnauthorizedException('You are not authorized to perform this operation. Perhaps wrong username and password given?')
            elif err.response.status_code >= 400 and err.response.status_code >= 499:
                raise ClientException('A client side error occured', err)
            else:
                raise ServerException('An unexpected error occurred', err)


    def post_household_count(self, household_infos: list[HouseholdInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the household count data to the database.

        Args:
            household_infos (list[HouseholdInfo]): The household count data to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """        
        logging.debug("ApiClient: post_household_count")
        if not self.api_token:
            raise MissingCredentialsException('This endpoint is private. You need to provide username and password when initializing the client.')

        url: str = f"""{self.base_url}{self.HOUSEHOLD_COUNT_URL}"""
        household_infos_json = json.dumps(household_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(url, data=household_infos_json, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 403:
                raise UnauthorizedException('You are not authorized to perform this operation. Perhaps wrong username and password given?')
            elif err.response.status_code >= 400 and err.response.status_code >= 499:
                raise ClientException('A client side error occured', err)
            else:
                raise ServerException('An unexpected error occurred', err)


    def post_heating_commodity(self, heating_commodity_infos: list[HeatingCommodityInfo]) -> None:
        """[REQUIRES AUTHENTICATION]  Posts the heating commodity data to the database.

        Args:
            heating_commodity_infos (list[HeatingCommodityInfo]): The heating commodity data to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_heating_commodity")
        if not self.api_token:
            raise MissingCredentialsException('This endpoint is private. You need to provide username and password when initializing the client.')

        url: str = f"""{self.base_url}{self.HEATING_COMMODITY_URL}"""
        heating_commodity_infos_json = json.dumps(heating_commodity_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(url, data=heating_commodity_infos_json, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 403:
                raise UnauthorizedException('You are not authorized to perform this operation. Perhaps wrong username and password given?')
            elif err.response.status_code >= 400 and err.response.status_code >= 499:
                raise ClientException('A client side error occured', err)
            else:
                raise ServerException('An unexpected error occurred', err)

    def post_cooling_commodity(self, cooling_commodity_infos: list[CoolingCommodityInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the cooling commodity data to the database.

        Args:
            cooling_commodity_infos (list[CoolingCommodityInfo]): The cooling commodity data to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_cooling_commodity")
        if not self.api_token:
            raise MissingCredentialsException('This endpoint is private. You need to provide username and password when initializing the client.')

        url: str = f"""{self.base_url}{self.COOLING_COMMODITY_URL}"""
        cooling_commodity_infos_json = json.dumps(cooling_commodity_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(url, data=cooling_commodity_infos_json, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 403:
                raise UnauthorizedException('You are not authorized to perform this operation. Perhaps wrong username and password given?')
            elif err.response.status_code >= 400 and err.response.status_code >= 499:
                raise ClientException('A client side error occured', err)
            else:
                raise ServerException('An unexpected error occurred', err)
    
    def post_water_heating_commodity(self, water_heating_commodity_infos: list[WaterHeatingCommodityInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the water heating commodity data to the database.

        Args:
            water_heating_commodity_infos (list[WaterHeatingCommodityInfo]): The water heating commodity infos to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """        
        logging.debug("ApiClient: post_water_heating_commodity")
        if not self.api_token:
            raise MissingCredentialsException('This endpoint is private. You need to provide username and password when initializing the client.')

        url: str = f"""{self.base_url}{self.WARM_WATER_COMMODITY_URL}"""
        water_heating_commodity_infos_json = json.dumps(water_heating_commodity_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(url, data=water_heating_commodity_infos_json, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 403:
                raise UnauthorizedException('You are not authorized to perform this operation. Perhaps wrong username and password given?')
            elif err.response.status_code >= 400 and err.response.status_code >= 499:
                raise ClientException('A client side error occured', err)
            else:
                raise ServerException('An unexpected error occurred', err)
            
    def post_cooking_commodity(self, cooking_commodity_infos: list[CookingCommodityInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the cooking commodity data to the database.

        Args:
            cooking_commodity_infos (list[CookingCommodityInfo]): The cooking commodity infos to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_cooking_commodity")
        if not self.api_token:
            raise MissingCredentialsException('This endpoint is private. You need to provide username and password when initializing the client.')

        url: str = f"""{self.base_url}{self.COOKING_COMMODITY_URL}"""
        cooking_commodity_infos_json = json.dumps(cooking_commodity_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(url, data=cooking_commodity_infos_json, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 403:
                raise UnauthorizedException('You are not authorized to perform this operation. Perhaps wrong username and password given?')
            elif err.response.status_code >= 400 and err.response.status_code >= 499:
                raise ClientException('A client side error occured', err)
            else:
                raise ServerException('An unexpected error occurred', err)

    def post_energy_consumption(self, energy_consumption_infos: list[EnergyConsumption]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the energy consumption data to the database.

        Args:
            energy_consumption_infos (list[EnergyConsumption]): The energy consumption infos to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """        
        logging.debug("ApiClient: post_energy_consumption_commodity")
        if not self.api_token:
            raise MissingCredentialsException('This endpoint is private. You need to provide username and password when initializing the client.')

        url: str = f"""{self.base_url}{self.ENERGY_CONSUMPTION_URL}"""
        energy_consumption_infos_json = json.dumps(energy_consumption_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(url, data=energy_consumption_infos_json, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 403:
                raise UnauthorizedException('You are not authorized to perform this operation. Perhaps wrong username and password given?')
            elif err.response.status_code >= 400 and err.response.status_code >= 499:
                raise ClientException('A client side error occured', err)
            else:
                raise ServerException('An unexpected error occurred', err)

    def post_heat_demand(self, heat_demand_infos: list[HeatDemandInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the heat demand data to the database.

        Args:
            heat_demand_infos (list[HeatDemandInfo]): The heat demand infos to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """        
        logging.debug("ApiClient: post_heat_demand")
        if not self.api_token:
            raise MissingCredentialsException('This endpoint is private. You need to provide username and password when initializing the client.')

        url: str = f"""{self.base_url}{self.HEAT_DEMAND_URL}"""
        heat_demand_infos_json = json.dumps(heat_demand_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(url, data=heat_demand_infos_json, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 403:
                raise UnauthorizedException('You are not authorized to perform this operation. Perhaps wrong username and password given?')
            elif err.response.status_code >= 400 and err.response.status_code >= 499:
                raise ClientException('A client side error occured', err)
            else:
                raise ServerException('An unexpected error occurred', err)

    def post_pv_generation(self, pv_generation_infos: list[PvGenerationInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the pv generation data to the database.

        Args:
            pv_generation_infos (list[PvGenerationInfo]): The pv generation infos to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """        
        logging.debug("ApiClient: post_pv_generation")
        if not self.api_token:
            raise MissingCredentialsException('This endpoint is private. You need to provide username and password when initializing the client.')

        url: str = f"""{self.base_url}{self.PV_GENERATION_URL}"""
        pv_generation_infos_json = json.dumps(pv_generation_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(url, data=pv_generation_infos_json, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 403:
                raise UnauthorizedException('You are not authorized to perform this operation. Perhaps wrong username and password given?')
            elif err.response.status_code >= 400 and err.response.status_code >= 499:
                raise ClientException('A client side error occured', err)
            else:
                raise ServerException('An unexpected error occurred', err)


    def post_timing_log(self, function_name: str, measured_time: float):
        logging.debug("ApiClient: post_timing_log")
        if not self.api_token:
            raise MissingCredentialsException('This endpoint is private. You need to provide username and password when initializing the client.')

        url: str = f"""{self.base_url}{self.TIMING_LOG_URL}"""

        try:
            response: requests.Response = requests.post(url, data=json.dumps({'function_name': function_name, 'measured_time': measured_time}), headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 403:
                raise UnauthorizedException('You are not authorized to perform this operation. Perhaps wrong username and password given?')
            elif err.response.status_code >= 400 and err.response.status_code >= 499:
                raise ClientException('A client side error occured', err)
            else:
                raise ServerException('An unexpected error occurred', err)

    def get_nuts_region(self, nuts_code: str):
        logging.debug(f'ApiClient: get_nuts_region')
        url: str = f"""{self.base_url}{self.NUTS_URL}/{nuts_code}"""
        try:
            response: requests.Response = requests.get(url)
            response.raise_for_status()
        except requests.HTTPError as e:
            if e.response.status_code == 403:
                raise UnauthorizedException('You are not authorized to perform this operation.')
            else:
                raise ServerException('An unexpected error occured.')

        response_content: Dict = json.loads(response.content)

        nuts_region = NutsRegion(
            code = response_content['code'],
            name = response_content['name'],
            level = response_content['level'],
            parent = response_content['parent'],
            geometry = ewkt_loads(response_content['geometry']),
        )

        return nuts_region

    def get_children_nuts_codes(self, parent_region_code: str = "") -> list[str]:
        logging.debug(f'ApiClient: get_nuts_region')
        url: str = f"""{self.base_url}{self.NUTS_CODES_URL}?parent={parent_region_code}"""
        try:
            response: requests.Response = requests.get(url)
            response.raise_for_status()
        except requests.HTTPError as e:
            if e.response.status_code == 403:
                raise UnauthorizedException('You are not authorized to perform this operation.')
            else:
                raise ServerException('An unexpected error occured.')

        return json.loads(response.content)
