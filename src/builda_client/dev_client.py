from enum import Enum
import json
import logging
from typing import Any, Dict, Optional, Tuple
from uuid import UUID

import requests
from shapely.geometry import Polygon, shape
from builda_client.base_client import BaseClient
from builda_client.util import load_config

from builda_client.exceptions import (
    MissingCredentialsException,
    ServerException,
    UnauthorizedException,
)
from builda_client.model import (
    Address,
    AddressSource,
    CoordinatesSource,
    FloatSource,
    HeatDemandStatistics,
    BuildingUseStatistics,
    FootprintAreaStatistics,
    HeightStatistics,
    BuildingStatistics,
    IntSource,
    LineageResponseDto,
    SourceResponseDto,
    StringSource
)
from builda_client.dev_model import (
    AdditionalInfo,
    AddressInfo,
    Building,
    BuildingBase,
    BuildingStockInfo,
    Coordinates,
    ElevationInfo,
    FacadeAreaInfo,
    FloorAreasInfo,
    Lineage,
    NonResidentialBuilding,
    NormHeatingLoadInfo,
    PvPotential,
    PvPotentialSource,
    ResidentialBuilding,
    ResidentialBuildingResponseDto,
    ResidentialBuildingWithSourceDto,
    SizeClassInfo,
    BuildingParcel,
    BuildingStockEntry,
    ConstructionYearInfo,
    EnergyConsumption,
    EnhancedJSONEncoder,
    HeatDemandInfo,
    HeightInfo,
    HousingUnitCountInfo,
    Metadata,
    NutsRegion,
    Parcel,
    ParcelInfo,
    ParcelMinimalDto,
    PvPotentialInfo,
    RefurbishmentStateInfo,
    RoofCharacteristicsInfo,
    TabulaTypeInfo,
    TypeInfo,
    UseInfo,
    EnergySystemInfo,
    BuildingGeometry,
    NonResidentialEnergyConsumptionStatistics,
    EnergyCommodityStatistics,
    PvPotentialStatistics,
    Household,
    Person
)
from builda_client.util import determine_nuts_query_param, ewkt_loads

class Phase(Enum):
    LOCAL = "local"
    DEVELOPMENT = "development"
    PRODUCTION = "production"

class BuildaDevClient(BaseClient):
    """API-client for accessing the private endpoints of the ETHOS.BUILDA API, 
    which are currently available only for internal use to members of the IEK-3
    at Forschungszentrum Jülich.
    For accessing the public endpoints, please use BuildaClient by importing:
    from builda_client.client import BuildaClient

    Raises:
        UnauthorizedException: If current user is not authorized to execute method.
        ClientException: If a client side error occurs.
        ServerException: If a server side error occurs.
        MissingCredentialsException: If no credentials were provided to a method that 
            requires authentication.

    Returns:
        BuildaDevClient: Client for ETHOS.BUILDA API access with methods for internal use.
    """
    # For internal users and developpers/ write users of database

    # Buildings
    BUILDINGS_URL = "buildings/stripped"
    BUILDINGS_WITH_SOURCES_URL = "buildings/sources"
    RESIDENTIAL_BUILDINGS_URL = "buildings/residential/stripped"
    RESIDENTIAL_BUILDINGS_WITH_SOURCES_URL = "buildings/residential/sources"
    NON_RESIDENTIAL_BUILDINGS_URL = "buildings/non-residential/stripped"
    NON_RESIDENTIAL_BUILDINGS_WITH_SOURCES_URL = "buildings/non-residential/sources"

    AUTH_URL = "/auth/api-token"
    BUILDINGS_BASE_URL = "buildings-base/"
    ADDRESS_URL = "address/"
    BUILDINGS_PARCEL_URL = "buildings-parcel/"
    BUILDINGS_ENERGY_CHARACTERISTICS_URL = (
        "buildings/residential/energy-characteristics"
    )
    BUILDINGS_ID_URL = "buildings-id/"
    BUILDINGS_GEOMETRY_URL = "buildings-geometry/"
    SIZE_CLASS_URL = "size-class"
    VIEW_REFRESH_URL = "refresh-materialized_view"
    BUILDING_STOCK_URL = "building-stock"
    NUTS_URL = "nuts"
    NUTS_CODES_URL = "nuts-codes/"
    TYPE_URL = "type/"
    USE_URL = "use/"
    HEIGHT_URL = "height/"
    ELEVATION_URL = "elevation/"
    FACADE_AREA_URL = "facade-area/"
    HOUSING_UNIT_COUNT_URL = "housing-unit-count"
    HOUSEHOLD_URL = "households"
    PERSON_URL = "persons"
    MOBILITY_PREFERENCE_URL = "persons/mobility-preferences"
    ENERGY_SYSTEM_URL = "energy-system"
    ENERGY_CONSUMPTION_URL = "energy-consumption"
    HEAT_DEMAND_URL = "heat-demand"
    NORM_HEATING_LOAD_URL = "norm-heating-load"
    PV_POTENTIAL_URL = "pv-potential/"
    CONSTRUCTION_YEAR_URL = "construction-year"
    TIMING_LOG_URL = "admin/timing-log"
    NUTS_URL = "nuts"
    PARCEL_URL = "parcels"
    PARCEL_INFO_URL = "parcel-info"
    ROOF_CHARACTERISTICS_INFO_URL = "roof-characteristics/"
    TABULA_TYPE_URL = "tabula-type"
    FLOOR_AREAS_URL = "floor-areas"
    ADDITIONAL_URL = "additional"

    METADATA_URL = "metadata"
    LINEAGE_URL = "lineage"

    CUSTOM_QUERY_URL = "custom-query"

    # Statistics
    TYPE_STATISTICS_BY_GEOM_URL = "statistics/building-type/geom"
    FOOTPRINT_AREA_STATISTICS_BY_GEOM_URL = "statistics/footprint-area/geom"
    HEIGHT_STATISTICS_BY_GEOM_URL = "statistics/height/geom"
    NON_RESIDENTIAL_USE_STATISTICS_BY_GEOM_URL = (
        "statistics/non-residential/building-use/geom"
    )
    RESIDENTIAL_HEAT_DEMAND_STATISTICS_BY_GEOM_URL = (
        "statistics/residential/heat-demand/geom"
    )

    RESIDENTIAL_ENERGY_COMMODITY_STATISTICS_URL = (
        "statistics/residential/energy-commodities"
    )
    RESIDENTIAL_ENERGY_COMMODITY_STATISTICS_BY_GEOM_URL = (
        "statistics/residential/energy-commodities/geom"
    )
    RESIDENTIAL_ENERGY_CONSUMPTION_STATISTICS_URL = (
        "statistics/residential/energy-consumption"
    )
    RESIDENTIAL_ENERGY_CONSUMPTION_STATISTICS_BY_GEOM_URL = (
        "statistics/residential/energy-consumption/geom"
    )
    NON_RESIDENTIAL_ENERGY_CONSUMPTION_STATISTICS_URL = (
        "statistics/non-residential/energy-consumption"
    )
    NON_RESIDENTIAL_ENERGY_CONSUMPTION_STATISTICS_BY_GEOM_URL = (
        "statistics/non-residential/energy-consumption/geom"
    )
    PV_GENERATION_POTENTIAL_STATISTICS_URL = "statistics/pv-generation-potential"

    def __init__(
        self,
        username: str,
        password: str,
        phase: Phase = Phase.DEVELOPMENT,
        proxy: bool = False,
        version: str = "v0"
    ):
        """Constructor.

        Args:
            proxy (bool, optional): Whether to use a proxy or not. Proxy should be used
                when using client on cluster compute nodes. Defaults to False.
            username (str): Username for API authentication.
            password (str): Password for API authentication.
            phase (Phase, optional): The 'phase' the client is used in, i.e. which
                database to access. Defaults to Phase.DEVELOPMENT.
            version (str, optional): The API version. Defaults to v0
        """
        super().__init__()

        self.username = username
        self.password = password
        self.phase = phase
        self.config = load_config()

        address = self.config["proxy_address"] if proxy else self.config[self.phase.value]["api_address"]
      
        self.base_url = f"""{address}/api/{version}/"""
        self.authentication_url = f"""{address}{self.AUTH_URL}"""
        self.api_token = self.__get_authentication_token()

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
            logging.info(
                "Username and/or password not provided. Proceeding in unauthenticated mode."
            )
            return ""
        url: str = f"""{self.authentication_url}"""
        try:
            response: requests.Response = requests.post(
                url, data={"username": self.username, "password": self.password}
            )
            response.raise_for_status()
            return json.loads(response.content)["token"]
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def __construct_authorization_header(self, json=True) -> Dict[str, str]:
        """Constructs the header for authorization including the API token.

        Returns:
            Dict[str, str]: The authorization header.
        """
        if json:
            return {
                "Authorization": f"Token {self.api_token}",
                "Content-Type": "application/json",
            }
        else:
            return {"Authorization": f"Token {self.api_token}"}

    def get_buildings_base(
        self,
        nuts_code: str = "",
        building_type: str | None = "",
        geom: Optional[Polygon] = None,
    ) -> list[BuildingBase]:
        """Gets buildings with reduced parameter set within the specified NUTS region
        that fall into the provided type category.

        Args:
            nuts_code (str | None, optional): The NUTS-code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions. Defaults to None.
            building_type (str): The type of building ('residential', 'non-residential', 'mixed').
                If None will return all buildings with no type, if empty string (or not
                provided) will return all buildings independent of type.

        Raises:
            ServerException: When the DB is inconsistent and more than one building with
                same ID is returned.

        Returns:
            gpd.GeoDataFrame: A geodataframe with all buildings.
        """
        logging.debug(
            "ApiClient: get_buildings_base(nuts_code = %s, type = %s)",
            nuts_code,
            building_type,
        )
        nuts_query_param: str = determine_nuts_query_param(nuts_code)
        type_is_null = "False"
        if building_type is None:
            type_is_null = "True"
            building_type = ""
        if building_type == '':
            type_is_null = ""


        url: str = f"""{self.base_url}{self.BUILDINGS_BASE_URL}?{nuts_query_param}={nuts_code}&type={building_type}&type__isnull={type_is_null}"""
        if geom:
            url += f"&geom={geom}"

        try:
            response: requests.Response = requests.get(url, headers=self.__construct_authorization_header())
            logging.debug("ApiClient: received response. Checking for errors.")
            response.raise_for_status()
        except requests.HTTPError as e:
            self.handle_exception(e)

        logging.debug(
            "ApiClient: received ok response, proceeding with deserialization."
        )
        buildings = self.__deserialize(response.content)
        return buildings

    def get_buildings(
        self,
        building_type: Optional[str] = "",
        street: str = "",
        housenumber: str = "",
        postcode: str = "",
        city: str = "",
        nuts_code: str = "",
        ids: Optional[list[str]] = None
    ) -> list[Building]:
        """[REQUIRES AUTHENTICATION] 
        Gets all buildings that match the query parameters without sources.
        
        Args:
            building_type (str): The type of building ('residential', 'non-residential', 'mixed'). 
                If building_type = None returns all buildings without type.
                If building_type = "", returns all buildings independent of type.
            street (str, optional): The name of the street. Defaults to "".
            housenumber (str, optional): The house number. Defaults to "".
            postcode (str, optional): The postcode. Defaults to "".
            city (str, optional): The city. Defaults to "".
            nuts_code (str, optional): The NUTS-code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions or 2019 LAU definition.
                Defaults to None.

        Raises:
            ServerException: When an error occurs on the server side..

        Returns:
            list[Building]: A list of buildings.
        """

        logging.debug(
            """ApiClient: get_buildings(street=%s, housenumber=%s, postcode=%s, city=%s, 
            nuts_code=%s, type=%s)""",
            street,
            housenumber,
            postcode,
            city,
            nuts_code,
            building_type,
        )
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )
        nuts_query_param: str = determine_nuts_query_param(nuts_code)

        type_is_null = "False"
        if building_type is None:
            type_is_null = "True"
            building_type = ""
        elif building_type == '':
            type_is_null = ""

        url: str = f"""{self.base_url}{self.BUILDINGS_URL}?street={street}&house_number={housenumber}&postcode={postcode}&city={city}&{nuts_query_param}={nuts_code}&type={building_type}&type__isnull={type_is_null}&type__isnull={type_is_null}"""
        if ids:
            url += f"&id__in={','.join(ids)}"

        try:
            response: requests.Response = requests.get(url, timeout=3600, headers=self.__construct_authorization_header())
            logging.debug("ApiClient: received response. Checking for errors.")
            response.raise_for_status()
        except requests.HTTPError as err:
            self.handle_exception(err)

        logging.debug(
            "ApiClient: received ok response, proceeding with deserialization."
        )
        results: list[Dict] = json.loads(response.content)
        buildings: list[Building] = []
        for result in results:
            coordinates = Coordinates(
                latitude=result["coordinates"]["latitude"],
                longitude=result["coordinates"]["longitude"],
            )
            pv_potential = PvPotential(
                capacity_kW=result["pv_potential"]["capacity_kW"],
                generation_kWh=result["pv_potential"]["generation_kWh"],
            ) if result["pv_potential"] else None
            building = Building(
                id=result["id"],
                coordinates=coordinates,
                address=result["address"],
                footprint_area_m2=result["footprint_area_m2"],
                height_m=result["height_m"],
                elevation_m=result["elevation_m"],
                facade_area_m2=result["facade_area_m2"],
                type=result["type"],
                roof_shape=result["roof_shape"],
                pv_potential=pv_potential,
                additional=result["additional"]
            )
            buildings.append(building)

        return buildings
    
    def get_residential_buildings(
        self,
        street: str = "",
        housenumber: str = "",
        postcode: str = "",
        city: str = "",
        nuts_code: str = "",
        include_mixed: bool = True,
    ) -> list[ResidentialBuilding]:
        """[REQUIRES AUTHENTICATION] 
        Gets all residential buildings that match the query parameters.

        Args:
            street (str, optional): The name of the street. Defaults to "".
            housenumber (str, optional): The house number. Defaults to "".
            postcode (str, optional): The postcode. Defaults to "".
            city (str, optional): The city. Defaults to "".
            nuts_code (str, optional): The NUTS-code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions or 2019 LAU definition.
                Defaults to "".
            include_mixed (bool, optional): Whether or not to include mixed buildings.
                Defaults to True.

        Raises:
            ServerException: When an error occurs on the server side..

        Returns:
            list[ResidentialBuilding]: A list of residential buildings.
        """

        logging.debug(
            """ApiClient: get_buildings(street=%s, housenumber=%s, postcode=%s, city=%s, 
            nuts_code=%s)""",
            street,
            housenumber,
            postcode,
            city,
            nuts_code,
        )
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )
        nuts_query_param: str = determine_nuts_query_param(nuts_code)
        building_type = "" if include_mixed else "residential"

        url: str = f"""{self.base_url}{self.RESIDENTIAL_BUILDINGS_URL}?street={street}&house_number={housenumber}&postcode={postcode}&city={city}&{nuts_query_param}={nuts_code}&type={building_type}"""
        try:
            response: requests.Response = requests.get(
                url, 
                timeout=3600, 
                headers=self.__construct_authorization_header()
            )
            logging.debug("ApiClient: received response. Checking for errors.")
            response.raise_for_status()
        except requests.HTTPError as err:
            self.handle_exception(err)

        logging.debug(
            "ApiClient: received ok response, proceeding with deserialization."
        )
        results: list[Dict] = json.loads(response.content)
        buildings: list[ResidentialBuilding] = []
        for result in results:
            coordinates = Coordinates(
                latitude=result["coordinates"]["latitude"],
                longitude=result["coordinates"]["longitude"],
            )
            pv_potential = PvPotential(
                capacity_kW=result["pv_potential"]["capacity_kW"],
                generation_kWh=result["pv_potential"]["generation_kWh"],
            ) if result["pv_potential"] else None
            building = ResidentialBuilding(
                id=result["id"],
                coordinates=coordinates,
                address=result["address"],
                footprint_area_m2=result["footprint_area_m2"],
                height_m=result["height_m"],
                elevation_m=result["elevation_m"],
                facade_area_m2=result["facade_area_m2"],
                type=result["type"],
                construction_year=result["construction_year"],
                roof_shape=result["roof_shape"],
                pv_potential=pv_potential,
                size_class=result["size_class"],
                refurbishment_state=result["refurbishment_state"],
                tabula_type=result["tabula_type"],
                useful_area_m2=result["useful_area_m2"],
                conditioned_living_area_m2=result["conditioned_living_area_m2"],
                net_floor_area_m2=result["net_floor_area_m2"],
                yearly_heat_demand_mwh=result["yearly_heat_demand_mwh"],
                housing_unit_count=result["housing_unit_count"],
                norm_heating_load_kw=result["norm_heating_load_kw"],
                households=result["households"],
                energy_system=result["energy_system"],
                additional=result["additional"],
            )
            buildings.append(building)

        return buildings
    
    def get_residential_buildings_with_sources(
        self,
        street: str = "",
        housenumber: str = "",
        postcode: str = "",
        city: str = "",
        nuts_code: str = "",
        include_mixed: bool = True,
    ) -> ResidentialBuildingResponseDto:
        """Gets all residential buildings that match the query parameters.

        Args:
            street (str, optional): The name of the street. Defaults to "".
            housenumber (str, optional): The house number. Defaults to "".
            postcode (str, optional): The postcode. Defaults to "".
            city (str, optional): The city. Defaults to "".
            nuts_code (str, optional): The NUTS-code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions or 2019 LAU definition.
                Defaults to "".
            include_mixed (bool, optional): Whether or not to include mixed buildings.
                Defaults to True.

        Raises:
            ServerException: When an error occurs on the server side..

        Returns:
            list[ResidentialBuilding]: A list of residential buildings.
        """

        logging.debug(
            """ApiClient: get_buildings(street=%s, housenumber=%s, postcode=%s, city=%s, 
            nuts_code=%s)""",
            street,
            housenumber,
            postcode,
            city,
            nuts_code,
        )
        nuts_query_param: str = determine_nuts_query_param(nuts_code)
        building_type = "" if include_mixed else "residential"

        url: str = f"""{self.base_url}{self.RESIDENTIAL_BUILDINGS_WITH_SOURCES_URL}?street={street}&house_number={housenumber}&postcode={postcode}&city={city}&{nuts_query_param}={nuts_code}&type={building_type}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600, headers=self.__construct_authorization_header())
            logging.debug("ApiClient: received response. Checking for errors.")
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

        logging.debug(
            "ApiClient: received ok response, proceeding with deserialization."
        )
        results: Dict = json.loads(response.content)
        buildings: list[ResidentialBuildingWithSourceDto] = []
        for result in results["buildings"]:
            coordinates = CoordinatesSource(
                value = Coordinates(
                    latitude=result["coordinates"]['value']["latitude"],
                    longitude=result["coordinates"]['value']["longitude"]),
                source = result["coordinates"]["source"],
                lineage = result["coordinates"]["lineage"],
            )
            pv_potential = PvPotentialSource(
                value = PvPotential(
                    capacity_kW=result["pv_potential"]["value"]["capacity_kW"],
                    generation_kWh=result["pv_potential"]["value"]["generation_kWh"]),
                source = result["pv_potential"]["source"],
                lineage = result["pv_potential"]["lineage"],
            ) if result["pv_potential"]["value"] else None
            address = AddressSource(
                value = Address(
                    street = result["address"]["value"]["street"],
                    house_number = result["address"]["value"]["house_number"],
                    postcode = result["address"]["value"]["postcode"],
                    city = result["address"]["value"]["city"],
                ),
                source = result["address"]["source"],
                lineage = result["address"]["lineage"],
            )
            building = ResidentialBuildingWithSourceDto(
                id=result["id"],
                coordinates=coordinates,
                address=address,
                footprint_area_m2=result["footprint_area_m2"],
                height_m=FloatSource(
                    value=result["height_m"]["value"], 
                    source=result["height_m"]["source"],
                    lineage=result["height_m"]["lineage"],
                    ),
                elevation_m=FloatSource(
                    value=result["elevation_m"]["value"], 
                    source=result["elevation_m"]["source"],
                    lineage=result["elevation_m"]["lineage"],
                    ),
                facade_area_m2=FloatSource(
                    value=result["facade_area_m2"]["value"], 
                    source=result["facade_area_m2"]["source"],
                    lineage=result["facade_area_m2"]["lineage"],
                    ),
                type=StringSource(
                    value=result["type"]["value"], 
                    source=result["type"]["source"],
                    lineage=result["type"]["source"],
                    ),
                roof_shape=StringSource(
                    value=result["roof_shape"]["value"], 
                    source=result["roof_shape"]["source"],
                    lineage=result["roof_shape"]["lineage"],
                    ),
                construction_year=IntSource(
                    value=result["construction_year"]["value"],
                    source=result["construction_year"]["source"],
                    lineage=result["construction_year"]["lineage"],
                    ),
                pv_potential=pv_potential,
                size_class=StringSource(
                    value=result["size_class"]["value"], 
                    source=result["size_class"]["source"],
                    lineage=result["size_class"]["lineage"],
                    ),
                refurbishment_state=IntSource(
                    value=result["refurbishment_state"]["value"], 
                    source=result["refurbishment_state"]["source"],
                    lineage=result["refurbishment_state"]["lineage"],
                    ),
                tabula_type=StringSource(
                    value=result["tabula_type"]["value"], 
                    source=result["tabula_type"]["source"],
                    lineage=result["tabula_type"]["lineage"],
                    ),
                useful_area_m2=FloatSource(
                    value=result["useful_area_m2"]["value"], 
                    source=result["useful_area_m2"]["source"],
                    lineage=result["useful_area_m2"]["lineage"],
                    ),
                conditioned_living_area_m2=FloatSource(
                    value=result["conditioned_living_area_m2"]["value"], 
                    source=result["conditioned_living_area_m2"]["source"],
                    lineage=result["conditioned_living_area_m2"]["lineage"],
                    ),
                net_floor_area_m2=FloatSource(
                    value=result["net_floor_area_m2"]["value"], 
                    source=result["net_floor_area_m2"]["source"],
                    lineage=result["net_floor_area_m2"]["lineage"],
                    ),
                yearly_heat_demand_mwh=FloatSource(
                    value=result["yearly_heat_demand_mwh"]["value"], 
                    source=result["yearly_heat_demand_mwh"]["source"],
                    lineage=result["yearly_heat_demand_mwh"]["lineage"],
                    ),
                housing_unit_count=IntSource(
                    value=result["housing_unit_count"]["value"], 
                    source=result["housing_unit_count"]["source"],
                    lineage=result["housing_unit_count"]["lineage"],
                    ),
                norm_heating_load_kw=FloatSource(
                    value=result["norm_heating_load_kw"]["value"], 
                    source=result["norm_heating_load_kw"]["source"],
                    lineage=result["norm_heating_load_kw"]["lineage"],
                    ),
                households=StringSource(
                    value=result["households"]["value"], 
                    source=result["households"]["source"],
                    lineage=result["households"]["lineage"],
                    ),
                energy_system=result["energy_system"],
                additional=result["additional"],
            )
            buildings.append(building)

        data_sources: list[SourceResponseDto] = []
        for entry in results["sources"]:
            source = SourceResponseDto(
                key=entry["key"],
                name=entry["name"],
                provider=entry["provider"],
                referring_website=entry["referring_website"],
                license=entry["license"],
                citation=entry["citation"],
            )
            
            data_sources.append(source)
        
        data_lineages: list[LineageResponseDto] = []
        for entry in results["lineages"]:
            lineage = LineageResponseDto(
                key=entry["key"],
                description=entry["description"],
            )
            
            data_lineages.append(lineage)

        return ResidentialBuildingResponseDto(
            buildings=buildings, 
            sources=data_sources, 
            lineages=data_lineages)
    
    def get_non_residential_buildings(
        self,
        street: str = "",
        housenumber: str = "",
        postcode: str = "",
        city: str = "",
        nuts_code: str = "",
        include_mixed: bool = True,
        exclude_auxiliary: bool = False,
        geom: Polygon | None = None,
    ) -> list[NonResidentialBuilding]:
        """[REQUIRES AUTHENTICATION] 
        Gets all non-residential buildings that match the query parameters.

        Args:
            street (str, optional): The name of the street. Defaults to "".
            housenumber (str, optional): The house number. Defaults to "".
            postcode (str, optional): The postcode. Defaults to "".
            city (str, optional): The city. Defaults to "".
            nuts_code (str, optional): The NUTS-code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions or 2019 LAU definition.
                Defaults to "".
            include_mixed (bool, optional): Whether or not to include mixed buildings.
                Defaults to True.
            exclude_auxiliary (bool, optional): Whether to exclude auxiliary buildings.
                Defaults to False.
            geom (Polygon, optional): Only return buildings within this geometry.

        Raises:
            ServerException: When an error occurs on the server side..

        Returns:
            list[NonResidentialBuilding]: A list of non-residential buildings.
        """

        logging.debug(
            """ApiClient: get_non_residential_buildings(street=%s, housenumber=%s, 
            postcode=%s, city=%s, nuts_code=%s)""",
            street,
            housenumber,
            postcode,
            city,
            nuts_code,
        )
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )
        nuts_query_param: str = determine_nuts_query_param(nuts_code)
        building_type = "" if include_mixed else "non-residential"

        url: str = f"""{self.base_url}{self.NON_RESIDENTIAL_BUILDINGS_URL}?street={street}&house_number={housenumber}&postcode={postcode}&city={city}&{nuts_query_param}={nuts_code}&type={building_type}&exclude_auxiliary={exclude_auxiliary}"""
        if geom:
            url += f"&geom={geom}"
        try:
            response: requests.Response = requests.get(
                url, 
                timeout=3600,
                headers=self.__construct_authorization_header())
            logging.debug("ApiClient: received response. Checking for errors.")
            response.raise_for_status()
        except requests.HTTPError as err:
            self.handle_exception(err)

        logging.debug(
            "ApiClient: received ok response, proceeding with deserialization."
        )
        results: list[Dict] = json.loads(response.content)
        buildings: list[NonResidentialBuilding] = []
        for result in results:
            coordinates = Coordinates(
                latitude=result["coordinates"]["latitude"],
                longitude=result["coordinates"]["longitude"],
            )
            pv_potential = PvPotential(
                capacity_kW=result["pv_potential"]["capacity_kW"],
                generation_kWh=result["pv_potential"]["generation_kWh"],
            ) if result["pv_potential"] else None
            building = NonResidentialBuilding(
                id=result["id"],
                coordinates=coordinates,
                address=result["address"],
                footprint_area_m2=result["footprint_area_m2"],
                height_m=result["height_m"],
                elevation_m=result["elevation_m"],
                facade_area_m2=result["facade_area_m2"],
                type=result["type"],
                roof_shape=result["roof_shape"],
                use=result["use"],
                pv_potential=pv_potential,
                electricity_consumption_mwh=result["electricity_consumption_MWh"],
                additional=result["additional"]
            )
            buildings.append(building)
        return buildings

    def get_buildings_parcel(
        self, nuts_code: str = "", type: str = "", geom: Optional[Polygon] = None
    ) -> list[BuildingParcel]:
        """Gets buildings with reduced parameter set including parcel within the
        specified NUTS region that fall into the provided type category.

        Args:
            nuts_code (str | None, optional): The NUTS-code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions. Defaults to None.
            type (str): The type of building ('residential', 'non-residential')

        Raises:
            ServerException: When the DB is inconsistent and more than one building with
                same ID is returned.

        Returns:
            gpd.GeoDataFrame: A geodataframe with all buildings.
        """
        logging.debug(
            f"ApiClient: get_buildings_parcel(nuts_code = {nuts_code}, type = {type})"
        )
        nuts_query_param: str = determine_nuts_query_param(nuts_code)
        url: str = f"""{self.base_url}{self.BUILDINGS_PARCEL_URL}?{nuts_query_param}={nuts_code}&type={type}"""
        if geom:
            url += f"&geom={geom}"

        try:
            response: requests.Response = requests.get(url, headers=self.__construct_authorization_header())
            logging.debug("ApiClient: received response. Checking for errors.")
            response.raise_for_status()
        except requests.HTTPError as err:
            self.handle_exception(err)

        logging.debug(
            "ApiClient: received ok response, proceeding with deserialization."
        )
        buildings = self.__deserialize_buildings_parcel(response.content)
        return buildings

    def get_building_ids(
        self, nuts_code: str = "", type: str = "", geom: Optional[Polygon] = None, height_max: Optional[float] = None
    ) -> list[str]:
        logging.debug(
            f"ApiClient: get_building_ids(nuts_code = {nuts_code}, type = {type})"
        )
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )
        nuts_query_param: str = determine_nuts_query_param(nuts_code)
        height_lt = "" if height_max is None else str(height_max)
        url: str = f"""{self.base_url}{self.BUILDINGS_ID_URL}?{nuts_query_param}={nuts_code}&type={type}&height__lt={height_lt}"""
        if geom:
            url += f"&geom={geom}"
            
        try:
            response: requests.Response = requests.get(url, headers=self.__construct_authorization_header())
            logging.debug("ApiClient: received response. Checking for errors.")
            response.raise_for_status()
        except requests.HTTPError as e:
            self.handle_exception(e)

        logging.debug(
            "ApiClient: received ok response, proceeding with deserialization."
        )
        building_ids: list[str] = json.loads(response.content)

        return building_ids

    def __deserialize(self, response_content):
        results: list[str] = json.loads(response_content)
        buildings: list[BuildingBase] = []
        for res_json in results:
            res = json.loads(res_json)
            building = BuildingBase(
                id=res["id"],
                footprint=shape(res["footprint"]),
                centroid=shape(res["centroid"]),
                type=res["type"],
            )
            buildings.append(building)
        return buildings

    def __deserialize_buildings_parcel(self, response_content):
        results: list[str] = json.loads(response_content)
        buildings: list[BuildingParcel] = []
        for res_json in results:
            res = json.loads(res_json)
            parcel: ParcelMinimalDto | None = None
            if res["parcel_id"] != "None" and res["parcel_geom"] != "None":
                parcel = ParcelMinimalDto(
                    id=UUID(res["parcel_id"]), shape=shape(res["parcel_geom"])
                )
            building = BuildingParcel(
                id=res["id"],
                footprint=shape(res["footprint"]),
                centroid=shape(res["centroid"]),
                type=res["type"],
                parcel=parcel,
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
            id_str = ",".join([str(id) for id in ids])
            url += f"?ids={id_str}"

        try:
            response: requests.Response = requests.get(
                url, headers=self.__construct_authorization_header()
            )
            response.raise_for_status()
        except requests.HTTPError as e:
            if e.response.status_code == 403:
                raise UnauthorizedException(
                    "You are not authorized to perform this operation."
                )
            else:
                raise ServerException("An unexpected error occured.")

        results: Dict = json.loads(response.content)
        parcels: list[Parcel] = []

        for result in results:
            parcel = Parcel(
                id=UUID(result["id"]), shape=ewkt_loads(result["shape"]), source="test"
            )
            parcels.append(parcel)
        return parcels

    def post_parcel_infos(self, parcel_infos: list[ParcelInfo]):
        logging.debug("ApiClient: post_parcel_infos")
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )

        url: str = f"""{self.base_url}{self.PARCEL_INFO_URL}"""

        parcel_infos_json = json.dumps(parcel_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(
                url,
                data=parcel_infos_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def add_parcels(self, parcels: list[Parcel]):
        """
        [REQUIRES AUTHENTICATION] Adds parcels.

        Args:
            parcels (list[Parcel]): A list of parcels.
        """
        if not self.api_token:
            raise MissingCredentialsException(
                "This endpoint is private. You need to provide username and password when initializing the client."
            )
        url: str = f"""{self.base_url}{self.PARCEL_URL}"""

        parcels_json = json.dumps(parcels, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(
                url, data=parcels_json, headers=self.__construct_authorization_header()
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def modify_building(self, building_id: str, building_data: Dict):
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )
        url: str = f"""{self.base_url}{self.BUILDING_STOCK_URL}/{building_id}"""
        building_json = json.dumps(building_data)
        try:
            response: requests.Response = requests.put(
                url, data=building_json, headers=self.__construct_authorization_header()
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)


    def refresh_buildings(self, building_type: str) -> None:
        """[REQUIRES AUTHENTICATION] Refreshes the materialized view 'buildings'.

        Args:
            building_type (str): The type of building (residential, non-residential, all).

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
                case because username and password were not specified when initializing
                the client.
        """
        logging.debug("ApiClient: refresh_buildings")
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password
                when initializing the client."""
            )

        if building_type == 'residential':
            view_name = 'result.residential_attributes'
        elif building_type in ['non_residential', 'non-residential']:
            view_name = 'result.non_residential_attributes'
        else:
            view_name = 'result.all_buildings'

        url: str = f"""{self.base_url}{self.VIEW_REFRESH_URL}/{view_name}"""
        try:
            response: requests.Response = requests.post(
                url, headers=self.__construct_authorization_header(json=False)
            )
            response.raise_for_status()
        except requests.HTTPError as err:
            self.handle_exception(err)

    def refresh_materialized_view(self, view_name: str):
        """[REQUIRES AUTHENTICATION] Refreshes the materialized view.

        Args:
            view_name (str): The name of the materialized view to be refreshed.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
                case because username and password were not specified when initializing
                the client.
        """
        logging.debug("ApiClient: refresh_buildings")
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password
                when initializing the client."""
            )

        url: str = f"""{self.base_url}{self.VIEW_REFRESH_URL}/{view_name}"""
        try:
            response: requests.Response = requests.post(
                url, headers=self.__construct_authorization_header(json=False)
            )
            response.raise_for_status()
        except requests.HTTPError as err:
            self.handle_exception(err)

    def get_building_stock(
        self, geom: Polygon | None = None, nuts_code: str = ""
    ) -> list[BuildingStockEntry]:
        """[REQUIRES AUTHENTICATION]  Gets all entries of the building stock within the
        specified geometry.

        Args:
            geom (Polygon, optional): The polygon for which to retrieve buildings.
            nuts_code (str, optional): The NUTS region to get buildings from.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
            case because username and password were not specified when initializing the
            client.

        Returns:
            list[BuildingStockEntry]: All building stock entries that lie within the
            given polygon.
        """
        logging.debug("ApiClient: get_building_stock")

        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )

        query_params: str = ""
        if geom is not None and nuts_code:
            nuts_query_param = determine_nuts_query_param(nuts_code)
            query_params = f"?geom={geom}&{nuts_query_param}={nuts_code}"
        elif geom is not None:
            query_params = f"?geom={geom}"
        elif nuts_code:
            nuts_query_param = determine_nuts_query_param(nuts_code)
            query_params = f"?{nuts_query_param}={nuts_code}"

        url: str = f"""{self.base_url}{self.BUILDING_STOCK_URL}{query_params}"""

        try:
            response: requests.Response = requests.get(
                url, headers=self.__construct_authorization_header()
            )
            response.raise_for_status()
        except requests.HTTPError as e:
            if e.response.status_code == 403:
                raise UnauthorizedException(
                    "You are not authorized to perform this operation."
                )
            else:
                raise ServerException("An unexpected error occured.")

        buildings: list[BuildingStockEntry] = []
        results: Dict = json.loads(response.content)
        for result in results:
            building = BuildingStockEntry(
                building_id=result["building_id"],
                footprint=ewkt_loads(result["footprint"]),
                centroid=ewkt_loads(result["centroid"]),
                footprint_area=result["footprint_area"],
                nuts3=result["nuts3"],
                nuts2=result["nuts2"],
                nuts1=result["nuts1"],
                nuts0=result["nuts0"],
                lau=result["lau"],
            )
            buildings.append(building)

        return buildings

    def post_building_stock(self, buildings: list[BuildingStockInfo]) -> None:
        """[REQUIRES AUTHENTICATION]  Posts the building_stock data to the database.

        Args:
            buildings (list[BuildingStockEntry]): The building stock entries to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
            case because username and password were not specified when initializing the
            client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """

        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )
        url: str = f"""{self.base_url}{self.BUILDING_STOCK_URL}"""

        buildings_json = json.dumps(buildings, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(
                url,
                data=buildings_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def get_buildings_geometry(
        self, geom: Polygon | None = None, nuts_code: str = "", building_type: str | None = "",
    ) -> list[BuildingGeometry]:
        """[REQUIRES AUTHENTICATION]  Gets all entries of the buildings within the
        specified geometry with geometric attributes.

        Args:
            geom (Polygon, optional): The polygon for which to retrieve buildings.
            nuts_code (str, optional): The NUTS region to get buildings from.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
            case because username and password were not specified when initializing the
            client.

        Returns:
            list[BuildingStockEntry]: All building stock entries that lie within the
            given polygon.
        """
        logging.debug("ApiClient: get_buildings_geometry")

        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )

        query_params: str = ""

        type_is_null = "False"
        if building_type is None:
            type_is_null = "True"
            building_type = ""
        elif building_type == '':
            type_is_null = ""

        if geom is not None and nuts_code:
            nuts_query_param = determine_nuts_query_param(nuts_code)
            query_params = f"?geom={geom}&{nuts_query_param}={nuts_code}"
        elif geom is not None:
            query_params = f"?geom={geom}"
        elif nuts_code:
            nuts_query_param = determine_nuts_query_param(nuts_code)
            query_params = f"?{nuts_query_param}={nuts_code}"
        query_params += f"&type={building_type}&type__isnull={type_is_null}"

        url: str = f"""{self.base_url}{self.BUILDINGS_GEOMETRY_URL}{query_params}"""

        try:
            response: requests.Response = requests.get(
                url, headers=self.__construct_authorization_header()
            )
            response.raise_for_status()
        except requests.HTTPError as e:
            if e.response.status_code == 403:
                raise UnauthorizedException(
                    "You are not authorized to perform this operation."
                )
            else:
                raise ServerException("An unexpected error occured.")

        buildings: list[BuildingGeometry] = []
        results: Dict = json.loads(response.content)
        for result_json in results:
            result = json.loads(result_json)
            building = BuildingGeometry(
                id=result["id"],
                footprint=shape(result["footprint"]),
                centroid=shape(result["centroid"]),
                height_m=result["height_m"],
                roof_shape=result["roof_shape"],
                roof_geometry=result["roof_geometry"],
                type=result["type"],
                nuts0=result["nuts0"],
                nuts1=result["nuts1"],
                nuts2=result["nuts2"],
                nuts3=result["nuts3"],
                lau=result["lau"],
            )
            buildings.append(building)

        return buildings


    def post_nuts(self, nuts_regions: list[NutsRegion]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the nuts data to the database. Private
        endpoint: requires client to have credentials.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
                    case because username and password were not specified when initializing the
                client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_nuts")

        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )

        url: str = f"""{self.base_url}{self.NUTS_URL}"""

        nuts_regions_json = json.dumps(nuts_regions, cls=EnhancedJSONEncoder)

        try:
            response: requests.Response = requests.post(
                url,
                data=nuts_regions_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def post_addresses(self, addresses: list[AddressInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts addresses to the database.

        Args:
            addresses (list[Address]): The address data to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
                case because username and password were not specified when initializing
                the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_addresses")
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )

        url: str = f"""{self.base_url}{self.ADDRESS_URL}"""
        addresses_json = json.dumps(addresses, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(
                url,
                data=addresses_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def post_type_info(self, type_infos: list[TypeInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the type info data to the database.

        Args:
            type_infos (list[TypeInfo]): The type info data to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
                case because username and password were not specified when initializing
                the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """

        logging.debug("ApiClient: post_type_info")
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )

        url: str = f"""{self.base_url}{self.TYPE_URL}"""

        type_infos_json = json.dumps(type_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(
                url,
                data=type_infos_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def post_use_info(self, use_infos: list[UseInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the use info data to the database.

        Args:
            use_infos (list[UseInfo]): The use info data to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
                case because username and password were not specified when initializing
                the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """

        logging.debug("ApiClient: post_use_info")
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )

        url: str = f"""{self.base_url}{self.USE_URL}"""

        use_infos_json = json.dumps(use_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(
                url,
                data=use_infos_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def post_height_info(self, height_infos: list[HeightInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the height data to the database.

        Args:
            height_infos (list[HeightInfo]): The height data to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
                case because username and password were not specified when initializing
                the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_height_info")
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )

        url: str = f"""{self.base_url}{self.HEIGHT_URL}"""
        height_infos_json = json.dumps(height_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(
                url,
                data=height_infos_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def post_elevation_info(self, infos: list[ElevationInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the elevation data to the database.

        Args:
            infos (list[ElevationInfo]): The elevation data to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
                case because username and password were not specified when initializing
                the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_elevation_info")
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )

        url: str = f"""{self.base_url}{self.ELEVATION_URL}"""
        infos_json = json.dumps(infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(
                url,
                data=infos_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)


    def post_facade_area_info(self, infos: list[FacadeAreaInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the facade area data to the database.

        Args:
            infos (list[FacadeAreaInfo]): The facade area data to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
                case because username and password were not specified when initializing
                the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_facade_area_info")
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )

        url: str = f"""{self.base_url}{self.FACADE_AREA_URL}"""
        infos_json = json.dumps(infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(
                url,
                data=infos_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)


    def post_floor_areas_info(self, floor_areas_infos: list[FloorAreasInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the floor area data to the database.

        Args:
            floor_areas_infos (list[FloorAreasInfo]): The floor areas data to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
                case because username and password were not specified when initializing
                the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_floor_areas_info")
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )

        url: str = f"""{self.base_url}{self.FLOOR_AREAS_URL}"""
        floor_areas_infos_json = json.dumps(floor_areas_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(
                url,
                data=floor_areas_infos_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def post_housing_unit_count_info(self, housing_unit_count_infos: list[HousingUnitCountInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the housing unit count data to 
        the database.

        Args:
            housing_unit_count_infos (list[HousingUnitCountInfo]): The housing unit count data to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
                case because username and password were not specified when initializing
                the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_housing_unit_count")
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )

        url: str = f"""{self.base_url}{self.HOUSING_UNIT_COUNT_URL}"""
        housing_unit_count_infos_json = json.dumps(housing_unit_count_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(
                url,
                data=housing_unit_count_infos_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def post_households(self, households: list[Household]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the households data to the database.

        Args:
            households (list[Household]): The household data to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
                case because username and password were not specified when initializing
                the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_households")
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )

        url: str = f"""{self.base_url}{self.HOUSEHOLD_URL}"""
        households_json = json.dumps(households, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(
                url,
                data=households_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def post_persons(self, persons: list[Person]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the person data to the database.

        Args:
            persons (list[Person]): The person data to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
                case because username and password were not specified when initializing
                the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_persons")
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )

        url: str = f"""{self.base_url}{self.PERSON_URL}"""
        persons_json = json.dumps(persons, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(
                url,
                data=persons_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def update_mobility_preference(self, mobility_preference: list[Tuple[str, str]]) -> None:
        """[REQUIRES AUTHENTICATION] Updates mobility preference data.

        Args:
            mobility_preference (list[Tuple[str, str]]): Mobility preference of person.
                The tuple should consist of person id and a json with the mobility preference.
                For example:
                [
                    ('8d5aeaab-3f82-4524-8425-e65bee5ccbd0', '{"pref": "walk"}'),
                    ('0c7d1e92-f834-473f-8aa6-28d0b47eb0e0', '{"pref": "train"}')
                ]
        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
                case because username and password were not specified when initializing
                the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: update_mobility_preference")
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )

        url: str = f"""{self.base_url}{self.MOBILITY_PREFERENCE_URL}"""
        try:
            response: requests.Response = requests.post(
                url,
                json=mobility_preference,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)


    def post_energy_system_infos(
        self, energy_system_infos: list[EnergySystemInfo]
    ) -> None:
        """[REQUIRES AUTHENTICATION] Posts the energy system data to the
        database.

        Args:
            energy_system_infos (list[WaterHeatingCommodityInfo]): The water
                heating commodity infos to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
                case because username and password were not specified when initializing
                the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_water_heating_commodity")
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )

        url: str = f"""{self.base_url}{self.ENERGY_SYSTEM_URL}"""
        energy_system_infos_json = json.dumps(
            energy_system_infos, cls=EnhancedJSONEncoder
        )
        try:
            response: requests.Response = requests.post(
                url,
                data=energy_system_infos_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)


    def post_energy_consumption(
        self, energy_consumption_infos: list[EnergyConsumption]
    ) -> None:
        """[REQUIRES AUTHENTICATION] Posts the energy consumption data to the database.

        Args:
            energy_consumption_infos (list[EnergyConsumption]): The energy consumption
                infos to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
                case because username and password were not specified when initializing
                the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_energy_consumption_commodity")
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )

        url: str = f"""{self.base_url}{self.ENERGY_CONSUMPTION_URL}"""
        energy_consumption_infos_json = json.dumps(
            energy_consumption_infos, cls=EnhancedJSONEncoder
        )
        try:
            response: requests.Response = requests.post(
                url,
                data=energy_consumption_infos_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def post_heat_demand(self, heat_demand_infos: list[HeatDemandInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the heat demand data to the database.

        Args:
            heat_demand_infos (list[HeatDemandInfo]): The heat demand infos to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
                case because username and password were not specified when initializing
                the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_heat_demand")
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )

        url: str = f"""{self.base_url}{self.HEAT_DEMAND_URL}"""
        heat_demand_infos_json = json.dumps(heat_demand_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(
                url,
                data=heat_demand_infos_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def post_norm_heating_load(self, heating_load_infos: list[NormHeatingLoadInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the norm heating load data to the database.

        Args:
            heat_demand_infos (list[NormHeatingLoadInfo]): The heat demand infos to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
                case because username and password were not specified when initializing
                the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_norm_heating_load")
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )

        url: str = f"""{self.base_url}{self.NORM_HEATING_LOAD_URL}"""
        heating_load_infos_json = json.dumps(heating_load_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(
                url,
                data=heating_load_infos_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def post_pv_potential(self, pv_potential_infos: list[PvPotentialInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the pv potential data to the database.

        Args:
            pv_potential_infos (list[PvPotentialInfo]): The pv potential infos to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_pv_potential")
        if not self.api_token:
            raise MissingCredentialsException(
                "This endpoint is private. You need to provide username and password when initializing the client."
            )

        url: str = f"""{self.base_url}{self.PV_POTENTIAL_URL}"""
        pv_potential_infos_json = json.dumps(
            pv_potential_infos, cls=EnhancedJSONEncoder
        )
        try:
            response: requests.Response = requests.post(
                url,
                data=pv_potential_infos_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def post_construction_year(
        self, construction_year_infos: list[ConstructionYearInfo]
    ) -> None:
        """[REQUIRES AUTHENTICATION] Posts the construction year data to the database.

        Args:
            construction_year_infos (list[ConstructionYearInfo]): The construction year data to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_construction_year")
        if not self.api_token:
            raise MissingCredentialsException(
                "This endpoint is private. You need to provide username and password when initializing the client."
            )

        url: str = f"""{self.base_url}{self.CONSTRUCTION_YEAR_URL}"""
        construction_year_json = json.dumps(
            construction_year_infos, cls=EnhancedJSONEncoder
        )
        try:
            response: requests.Response = requests.post(
                url,
                data=construction_year_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def post_tabula_type(self, tabula_type_infos: list[TabulaTypeInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the tabula type data to the database.

        Args:
            tabula_type_infos (list[TabulaTypeInfo]): The tabula type data to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_tabula_type")
        if not self.api_token:
            raise MissingCredentialsException(
                "This endpoint is private. You need to provide username and password when initializing the client."
            )

        url: str = f"""{self.base_url}{self.TABULA_TYPE_URL}"""
        tabula_type_json = json.dumps(tabula_type_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(
                url,
                data=tabula_type_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def post_size_class(
        self, size_class_infos: list[SizeClassInfo]
    ) -> None:
        """[REQUIRES AUTHENTICATION] Posts the building size class data to the database.

        Args:
            size_class_infos (list[SizeClassInfo]): The building size class data to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_size_class")
        if not self.api_token:
            raise MissingCredentialsException(
                "This endpoint is private. You need to provide username and password when initializing the client."
            )

        url: str = f"""{self.base_url}{self.SIZE_CLASS_URL}"""
        size_class_json = json.dumps(size_class_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(
                url,
                data=size_class_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)


    def post_additional_info(self, additional_infos: list[AdditionalInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the additional data to the database.

        Args:
            household_infos (list[AdditionalInfo]): The additional data to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the
                case because username and password were not specified when initializing
                the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_additional_info")
        if not self.api_token:
            raise MissingCredentialsException(
                """This endpoint is private. You need to provide username and password 
                when initializing the client."""
            )

        url: str = f"""{self.base_url}{self.ADDITIONAL_URL}"""
        additional_infos_json = json.dumps(additional_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(
                url,
                data=additional_infos_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)


    def post_timing_log(self, function_name: str, measured_time: float):
        logging.debug("ApiClient: post_timing_log")
        if not self.api_token:
            raise MissingCredentialsException(
                "This endpoint is private. You need to provide username and password when initializing the client."
            )

        url: str = f"""{self.base_url}{self.TIMING_LOG_URL}"""

        try:
            response: requests.Response = requests.post(
                url,
                data=json.dumps(
                    {"function_name": function_name, "measured_time": measured_time}
                ),
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def get_nuts_region(self, nuts_code: str):
        logging.debug("ApiClient: get_nuts_region")
        url: str = f"""{self.base_url}{self.NUTS_URL}/{nuts_code}"""
        try:
            response: requests.Response = requests.get(url, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.HTTPError as e:
            if e.response.status_code == 403:
                raise UnauthorizedException(
                    "You are not authorized to perform this operation."
                )
            else:
                raise ServerException("An unexpected error occured.")

        response_content: Dict = json.loads(response.content)

        nuts_region = NutsRegion(
            code=response_content["code"],
            name=response_content["name"],
            level=response_content["level"],
            parent=response_content["parent"],
            geometry=ewkt_loads(response_content["geometry"]),
        )

        return nuts_region

    def get_children_nuts_codes(self, parent_region_code: str = "") -> list[str]:
        logging.debug("ApiClient: get_nuts_region")
        url: str = (
            f"""{self.base_url}{self.NUTS_CODES_URL}?parent={parent_region_code}"""
        )
        try:
            response: requests.Response = requests.get(url, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.HTTPError as e:
            if e.response.status_code == 403:
                raise UnauthorizedException(
                    "You are not authorized to perform this operation."
                )
            else:
                raise ServerException("An unexpected error occured.")

        return json.loads(response.content)

    def post_refurbishment_state(
        self, refurbishment_state_infos: list[RefurbishmentStateInfo]
    ) -> None:
        """[REQUIRES AUTHENTICATION] Posts the refurbishment state data to the database.

        Args:
            refurbishment_state_infos (list[RefurbishmentStateInfo]): The refurbishment state infos to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_refurbishment_state")
        if not self.api_token:
            raise MissingCredentialsException(
                "This endpoint is private. You need to provide username and password when initializing the client."
            )

        url: str = f"""{self.base_url}{self.REFURBISHMENT_STATE_URL}"""
        refurbishment_state_infos_json = json.dumps(
            refurbishment_state_infos, cls=EnhancedJSONEncoder
        )
        try:
            response: requests.Response = requests.post(
                url,
                data=refurbishment_state_infos_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def post_roof_characteristics(self, roof_characteristics_infos: list[RoofCharacteristicsInfo]) -> None:
        """[REQUIRES AUTHENTICATION] Posts the roof characteristics data to the database.

        Args:
            roof_characteristics_infos (list[RoofCharacteristicsInfo]): The roof characteristics data to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_roof_characteristics")
        if not self.api_token:
            raise MissingCredentialsException(
                "This endpoint is private. You need to provide username and password when initializing the client."
            )

        url: str = f"""{self.base_url}{self.ROOF_CHARACTERISTICS_INFO_URL}"""
        roof_characteristics_json = json.dumps(roof_characteristics_infos, cls=EnhancedJSONEncoder)
        try:
            response: requests.Response = requests.post(
                url,
                data=roof_characteristics_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def post_metadata(
        self, metadata: list[Metadata]
    ) -> None:
        """[REQUIRES AUTHENTICATION] Posts the source metadata to the database.

        Args:
            metadata (list[Metadata]): The metadata to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_metadata")
        if not self.api_token:
            raise MissingCredentialsException(
                "This endpoint is private. You need to provide username and password when initializing the client."
            )

        url: str = f"""{self.base_url}{self.METADATA_URL}"""
        metadata_json = json.dumps(
            metadata, cls=EnhancedJSONEncoder
        )
        try:
            response: requests.Response = requests.post(
                url,
                data=metadata_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def post_lineage(
        self, lineage: list[Lineage]
    ) -> None:
        """[REQUIRES AUTHENTICATION] Posts the lineage descriptions to the database.

        Args:
            metadata (list[Metadata]): The metadata to post.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_metadata")
        if not self.api_token:
            raise MissingCredentialsException(
                "This endpoint is private. You need to provide username and password when initializing the client."
            )

        url: str = f"""{self.base_url}{self.LINEAGE_URL}"""
        metadata_json = json.dumps(
            lineage, cls=EnhancedJSONEncoder
        )
        try:
            response: requests.Response = requests.post(
                url,
                data=metadata_json,
                headers=self.__construct_authorization_header(),
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def execute_query(
        self, query: str
    ) -> Any:
        """[REQUIRES AUTHENTICATION] Executes a custom sql query.

        Args:
            query (str): SQL query.

        Raises:
            MissingCredentialsException: If no API token exists. This is probably the case because username and password were not specified when initializing the client.
            UnauthorizedException: If the API token is not accepted.
            ClientException: If an error on the client side occurred.
            ServerException: If an unexpected error on the server side occurred.
        """
        logging.debug("ApiClient: post_metadata")
        if not self.api_token:
            raise MissingCredentialsException(
                "This endpoint is private. You need to provide username and password when initializing the client."
            )

        url: str = f"""{self.base_url}{self.CUSTOM_QUERY_URL}"""
        try:
            response: requests.Response = requests.post(
                url,
                data={"query": query},
                headers=self.__construct_authorization_header(json=False),
            )
            response.raise_for_status()
            return json.loads(response.content)
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

    def get_non_residential_energy_consumption_statistics(
        self,
        country: str = "",
        nuts_level: Optional[int] = None,
        nuts_code: Optional[str] = None,
        geom: Optional[Polygon] = None,
    ) -> list[NonResidentialEnergyConsumptionStatistics]:
        """Get the energy consumption statistics [MWh] for the given nuts level or nuts
        code. Only one of nuts_level and nuts_code may be specified.

        Args:
            country (str, optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to "".
            nuts_level (int | None, optional): The NUTS level, e.g. 1 for federal states
                of Germany. Defaults to None.
            nuts_code (str | None, optional): The NUTS code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions. Defaults to None.
            geom (Polygon | None, optional): A custom geometry.

        Raises:
            ValueError: If both nuts_level and nuts_code are given.
            ServerException: If an error occurrs on the server side.

        Returns:
            list[EnergyConsumptionStatistics]: A list of energy consumption statistics
                of non-residential buildings.
        """
        logging.debug(
            "ApiClient: get_energy_consumption_statistics(nuts_level=%s, nuts_code=%s)",
            nuts_level,
            nuts_code,
        )

        if nuts_level is not None and nuts_code is not None:
            raise ValueError(
                "Either nuts_level or nuts_code can be specified, not both."
            )

        if (nuts_level or nuts_code or country) and geom:
            raise ValueError(
                "You can query either by NUTS or by custom geometry, not both."
            )

        if geom is not None:
            statistics_url = (
                self.NON_RESIDENTIAL_ENERGY_CONSUMPTION_STATISTICS_BY_GEOM_URL
            )
            query_params = f"?geom={geom.wkt}"
        else:
            statistics_url = self.NON_RESIDENTIAL_ENERGY_CONSUMPTION_STATISTICS_URL
            query_params = f"?country={country}"
            if nuts_level is not None:
                query_params += f"&nuts_level={nuts_level}"
            elif nuts_code is not None:
                query_params += f"&nuts_code={nuts_code}"

        url: str = f"""{self.base_url}{statistics_url}{query_params}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

        results: list = json.loads(response.content)
        statistics: list[NonResidentialEnergyConsumptionStatistics] = []
        for res in results:
            statistic = NonResidentialEnergyConsumptionStatistics(
                nuts_code=res["nuts_code"],
                use=res["use"],
                electricity_consumption_mwh=res["electricity_consumption_MWh"],
            )
            statistics.append(statistic)
        return statistics


    def get_residential_energy_commodity_statistics(
        self,
        country: str = "",
        nuts_level: Optional[int] = None,
        nuts_code: Optional[str] = None,
        geom: Optional[Polygon] = None,
        commodity: str = "",
    ) -> list[EnergyCommodityStatistics]:
        """Get the energy commodity statistics for the given nuts level or nuts code.
        Only one of nuts_level and nuts_code may be specified.

        Args:
            country (str, optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to "".
            nuts_level (int | None, optional): The NUTS level, e.g. 1 for federal states
                of Germany. Defaults to None.
            nuts_code (str | None, optional): The NUTS code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions. Defaults to None.
            geom (Polygon | None, optional): A custom geometry.
            commodity (str, optional): The commodity for which to get statistics.
                Defaults to "".

        Raises:
            ValueError: If both nuts_level and nuts_code are given.
            ServerException: If an error occurrs on the server side.
            geom (str | None, optional): A custom geometry.

        Returns:
            list[EnergyCommodityStatistics]: A list of building commodity statistics
                of residential buildings.
        """
        logging.debug(
            """ApiClient: get_energy_commodity_statistics(nuts_level=%d, nuts_code=%s, 
            commodity=%s)""",
            nuts_level,
            nuts_code,
            commodity,
        )

        if nuts_level is not None and nuts_code is not None:
            raise ValueError(
                "Either nuts_level or nuts_code can be specified, not both."
            )

        if (nuts_level or nuts_code or country) and geom:
            raise ValueError(
                "You can query either by NUTS or by custom geometry, not both."
            )

        if geom is not None:
            statistics_url = self.RESIDENTIAL_ENERGY_COMMODITY_STATISTICS_BY_GEOM_URL
            query_params = f"?geom={geom.wkt}"
        else:
            statistics_url = self.RESIDENTIAL_ENERGY_COMMODITY_STATISTICS_URL
            query_params = f"?country={country}"
            if nuts_level is not None:
                query_params += f"&nuts_level={nuts_level}"
            elif nuts_code is not None:
                query_params += f"&nuts_code={nuts_code}"

        url: str = f"""{self.base_url}{statistics_url}{query_params}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

        results: list = json.loads(response.content)
        statistics: list[EnergyCommodityStatistics] = []
        for res in results:
         
            statistic = EnergyCommodityStatistics(
                nuts_code=res["nuts_code"],
                energy_system=res["energy_system"],
                commodity_name=res["commodity"],
                commodity_count= res["commodity_count"]
            )
            statistics.append(statistic)

        return statistics

    def get_pv_potential_statistics(
        self,
        country: str = "",
        nuts_level: Optional[int] = None,
        nuts_code: Optional[str] = None,
    ) -> list[PvPotentialStatistics]:
        """Get the PV potential statistics [kWh] for the given nuts level or
        nuts code. Only one of nuts_level and nuts_code may be specified.

        Args:
            country (str, optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to "".
            nuts_level (int | None, optional): The NUTS level, e.g. 1 for federal states
                of Germany. Defaults to None.
            nuts_code (str | None, optional): The NUTS code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions. Defaults to None.

        Raises:
            ValueError: If both nuts_level and nuts_code are specified.
            ServerException: If an unexpected error occurrs on the server side.

        Returns:
            list[BuildingStatistics]: A list of objects per NUTS region with statistical
                info about rooftop PV potentials of buildings.
        """
        if nuts_level is not None and nuts_code is not None:
            raise ValueError(
                "Either nuts_level or nuts_code can be specified, not both."
            )

        query_params = f"?country={country}"
        if nuts_level is not None:
            query_params += f"&nuts_level={nuts_level}"
        elif nuts_code is not None:
            query_params += f"&nuts_code={nuts_code}"

        url: str = f"""{self.base_url}{self.PV_GENERATION_POTENTIAL_STATISTICS_URL}{query_params}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

        results: list = json.loads(response.content)
        statistics: list[PvPotentialStatistics] = []
        for res in results:
            statistic = PvPotentialStatistics(
                nuts_code=res["nuts_code"],
                sum_pv_generation_potential_kwh=res["nuts_code"],
                avg_pv_generation_potential_residential_kwh=res["nuts_code"],
                median_pv_generation_potential_residential_kwh=res["nuts_code"],
                sum_pv_generation_potential_residential_kwh=res["nuts_code"],
                avg_pv_generation_potential_non_residential_kwh=res["nuts_code"],
                median_pv_generation_potential_non_residential_kwh=res["nuts_code"],
                sum_pv_generation_potential_non_residential_kwh=res["nuts_code"],
                avg_pv_generation_potential_mixed_kwh=res["nuts_code"],
                median_pv_generation_potential_mixed_kwh=res["nuts_code"],
                sum_pv_generation_potential_mixed_kwh=res["nuts_code"],
            )
            statistics.append(statistic)
        return statistics

def get_building_type_statistics(
        self,
        country: str = "",
        nuts_level: Optional[int] = None,
        nuts_code: Optional[str] = None,
        geom: Optional[Polygon] = None,
    ) -> list[BuildingStatistics]:
        """Get the building type statistics for the given nuts level or nuts code. Only
        one of nuts_level and nuts_code may be specified.

        Args:
            country (str, optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to "".
            nuts_level (int | None, optional): The NUTS level, e.g. 1 for federal states
                of Germany. Defaults to None.
            nuts_code (str | None, optional): The NUTS code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions. Defaults to None.
            geom (Polygon | None, optional): A custom geometry.
        Raises:
            ValueError: If both nuts_level and nuts_code are specified.
            ServerException: If an unexpected error occurrs on the server side.

        Returns:
            list[BuildingStatistics]: A list of objects per NUTS region or custom
                geometry with statistical info about building types.
        """
        if nuts_level is not None and nuts_code is not None:
            raise ValueError(
                "Either nuts_level or nuts_code can be specified, not both."
            )

        if (nuts_level or nuts_code or country) and geom:
            raise ValueError(
                "You can query either by NUTS or by custom geometry, not both."
            )

        if geom is not None:
            statistics_url = self.TYPE_STATISTICS_BY_GEOM_URL
            query_params = f"?geom={geom.wkt}"
        else:
            statistics_url = self.TYPE_STATISTICS_URL
            query_params = f"?country={country}"
            if nuts_level is not None:
                query_params += f"&nuts_level={nuts_level}"
            elif nuts_code is not None:
                query_params += f"&nuts_code={nuts_code}"

        url: str = f"""{self.base_url}{statistics_url}{query_params}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

        results: list[Dict] = json.loads(response.content)
        statistics: list[BuildingStatistics] = []
        for result in results:
            statistic = BuildingStatistics(
                nuts_code=result["nuts_code"],
                building_count_total=result["building_count_total"],
                building_count_residential=result["building_count_residential"],
                building_count_non_residential=result["building_count_non_residential"],
                building_count_mixed=result["building_count_mixed"],
            )
            statistics.append(statistic)
        return statistics

def get_non_residential_building_use_statistics(
        self,
        country: str = "",
        nuts_level: Optional[int] = None,
        nuts_code: Optional[str] = None,
        geom: Optional[Polygon] = None,
    ) -> list[BuildingUseStatistics]:
        """Get the building use statistics for the given nuts level or nuts code. Only
        one of nuts_level and nuts_code may be specified.

        Args:
            country (str, optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to "".
            nuts_level (int | None, optional): The NUTS level, e.g. 1 for federal states
                of Germany. Defaults to None.
            nuts_code (str | None, optional): The NUTS code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions. Defaults to None.
            geom (Polygon | None, optional): A custom geometry.

        Raises:
            ValueError: If both nuts_level and nuts_code are specified.
            ServerException: If an unexpected error occurrs on the server side.

        Returns:
            list[BuildingStatistics]: A list of objects per NUTS region with statistical
                info about non-residential building uses.
        """
        if nuts_level is not None and nuts_code is not None:
            raise ValueError(
                "Either nuts_level or nuts_code can be specified, not both."
            )

        if (nuts_level or nuts_code or country) and geom:
            raise ValueError(
                "You can query either by NUTS or by custom geometry, not both."
            )

        if geom is not None:
            statistics_url = self.NON_RESIDENTIAL_USE_STATISTICS_BY_GEOM_URL
            query_params = f"?geom={geom.wkt}"
        else:
            statistics_url = self.NON_RESIDENTIAL_USE_STATISTICS_URL
            query_params = f"?country={country}"
            if nuts_level is not None:
                query_params += f"&nuts_level={nuts_level}"
            elif nuts_code is not None:
                query_params += f"&nuts_code={nuts_code}"

        url: str = f"""{self.base_url}{statistics_url}{query_params}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

        results: list = json.loads(response.content)
        statistics: list[BuildingUseStatistics] = []
        for res in results:
            statistic = BuildingUseStatistics(
                nuts_code=res["nuts_code"],
                type=res["type"],
                use=res["use"],
                building_count=res["building_count"],
            )
            statistics.append(statistic)
        return statistics

def get_footprint_area_statistics(
        self,
        country: str = "",
        nuts_level: Optional[int] = None,
        nuts_code: Optional[str] = None,
        geom: Optional[Polygon] = None,
    ) -> list[FootprintAreaStatistics]:
        """Get the footprint area statistics [m2] for the given nuts level or nuts code.
        Only one of nuts_level and nuts_code may be specified.

        Args:
            country (str, optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to "".
            nuts_level (int | None, optional): The NUTS level, e.g. 1 for federal states
                of Germany. Defaults to None.
            nuts_code (str | None, optional): The NUTS code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions. Defaults to None.
            geom (Polygon | None, optional): A custom geometry.

        Raises:
            ValueError: If both nuts_level and nuts_code are specified.
            ServerException: If an unexpected error occurrs on the server side.

        Returns:
            list[BuildingStatistics]: A list of objects per NUTS region with statistical
                info about building footprint areas.
        """
        if nuts_level is not None and nuts_code is not None:
            raise ValueError(
                "Either nuts_level or nuts_code can be specified, not both."
            )

        if (nuts_level or nuts_code or country) and geom:
            raise ValueError(
                "You can query either by NUTS or by custom geometry, not both."
            )

        if geom is not None:
            statistics_url = self.FOOTPRINT_AREA_STATISTICS_BY_GEOM_URL
            query_params = f"?geom={geom.wkt}"
        else:
            statistics_url = self.FOOTPRINT_AREA_STATISTICS_URL
            query_params = f"?country={country}"
            if nuts_level is not None:
                query_params += f"&nuts_level={nuts_level}"
            elif nuts_code is not None:
                query_params += f"&nuts_code={nuts_code}"

        url: str = f"""{self.base_url}{statistics_url}{query_params}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

        results: list = json.loads(response.content)
        statistics: list[FootprintAreaStatistics] = []
        for res in results:
            statistic = FootprintAreaStatistics(
                nuts_code=res["nuts_code"],
                sum_footprint_area_total_m2=res["sum_footprint_area_total_m2"],
                avg_footprint_area_total_m2=res["avg_footprint_area_total_m2"],
                median_footprint_area_total_m2=res["median_footprint_area_total_m2"],
                sum_footprint_area_residential_m2=res[
                    "sum_footprint_area_residential_m2"
                ],
                avg_footprint_area_residential_m2=res[
                    "avg_footprint_area_residential_m2"
                ],
                median_footprint_area_residential_m2=res[
                    "median_footprint_area_residential_m2"
                ],
                sum_footprint_area_non_residential_m2=res[
                    "sum_footprint_area_non_residential_m2"
                ],
                avg_footprint_area_non_residential_m2=res[
                    "avg_footprint_area_non_residential_m2"
                ],
                median_footprint_area_non_residential_m2=res[
                    "median_footprint_area_non_residential_m2"
                ],
                sum_footprint_area_mixed_m2=res["sum_footprint_area_mixed_m2"],
                avg_footprint_area_mixed_m2=res["avg_footprint_area_mixed_m2"],
                median_footprint_area_mixed_m2=res["median_footprint_area_mixed_m2"],
            )
            statistics.append(statistic)
        return statistics

def get_height_statistics(
        self,
        country: str = "",
        nuts_level: Optional[int] = None,
        nuts_code: Optional[str] = None,
        geom: Optional[Polygon] = None,
    ) -> list[HeightStatistics]:
        """Get the height statistics [m] for the given nuts level or nuts code. Only one
        of nuts_level and nuts_code may be specified.

        Args:
            country (str, optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to "".
            nuts_level (int | None, optional): The NUTS level, e.g. 1 for federal states
                of Germany. Defaults to None.
            nuts_code (str | None, optional): The NUTS code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions. Defaults to None.
            geom (Polygon | None, optional): A custom geometry.

        Raises:
            ValueError: If both nuts_level and nuts_code are specified.
            ServerException: If an unexpected error occurrs on the server side.

        Returns:
            list[BuildingStatistics]: A list of objects per NUTS region with statistical
                info about building heights.
        """
        if nuts_level is not None and nuts_code is not None:
            raise ValueError(
                "Either nuts_level or nuts_code can be specified, not both."
            )

        if (nuts_level or nuts_code or country) and geom:
            raise ValueError(
                "You can query either by NUTS or by custom geometry, not both."
            )

        if geom is not None:
            statistics_url = self.HEIGHT_STATISTICS_BY_GEOM_URL
            query_params = f"?geom={geom.wkt}"
        else:
            statistics_url = self.HEIGHT_STATISTICS_URL
            query_params = f"?country={country}"
            if nuts_level is not None:
                query_params += f"&nuts_level={nuts_level}"
            elif nuts_code is not None:
                query_params += f"&nuts_code={nuts_code}"

        url: str = f"""{self.base_url}{statistics_url}{query_params}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

        results: list = json.loads(response.content)
        statistics: list[HeightStatistics] = []
        for res in results:
            statistic = HeightStatistics(
                nuts_code=res["nuts_code"],
                avg_height_total_m=res["avg_height_total_m"],
                median_height_total_m=res["median_height_total_m"],
                avg_height_residential_m=res["avg_height_residential_m"],
                median_height_residential_m=res["median_height_residential_m"],
                avg_height_non_residential_m=res["avg_height_non_residential_m"],
                median_height_non_residential_m=res["median_height_non_residential_m"],
                avg_height_mixed_m=res["avg_height_mixed_m"],
                median_height_mixed_m=res["median_height_mixed_m"],
            )
            statistics.append(statistic)
        return statistics

def get_residential_heat_demand_statistics(
        self,
        country: str = "",
        nuts_level: Optional[int] = None,
        nuts_code: Optional[str] = None,
        geom: Optional[Polygon] = None,
    ) -> list[HeatDemandStatistics]:
        """Get the residential heat demand statistics [MWh] for the given NUTS level or
            NUTS/LAU code. Results can be limited to a certain country by setting the
            country parameter. Only one of nuts_level and nuts_code may be specified.

        Args:
            country (str, optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to "".
            nuts_level (int | None, optional): The NUTS level, e.g. 1 for federal states
                of Germany. Defaults to None.
            nuts_code (str | None, optional): The NUTS code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions. Defaults to None.
            geom (Polygon | None, optional): A custom geometry.

        Raises:
            ValueError: If both nuts_level and nuts_code are specified or the nuts_level
                is invalid.
            ServerException: If an unexpected error occurrs on the server side.

        Returns:
            list[HeatDemandStatistics]: A list of objects per NUTS/LAU region with
                statistical info about heat demand [MWh].
        """
        if nuts_level is not None and nuts_code is not None:
            raise ValueError(
                "Either nuts_level or nuts_code can be specified, not both."
            )
        if nuts_level is not None and nuts_level not in range(0, 5):
            raise ValueError(
                "Invalid NUTS/LAU level provided; nuts_level must be in range [0,4]."
            )

        if (nuts_level or nuts_code or country) and geom:
            raise ValueError(
                "You can query either by NUTS or by custom geometry, not both."
            )

        if geom is not None:
            statistics_url = self.RESIDENTIAL_HEAT_DEMAND_STATISTICS_BY_GEOM_URL
            query_params = f"?geom={geom.wkt}"
        else:
            statistics_url = self.RESIDENTIAL_HEAT_DEMAND_STATISTICS_URL
            query_params = f"?country={country}"
            if nuts_level is not None:
                query_params += f"&nuts_level={nuts_level}"
            elif nuts_code is not None:
                query_params += f"&nuts_code={nuts_code}"

        url: str = f"""{self.base_url}{statistics_url}{query_params}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600, headers=self.__construct_authorization_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

        results: list = json.loads(response.content)
        statistics: list[HeatDemandStatistics] = []
        for res in results:
            statistic = HeatDemandStatistics(
                nuts_code=res["nuts_code"],
                yearly_heat_demand_mwh=res["yearly_heat_demand_mwh"],
            )
            statistics.append(statistic)
        return statistics