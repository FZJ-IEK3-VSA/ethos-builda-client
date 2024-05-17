import json
import logging
from typing import Dict, Optional

import requests
from shapely.geometry import Polygon
from builda_client.base_client import BaseClient
from builda_client.util import load_config
from builda_client.model import (
    Address,
    AddressSource,
    BuildingResponseDto,
    BuildingWithSourceDto,
    CoordinatesSource,
    FloatSource,
    HeatDemandStatisticsByBuildingCharacteristics,
    IntSource,
    LineageResponseDto,
    NonResidentialBuildingResponseDto,
    ResidentialBuildingResponseDto,
    SourceResponseDto,
    NonResidentialBuildingWithSourceDto,
    ResidentialBuildingWithSourceDto,
    SizeClassStatistics,
    BuildingStatistics,
    BuildingUseStatistics,
    ConstructionYearStatistics,
    Coordinates,
    FootprintAreaStatistics,
    HeatDemandStatistics,
    HeightStatistics,
    RefurbishmentStateStatistics,
    StringSource,
)
from builda_client.util import determine_nuts_query_param


class BuildaClient(BaseClient):

    # Buildings
    BUILDINGS_URL = "buildings"
    RESIDENTIAL_BUILDINGS_URL = "buildings/residential"
    NON_RESIDENTIAL_BUILDINGS_URL = "buildings/non-residential"
    BUILDINGS_GEOMETRY_URL = "buildings-geometry"

    # Statistics
    TYPE_STATISTICS_URL = "statistics/building-type"
    CONSTRUCTION_YEAR_STATISTICS_URL = "statistics/construction-year"
    FOOTPRINT_AREA_STATISTICS_URL = "statistics/footprint-area"
    HEIGHT_STATISTICS_URL = "statistics/height"
    CONSTRUCTION_YEAR_STATISTICS_URL = "statistics/construction-year"
    REFURBISHMENT_STATE_STATISTICS_URL = "statistics/refurbishment-state"
    NON_RESIDENTIAL_USE_STATISTICS_URL = "statistics/non-residential/building-use"

    RESIDENTIAL_SIZE_CLASS_STATISTICS_URL = "statistics/residential/size-class"
    RESIDENTIAL_HEAT_DEMAND_STATISTICS_URL = "statistics/residential/heat-demand"
    RESIDENTIAL_HEAT_DEMAND_STATISTICS_BY_BUILDING_CHARACTERISTICS_URL = (
        "statistics/residential/heat-demand-by-building-characteristics"
    )

    REFURBISHMENT_STATE_URL = "refurbishment-state"


    def __init__(
        self,
    ):
        """This is the API client for the building database ETHOS.BUILDA by
        Forschungszentrum Jülich - Jülich Systems Analysis (IEK-3) available
        at https://ethos-builda.fz-juelich.de.
        """
        self.config = load_config()
        self.BASE_URL = self.config["production"]["api_address"] + self.config["base_url"]
        logging.basicConfig(level=logging.WARN)

        requests_log = logging.getLogger("urllib3")
        requests_log.setLevel(logging.WARN)
        requests_log.propagate = True


    def get_buildings(
        self,
        building_type: Optional[str] = "",
        street: str = "",
        housenumber: str = "",
        postcode: str = "",
        city: str = "",
        nuts_code: str = "",
    ) -> BuildingResponseDto:
        """Gets all buildings that match the query parameters with the basic attributes
        common to all building use types.
        
        Args:
            building_type (str): The type of building ('residential', 'non-residential', 'mixed').
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
            list[BuildingResponseDto]: A list of buildings with attribute sources and lineages.
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
        nuts_query_param: str = determine_nuts_query_param(nuts_code)

        type_is_null = "False"
        if building_type is None:
            type_is_null = "True"
            building_type = ""
        elif building_type == '':
            type_is_null = ""

        url: str = f"""{self.BASE_URL}{self.BUILDINGS_URL}?street={street}&house_number={housenumber}&postcode={postcode}&city={city}&{nuts_query_param}={nuts_code}&type={building_type}&type__isnull={type_is_null}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600)
            logging.debug("ApiClient: received response. Checking for errors.")
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

        logging.debug(
            "ApiClient: received ok response, proceeding with deserialization."
        )
        results: Dict = json.loads(response.content)
        buildings: list[BuildingWithSourceDto] = []
        for result in results["buildings"]:
            coordinates = CoordinatesSource(
                value = Coordinates(
                    latitude=result["coordinates"]["value"]["latitude"],
                    longitude=result["coordinates"]["value"]["longitude"]
                ),
                source = result["coordinates"]["source"],
                lineage = result["coordinates"]["lineage"],
            )
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

            building = BuildingWithSourceDto(
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
                type=StringSource(
                    value=result["type"]["value"], 
                    source=result["type"]["source"],
                    lineage=result["type"]["lineage"],
                    ),
                roof_shape=StringSource(
                    value=result["roof_shape"]["value"], 
                    source=result["roof_shape"]["source"],
                    lineage=result["roof_shape"]["lineage"],
                    ),
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

        lineages: list[LineageResponseDto] = []
        for entry in results["lineages"]:
            lineage = LineageResponseDto(
                key=entry["key"],
                description=entry["description"],
            )
            
            lineages.append(lineage)

        return BuildingResponseDto(
            buildings=buildings, 
            sources=data_sources, 
            lineages=lineages)

    
    def get_residential_buildings(
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
            street (str, optional): The name of the street.
            housenumber (str, optional): The house number.
            postcode (str, optional): The postcode.
            city (str, optional): The city.
            nuts_code (str, optional): The NUTS-code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions or 2019 LAU definition.
            include_mixed (bool, optional): Whether or not to include mixed buildings.
                Defaults to True.

        Raises:
            ServerException: When an error occurs on the server side..

        Returns:
            list[ResidentialBuildingResponseDto]: A list of residential buildings.
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

        url: str = f"""{self.BASE_URL}{self.RESIDENTIAL_BUILDINGS_URL}?street={street}&house_number={housenumber}&postcode={postcode}&city={city}&{nuts_query_param}={nuts_code}&type={building_type}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600)
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
    ) -> NonResidentialBuildingResponseDto:
        """Gets all non-residential buildings that match the query parameters.

        Args:
            street (str, optional): The name of the street.
            housenumber (str, optional): The house number.
            postcode (str, optional): The postcode.
            city (str, optional): The city.
            nuts_code (str, optional): The NUTS-code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions or 2019 LAU definition.
            include_mixed (bool, optional): Whether or not to include mixed buildings.
                Defaults to True.
            exclude_auxiliary (bool, optional): Whether to exclude auxiliary buildings.
                Defaults to False.

        Raises:
            ServerException: When an error occurs on the server side..

        Returns:
            list[NonResidentialBuildingResponseDto]: A list of non-residential buildings.
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
        nuts_query_param: str = determine_nuts_query_param(nuts_code)
        building_type = "" if include_mixed else "non-residential"

        url: str = f"""{self.BASE_URL}{self.NON_RESIDENTIAL_BUILDINGS_URL}?street={street}&house_number={housenumber}&postcode={postcode}&city={city}&{nuts_query_param}={nuts_code}&type={building_type}&exclude_auxiliary={exclude_auxiliary}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600)
            logging.debug("ApiClient: received response. Checking for errors.")
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

        logging.debug(
            "ApiClient: received ok response, proceeding with deserialization."
        )
        results: Dict = json.loads(response.content)
        buildings: list[NonResidentialBuildingWithSourceDto] = []
        for result in results["buildings"]:
            coordinates = CoordinatesSource(
                value = Coordinates(
                    latitude=result["coordinates"]["value"]["latitude"],
                    longitude=result["coordinates"]["value"]["longitude"]
                ),
                source = result["coordinates"]["source"],
                lineage = result["coordinates"]["lineage"],
            )
            building = NonResidentialBuildingWithSourceDto(
                id=result["id"],
                coordinates=coordinates,
                address=result["address"],
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
                type=StringSource(
                    value=result["type"]["value"], 
                    source=result["type"]["source"],
                    lineage=result["type"]["lineage"],
                    ),
                roof_shape=StringSource(
                    value=result["roof_shape"]["value"], 
                    source=result["roof_shape"]["source"],
                    lineage=result["roof_shape"]["lineage"],
                    ),
                use=StringSource(
                    value=result["use"]["value"], 
                    source=result["use"]["source"],
                    lineage=result["use"]["lineage"],
                    ),
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

        return NonResidentialBuildingResponseDto(
            buildings=buildings, 
            sources=data_sources, 
            lineages=data_lineages)
    

    def get_building_type_statistics(
        self,
        country: str = "",
        nuts_level: Optional[int] = None,
        nuts_code: Optional[str] = None,
    ) -> list[BuildingStatistics]:
        """Get the building type statistics for the given NUTS level or NUTS code. Only
        one of nuts_level and nuts_code may be specified.

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
            list[BuildingStatistics]: A list of objects per NUTS region or custom
                geometry with statistical info about building types.
        """
        if nuts_level is not None and nuts_code is not None:
            raise ValueError(
                "Either nuts_level or nuts_code can be specified, not both."
            )

        statistics_url = self.TYPE_STATISTICS_URL
        query_params = f"?country={country}"
        if nuts_level is not None:
            query_params += f"&nuts_level={nuts_level}"
        elif nuts_code is not None:
            query_params += f"&nuts_code={nuts_code}"

        url: str = f"""{self.BASE_URL}{statistics_url}{query_params}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600)
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
    ) -> list[BuildingUseStatistics]:
        """Get the building use statistics of non-residential buildings for the given 
        NUTS level or NUTS code. Only one of nuts_level and nuts_code may be specified.

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
                info about non-residential building uses.
        """
        if nuts_level is not None and nuts_code is not None:
            raise ValueError(
                "Either nuts_level or nuts_code can be specified, not both."
            )

        statistics_url = self.NON_RESIDENTIAL_USE_STATISTICS_URL
        query_params = f"?country={country}"
        if nuts_level is not None:
            query_params += f"&nuts_level={nuts_level}"
        elif nuts_code is not None:
            query_params += f"&nuts_code={nuts_code}"

        url: str = f"""{self.BASE_URL}{statistics_url}{query_params}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600)
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

    def get_residential_size_class_statistics(
        self,
        country: str = "",
        nuts_level: int | None = None,
        nuts_code: str | None = None,
    ) -> list[SizeClassStatistics]:
        """Get the building class statistics for the given NUTS level or NUTS code. Only
            one of nuts_level and nuts_code may be specified.

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
            list[SizeClassStatistics]: A list of objects per NUTS region with
                statistical info about residential building size classes.
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

        url: str = f"""{self.BASE_URL}{self.RESIDENTIAL_SIZE_CLASS_STATISTICS_URL}{query_params}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

        results: list = json.loads(response.content)
        statistics: list[SizeClassStatistics] = []
        for res in results:
            statistic = SizeClassStatistics(
                nuts_code=res["nuts_code"],
                sfh_count=res["count_sfh"],
                th_count=res["count_th"],
                mfh_count=res["count_mfh"],
                ab_count=res["count_ab"],
            )
            statistics.append(statistic)
        return statistics

    def get_residential_construction_year_statistics(
        self,
        country: str = "",
        nuts_level: int | None = None,
        nuts_code: str | None = None,
    ) -> list[ConstructionYearStatistics]:
        """Get the construction year statistics for the given NUTS level or NUTS code.
        Only one of nuts_level and nuts_code may be specified.

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
            list[ConstructionYearStatistics]: A list of objects per NUTS region with
                statistical info about residential building construction years.
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

        url: str = (
            f"""{self.BASE_URL}{self.CONSTRUCTION_YEAR_STATISTICS_URL}{query_params}"""
        )
        try:
            response: requests.Response = requests.get(url, timeout=3600)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

        results: list = json.loads(response.content)
        statistics: list[ConstructionYearStatistics] = []
        for res in results:
            statistic = ConstructionYearStatistics(
                nuts_code=res["nuts_code"],
                avg_construction_year=res["avg_construction_year"],
            )
            statistics.append(statistic)
        return statistics

    def get_footprint_area_statistics(
        self,
        country: str = "",
        nuts_level: Optional[int] = None,
        nuts_code: Optional[str] = None,
    ) -> list[FootprintAreaStatistics]:
        """Get the footprint area statistics [m2] for the given NUTS level or NUTS code.
        Only one of nuts_level and nuts_code may be specified.

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
                info about building footprint areas.
        """
        if nuts_level is not None and nuts_code is not None:
            raise ValueError(
                "Either nuts_level or nuts_code can be specified, not both."
            )

        statistics_url = self.FOOTPRINT_AREA_STATISTICS_URL
        query_params = f"?country={country}"
        if nuts_level is not None:
            query_params += f"&nuts_level={nuts_level}"
        elif nuts_code is not None:
            query_params += f"&nuts_code={nuts_code}"

        url: str = f"""{self.BASE_URL}{statistics_url}{query_params}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600)
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
    ) -> list[HeightStatistics]:
        """Get the height statistics [m] for the given NUTS level or NUTS code. Only one
        of nuts_level and nuts_code may be specified.

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
                info about building heights.
        """
        if nuts_level is not None and nuts_code is not None:
            raise ValueError(
                "Either nuts_level or nuts_code can be specified, not both."
            )

        statistics_url = self.HEIGHT_STATISTICS_URL
        query_params = f"?country={country}"
        if nuts_level is not None:
            query_params += f"&nuts_level={nuts_level}"
        elif nuts_code is not None:
            query_params += f"&nuts_code={nuts_code}"

        url: str = f"""{self.BASE_URL}{statistics_url}{query_params}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600)
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

    def get_refurbishment_state_statistics(
        self,
        country: str = "",
        nuts_level: Optional[int] = None,
        nuts_code: Optional[str] = None,
        geom: Optional[Polygon] = None,
    ) -> list[RefurbishmentStateStatistics]:
        """Get the refurbishment state statistics [m2] for the given NUTS level or NUTS 
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
            ValueError: If both nuts_level and nuts_code are specified.
            ServerException: If an unexpected error occurrs on the server side.

        Returns:
            list[RefurbishmentStateStatistics]: A list of objects per NUTS region with 
            statistical info about refurbishment state of buildings.
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
            statistics_url = self.REFURBISHMENT_STATE_STATISTICS_URL
            query_params = f"?geom={geom.wkt}"
        else:
            statistics_url = self.REFURBISHMENT_STATE_STATISTICS_URL
            query_params = f"?country={country}"
            if nuts_level is not None:
                query_params += f"&nuts_level={nuts_level}"
            elif nuts_code is not None:
                query_params += f"&nuts_code={nuts_code}"

        url: str = f"""{self.BASE_URL}{statistics_url}{query_params}"""
        try:
            response: requests.Response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

        results: list = json.loads(response.content)
        statistics: list[RefurbishmentStateStatistics] = []
        for res in results:
            statistic = RefurbishmentStateStatistics(
                nuts_code=res["nuts_code"],
                sum_1_refurbishment_state=res["sum_1_refurbishment_state"],
                sum_2_refurbishment_state=res["sum_2_refurbishment_state"],
                sum_3_refurbishment_state=res["sum_3_refurbishment_state"],
            )
            statistics.append(statistic)
        return statistics

    def get_residential_heat_demand_statistics(
        self,
        country: str = "",
        nuts_level: Optional[int] = None,
        nuts_code: Optional[str] = None,
    ) -> list[HeatDemandStatistics]:
        """Get the residential heat demand statistics [MWh] for the given NUTS level or
            NUTS code. Results can be limited to a certain country by setting the
            country parameter. Only one of nuts_level and nuts_code may be specified.

        Args:
            country (str, optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to "".
            nuts_level (int | None, optional): The NUTS level, e.g. 1 for federal states
                of Germany. Defaults to None.
            nuts_code (str | None, optional): The NUTS code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions. Defaults to None.

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

        statistics_url = self.RESIDENTIAL_HEAT_DEMAND_STATISTICS_URL
        query_params = f"?country={country}"
        if nuts_level is not None:
            query_params += f"&nuts_level={nuts_level}"
        elif nuts_code is not None:
            query_params += f"&nuts_code={nuts_code}"

        url: str = f"""{self.BASE_URL}{statistics_url}{query_params}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600)
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

    def get_residential_heat_demand_statistics_by_building_info(
        self,
        country: str = "",
        construction_year: Optional[int] = None,
        construction_year_before: Optional[int] = None,
        construction_year_after: Optional[int] = None,
        size_class: Optional[str] = "",
        refurbishment_state: Optional[str] = "",
    ) -> list[HeatDemandStatisticsByBuildingCharacteristics]:
        """Get the residential heat demand statistics [MWh] for the given country
        and combination of building characteristics.

        Args:
            country (str, optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to "".
            construcion_year (int | None, optional): Construction year of
                building should be exactly the given year.        
            construcion_year_before (int | None, optional): Construction year of
                building should be before given year.
            construcion_year_after (int | None, optional): Construction year of
                building should be after given year.
            size_class (str | "", optional): Size class of building (SFH, MFH, TH, AB).
            refurbishment_state (str | None, optional): Refurbishment state of building 
                (1=existing state, 2=usual refurbishment, 3=advanced refurbishment).
           
        Raises:
            ServerException: If an unexpected error occurrs on the server side.

        Returns:
            list[HeatDemandStatisticsByBuildingCharacteristics]: A list of objects per
                building parameter combination with statistical info about heat demand 
                [MWh].
        """
        if construction_year and (construction_year_before or construction_year_after):
            raise ValueError("You cannot query for an exact construction year and a range at the same time.")
        
        statistics_url = self.RESIDENTIAL_HEAT_DEMAND_STATISTICS_BY_BUILDING_CHARACTERISTICS_URL
        
        construction_year_after_param = str(construction_year_after) if construction_year_after else ""
        construction_year_before_param = str(construction_year_before) if construction_year_before else ""
        construction_year_param = str(construction_year) if construction_year else ""

        query_params = f"?country={country}&construction_year__gt={construction_year_after_param}&construction_year={construction_year_param}&construction_year__lt={construction_year_before_param}&size_class={size_class}&refurbishment_state={refurbishment_state}"

        url: str = f"""{self.BASE_URL}{statistics_url}{query_params}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.handle_exception(err)

        results: list = json.loads(response.content)
        statistics: list[HeatDemandStatisticsByBuildingCharacteristics] = []
        for res in results:
            statistic = HeatDemandStatisticsByBuildingCharacteristics(
                country=res["country"],
                construction_year=res["construction_year"],
                size_class=res["size_class"],
                refurbishment_state=res["refurbishment_state"],
                yearly_heat_demand_mwh=res["yearly_heat_demand_mwh"],
            )
            statistics.append(statistic)
        return statistics

