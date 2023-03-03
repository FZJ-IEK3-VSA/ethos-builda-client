import json
import logging
from typing import Dict, Optional
from uuid import UUID

import requests
from shapely.geometry import Polygon

from builda_client.exceptions import ServerException
from builda_client.model import (
    Address,
    Building,
    HeatDemandStatisticsByBuildingCharacteristics,
    SizeClassStatistics,
    BuildingStatistics,
    BuildingUseStatistics,
    CommodityCount,
    ConstructionYearStatistics,
    Coordinates,
    DataSource,
    EnergyCommodityStatistics,
    FootprintAreaStatistics,
    HeatDemandStatistics,
    HeightStatistics,
    MetadataResponseDto,
    NonResidentialBuilding,
    NonResidentialEnergyConsumptionStatistics,
    PvGenerationPotentialStatistics,
    RefurbishmentStateStatistics,
    ResidentialBuilding,
    ResidentialEnergyConsumptionStatistics,
)
from builda_client.util import determine_nuts_query_param, load_config


class BuildaClient:

    # Buildings
    BUILDINGS_URL = "buildings"
    BUILDINGS_SOURCES_URL = "buildings/{id}/sources"
    RESIDENTIAL_BUILDINGS_URL = "buildings/residential"
    NON_RESIDENTIAL_BUILDINGS_URL = "buildings/non-residential"

    # Statistics
    TYPE_STATISTICS_URL = "statistics/building-type"
    TYPE_STATISTICS_BY_GEOM_URL = "statistics/building-type/geom"
    CONSTRUCTION_YEAR_STATISTICS_URL = "statistics/construction-year"
    FOOTPRINT_AREA_STATISTICS_URL = "statistics/footprint-area"
    FOOTPRINT_AREA_STATISTICS_BY_GEOM_URL = "statistics/footprint-area/geom"
    HEIGHT_STATISTICS_URL = "statistics/height"
    HEIGHT_STATISTICS_BY_GEOM_URL = "statistics/height/geom"
    CONSTRUCTION_YEAR_STATISTICS_URL = "statistics/construction-year"
    REFURBISHMENT_STATE_STATISTICS_URL = "statistics/refurbishment-state"
    PV_GENERATION_POTENTIAL_STATISTICS_URL = "statistics/pv-generation-potential"

    NON_RESIDENTIAL_USE_STATISTICS_URL = "statistics/non-residential/building-use"
    NON_RESIDENTIAL_USE_STATISTICS_BY_GEOM_URL = (
        "statistics/non-residential/building-use/geom"
    )
    NON_RESIDENTIAL_ENERGY_CONSUMPTION_STATISTICS_URL = (
        "statistics/non-residential/energy-consumption"
    )
    NON_RESIDENTIAL_ENERGY_CONSUMPTION_STATISTICS_BY_GEOM_URL = (
        "statistics/non-residential/energy-consumption/geom"
    )

    RESIDENTIAL_SIZE_CLASS_STATISTICS_URL = "statistics/residential/size-class"
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
    RESIDENTIAL_HEAT_DEMAND_STATISTICS_URL = "statistics/residential/heat-demand"
    RESIDENTIAL_HEAT_DEMAND_STATISTICS_BY_BUILDING_CHARACTERISTICS_URL = (
        "statistics/residential/heat-demand-by-building-characteristics"
    )
    RESIDENTIAL_HEAT_DEMAND_STATISTICS_BY_GEOM_URL = (
        "statistics/residential/heat-demand/geom"
    )
    REFURBISHMENT_STATE_URL = "refurbishment-state"

    base_url: str

    def __init__(
        self,
        proxy: bool = False,
    ):
        """Constructor.

        Args:
            proxy (bool, optional): Whether to use a proxy or not. Proxy should be used
                when using client on cluster compute nodes. Defaults to False.
        """
        logging.basicConfig(level=logging.WARN)

        self.phase: str = "staging"
        requests_log = logging.getLogger("urllib3")
        requests_log.setLevel(logging.WARN)
        requests_log.propagate = True

        self.config = load_config()
        if proxy:
            host = self.config["proxy"]["host"]
            port = self.config["proxy"]["port"]
        else:
            host = self.config[self.phase]["api"]["host"]
            port = self.config[self.phase]["api"]["port"]

        self.base_url = f"""http://{host}:{port}{self.config['base_url']}"""

    def get_buildings(
        self,
        building_type: Optional[str] = "",
        street: str = "",
        housenumber: str = "",
        postcode: str = "",
        city: str = "",
        nuts_code: str = "",
        exclude_irrelevant: bool = False,
    ) -> list[Building]:
        """Gets all buildings that match the query parameters.
        Args:
            street (str | None, optional): The name of the street. Defaults to None.
            housenumber (str | None, optional): The house number. Defaults to None.
            postcode (str | None, optional): The postcode. Defaults to None.
            city (str | None, optional): The city. Defaults to None.
            nuts_code (str | None, optional): The NUTS-code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions or 2019 LAU definition.
                Defaults to None.
            type (str | None, optional): The type of building ('residential',
                'non-residential').
                If not provided the common attributes for residential and
                non-residential buildings are returned.

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
        nuts_query_param: str = determine_nuts_query_param(nuts_code)

        type_is_null = False
        if building_type is None:
            type_is_null = True
            building_type = ""

        url: str = f"""{self.base_url}{self.BUILDINGS_URL}?street={street}&house_number={housenumber}&postcode={postcode}&city={city}&{nuts_query_param}={nuts_code}&type={building_type}&type__isnull={type_is_null}&type__isnull={type_is_null}&exclude_irrelevant={exclude_irrelevant}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600)
            logging.debug("ApiClient: received response. Checking for errors.")
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException("An unexpected exception occurred.") from e

        logging.debug(
            "ApiClient: received ok response, proceeding with deserialization."
        )
        results: list[Dict] = json.loads(response.content)
        buildings: list[Building] = []
        for result in results:
            address = Address(
                street=result["street"],
                house_number=result["house_number"],
                postcode=result["postcode"],
                city=result["city"],
            )
            coordinates = Coordinates(
                latitude=result["coordinates"]["latitude"],
                longitude=result["coordinates"]["longitude"],
            )
            building = Building(
                id=result["id"],
                coordinates=coordinates,
                address=address,
                footprint_area_m2=result["footprint_area_m2"],
                height_m=result["height_m"],
                type=result["type"],
                construction_year=result["construction_year"],
                use=result["use"],
                pv_generation_potential_kwh=result["pv_generation_potential_kWh"],
            )
            buildings.append(building)

        return buildings

    def get_building_sources(self, building_id: UUID) -> list[DataSource]:
        """Gets sources for building.
        Args:
            building_id (UUID): ID of building

        Raises:
            ServerException: When an error occurs on the server side..

        Returns:
            list[DataSource]: A list of sources.
        """

        logging.debug(
            """ApiClient: get_building_sources(building_id=%s)""", building_id
        )

        url: str = f"""{self.base_url}{self.BUILDINGS_SOURCES_URL.replace('{id}', str(building_id))}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600)
            logging.debug("ApiClient: received response. Checking for errors.")
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException("An unexpected exception occurred.") from e

        logging.debug(
            "ApiClient: received ok response, proceeding with deserialization."
        )
        result: list[Dict] = json.loads(response.content)
        data_sources: list[DataSource] = []
        for entry in result["sources"]:
            metadata = MetadataResponseDto(
                name=entry["source"]["name"],
                provider=entry["source"]["provider"],
                refering_website_link=entry["source"]["refering_website_link"],
                license=entry["source"]["license"],
                citation=entry["source"]["citation"],
            )
            data_source = DataSource(
                attribute=entry["attribute"], lineage=entry["lineage"], source=metadata
            )
            data_sources.append(data_source)

        return data_sources

    def get_residential_buildings(
        self,
        street: str = "",
        housenumber: str = "",
        postcode: str = "",
        city: str = "",
        nuts_code: str = "",
        include_mixed: bool = True,
        exclude_irrelevant: bool = False,
    ) -> list[ResidentialBuilding]:
        """Gets all buildings that match the query parameters.
        Args:
            street (str | None, optional): The name of the street. Defaults to None.
            housenumber (str | None, optional): The house number. Defaults to None.
            postcode (str | None, optional): The postcode. Defaults to None.
            city (str | None, optional): The city. Defaults to None.
            nuts_code (str | None, optional): The NUTS-code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions or 2019 LAU definition.
                Defaults to None.

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

        url: str = f"""{self.base_url}{self.RESIDENTIAL_BUILDINGS_URL}?street={street}&house_number={housenumber}&postcode={postcode}&city={city}&{nuts_query_param}={nuts_code}&type={building_type}&exclude_irrelevant={exclude_irrelevant}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600)
            logging.debug("ApiClient: received response. Checking for errors.")
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException("An unexpected exception occurred.") from e

        logging.debug(
            "ApiClient: received ok response, proceeding with deserialization."
        )
        results: list[Dict] = json.loads(response.content)
        buildings: list[ResidentialBuilding] = []
        for result in results:
            address = Address(
                street=result["street"],
                house_number=result["house_number"],
                postcode=result["postcode"],
                city=result["city"],
            )
            coordinates = Coordinates(
                latitude=result["coordinates"]["latitude"],
                longitude=result["coordinates"]["longitude"],
            )

            building = ResidentialBuilding(
                id=result["id"],
                coordinates=coordinates,
                address=address,
                footprint_area_m2=result["footprint_area_m2"],
                height_m=result["height_m"],
                type=result["type"],
                construction_year=result["construction_year"],
                pv_generation_potential_kwh=result["pv_generation_potential_kWh"],
                use=result["use"],
                size_class=result["size_class"],
                refurbishment_state=result["refurbishment_state"],
                tabula_type=result["tabula_type"],
                living_area=result["living_area_m2"],
                heat_demand_mwh=result["heat_demand_MWh"],
                household_count=result["household_count"],
                heating_commodity=result["heating_commodity"],
                cooling_commodity=result["heating_commodity"],
                water_heating_commodity=result["heating_commodity"],
                cooking_commodity=result["heating_commodity"],
                solids_consumption_mwh=result["solids_consumption_MWh"],
                lpg_consumption_mwh=result["lpg_consumption_MWh"],
                gas_diesel_oil_consumption_mwh=result["gas_diesel_oil_consumption_MWh"],
                gas_consumption_mwh=result["gas_consumption_MWh"],
                biomass_consumption_mwh=result["biomass_consumption_MWh"],
                geothermal_consumption_mwh=result["geothermal_consumption_MWh"],
                derived_heat_consumption_mwh=result["derived_heat_consumption_MWh"],
                electricity_consumption_mwh=result["electricity_consumption_MWh"],
            )
            buildings.append(building)

        return buildings

    def get_non_residential_buildings(
        self,
        street: str = "",
        housenumber: str = "",
        postcode: str = "",
        city: str = "",
        nuts_code: str = "",
        include_mixed: bool = True,
        exclude_irrelevant: bool = False,
    ) -> list[NonResidentialBuilding]:
        """Gets all buildings that match the query parameters.
        Args:
            street (str | None, optional): The name of the street. Defaults to None.
            housenumber (str | None, optional): The house number. Defaults to None.
            postcode (str | None, optional): The postcode. Defaults to None.
            city (str | None, optional): The city. Defaults to None.
            nuts_code (str | None, optional): The NUTS-code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions or 2019 LAU definition.
                Defaults to None.

        Raises:
            ServerException: When an error occurs on the server side..

        Returns:
            list[NonResidentialBuilding]: A list of non-residential buildings.
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
        building_type = "" if include_mixed else "non-residential"

        url: str = f"""{self.base_url}{self.NON_RESIDENTIAL_BUILDINGS_URL}?street={street}&house_number={housenumber}&postcode={postcode}&city={city}&{nuts_query_param}={nuts_code}&type={building_type}&exclude_irrelevant={exclude_irrelevant}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600)
            logging.debug("ApiClient: received response. Checking for errors.")
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException("An unexpected exception occurred.") from e

        logging.debug(
            "ApiClient: received ok response, proceeding with deserialization."
        )
        results: list[Dict] = json.loads(response.content)
        buildings: list[NonResidentialBuilding] = []
        for result in results:
            address = Address(
                street=result["street"],
                house_number=result["house_number"],
                postcode=result["postcode"],
                city=result["city"],
            )
            coordinates = Coordinates(
                latitude=result["coordinates"]["latitude"],
                longitude=result["coordinates"]["longitude"],
            )
            building = NonResidentialBuilding(
                id=result["id"],
                coordinates=coordinates,
                address=address,
                footprint_area_m2=result["footprint_area_m2"],
                height_m=result["height_m"],
                type=result["type"],
                construction_year=result["construction_year"],
                use=result["use"],
                pv_generation_potential_kwh=result["pv_generation_potential_kWh"],
                electricity_consumption_mwh=result["electricity_consumption_MWh"],
            )
            buildings.append(building)
        return buildings

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
            country (str | None, optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to None.
            nuts_level (int | None, optional): The NUTS level. Defaults to None.
            nuts_code (str | None, optional): The NUTS code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions. Defaults to None.
            geom (str | None, optional): A custom geometry.
        Raises:
            ValueError: If both nuts_level and nuts_code are specified.
            ServerException: If an unexpected error occurrs on the server side.

        Returns:
            list[BuildingStatistics]: A list of objects per NUTS region or custom
                geometry with statistical info about buildings.
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
            response: requests.Response = requests.get(url, timeout=3600)
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException("An unexpected exception occurred.") from e

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
            country (str | None, optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to None.
            nuts_level (int | None, optional): The NUTS level. Defaults to None.
            nuts_code (str | None, optional): The NUTS code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions. Defaults to None.
            geom (str | None, optional): A custom geometry.

        Raises:
            ValueError: If both nuts_level and nuts_code are specified.
            ServerException: If an unexpected error occurrs on the server side.

        Returns:
            list[BuildingStatistics]: A list of objects per NUTS region with statistical
                info about buildings.
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
            response: requests.Response = requests.get(url, timeout=3600)
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException("An unexpected exception occurred.") from e

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
        """Get the building class statistics for the given nuts level or nuts code. Only
        one of nuts_level and nuts_code may be specified.

        Args:
            country (str | None, optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to None.
            nuts_level (int | None, optional): The NUTS level. Defaults to None.
            nuts_code (str | None, optional): The NUTS code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions. Defaults to None.

        Raises:
            ValueError: If both nuts_level and nuts_code are specified.
            ServerException: If an unexpected error occurrs on the server side.

        Returns:
            list[SizeClassStatistics]: A list of objects per NUTS region with
                statistical info about buildings.
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

        url: str = f"""{self.base_url}{self.RESIDENTIAL_SIZE_CLASS_STATISTICS_URL}{query_params}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600)
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException("An unexpected exception occurred.") from e

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

    def get_construction_year_statistics(
        self,
        country: str = "",
        nuts_level: int | None = None,
        nuts_code: str | None = None,
    ) -> list[ConstructionYearStatistics]:
        """Get the construction year statistics for the given nuts level or nuts code.
        Only one of nuts_level and nuts_code may be specified.

        Args:
            country (str | None, optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to None.
            nuts_level (int | None, optional): The NUTS level. Defaults to None.
            nuts_code (str | None, optional): The NUTS code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions. Defaults to None.

        Raises:
            ValueError: If both nuts_level and nuts_code are specified.
            ServerException: If an unexpected error occurrs on the server side.

        Returns:
            list[ConstructionYearStatistics]: A list of objects per NUTS region with
                statistical info about buildings.
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
            f"""{self.base_url}{self.CONSTRUCTION_YEAR_STATISTICS_URL}{query_params}"""
        )
        try:
            response: requests.Response = requests.get(url, timeout=3600)
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException("An unexpected exception occurred.") from e

        results: list = json.loads(response.content)
        statistics: list[ConstructionYearStatistics] = []
        for res in results:
            statistic = ConstructionYearStatistics(
                nuts_code=res["nuts_code"],
                avg_construction_year=res["avg_construction_year"],
                avg_construction_year_residential=res[
                    "avg_construction_year_residential"
                ],
                avg_construction_year_non_residential=res[
                    "avg_construction_year_non_residential"
                ],
                avg_construction_year_mixed=res["avg_construction_year_mixed"],
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
            country (str | None, optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to None.
            nuts_level (int | None, optional): The NUTS level. Defaults to None.
            nuts_code (str | None, optional): The NUTS code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions. Defaults to None.
            geom (str | None, optional): A custom geometry.

        Raises:
            ValueError: If both nuts_level and nuts_code are specified.
            ServerException: If an unexpected error occurrs on the server side.

        Returns:
            list[BuildingStatistics]: A list of objects per NUTS region with statistical
                info about buildings.
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
            response: requests.Response = requests.get(url, timeout=3600)
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException("An unexpected exception occurred.") from e

        results: list = json.loads(response.content)
        statistics: list[FootprintAreaStatistics] = []
        for res in results:
            statistic = FootprintAreaStatistics(
                nuts_code=res["nuts_code"],
                sum_footprint_area_total_m2=res["sum_footprint_area_total_m2"],
                avg_footprint_area_total_m2=res["avg_footprint_area_total_m2"],
                median_footprint_area_total_m2=res["median_footprint_area_total_m2"],
                avg_footprint_area_total_irrelevant_m2=res[
                    "avg_footprint_area_total_irrelevant_m2"
                ],
                sum_footprint_area_total_irrelevant_m2=res[
                    "sum_footprint_area_total_irrelevant_m2"
                ],
                median_footprint_area_total_irrelevant_m2=res[
                    "median_footprint_area_total_irrelevant_m2"
                ],
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
            country (str | None, optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to None.
            nuts_level (int | None, optional): The NUTS level. Defaults to None.
            nuts_code (str | None, optional): The NUTS code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions. Defaults to None.
            geom (str | None, optional): A custom geometry.

        Raises:
            ValueError: If both nuts_level and nuts_code are specified.
            ServerException: If an unexpected error occurrs on the server side.

        Returns:
            list[BuildingStatistics]: A list of objects per NUTS region with statistical
                info about buildings.
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
            response: requests.Response = requests.get(url, timeout=3600)
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException("An unexpected exception occurred.") from e

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
        """Get the refurbishment state statistics [m2] for the given nuts level or nuts code. Only one of nuts_level and nuts_code may be specified.

        Args:
            country (str | None, optional): The NUTS-0 code for the country, e.g. 'DE' for Germany. Defaults to None.
            nuts_level (int | None, optional): The NUTS level. Defaults to None.
            nuts_code (str | None, optional): The NUTS code, e.g. 'DE' for Germany according to the 2021 NUTS code definitions. Defaults to None.

        Raises:
            ValueError: If both nuts_level and nuts_code are specified.
            ServerException: If an unexpected error occurrs on the server side.

        Returns:
            list[RefurbishmentStateStatistics]: A list of objects per NUTS region with statistical info about refurbishment state of buildings.
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

        url: str = f"""{self.base_url}{statistics_url}{query_params}"""
        try:
            response: requests.Response = requests.get(url)
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException("An unexpected exception occurred.")

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

    def get_pv_generation_potential_statistics(
        self,
        country: str = "",
        nuts_level: Optional[int] = None,
        nuts_code: Optional[str] = None,
    ) -> list[PvGenerationPotentialStatistics]:
        """Get the PV generation potential statistics [kWh] for the given nuts level or
        nuts code. Only one of nuts_level and nuts_code may be specified.

        Args:
            country (str | None, optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to None.
            nuts_level (int | None, optional): The NUTS level. Defaults to None.
            nuts_code (str | None, optional): The NUTS code, e.g. 'DE' for Germany
                according to the 2021 NUTS code definitions. Defaults to None.

        Raises:
            ValueError: If both nuts_level and nuts_code are specified.
            ServerException: If an unexpected error occurrs on the server side.

        Returns:
            list[BuildingStatistics]: A list of objects per NUTS region with statistical
                info about buildings.
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
            response: requests.Response = requests.get(url, timeout=3600)
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException("An unexpected exception occurred.") from e

        results: list = json.loads(response.content)
        statistics: list[PvGenerationPotentialStatistics] = []
        for res in results:
            statistic = PvGenerationPotentialStatistics(
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

    def get_residential_heat_demand_statistics(
        self,
        country: str = "",
        nuts_level: Optional[int] = None,
        nuts_code: Optional[str] = None,
        geom: Optional[Polygon] = None,
    ) -> list[HeatDemandStatistics]:
        """Get the residential heat demand statistics [MWh] for the given NUTS level or
            NUTS/LAU code. Results can be limited to a certain country by setting the
            country parameter.
        Only one of nuts_level and nuts_code may be specified.

        Args:
            country (str | None, optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to None.
            nuts_level (int | None, optional): The NUTS level (0=NUTS-0, 1=NUTS-1,
                2=NUTS-2, 3=NUTS-3, 4=LAU). Defaults to None.
            nuts_code (str | None, optional): The NUTS or LAU code, e.g. 'DEA' for NRW
                in Germany according to the 2021 NUTS code and 2019 LAU code definitions. Defaults to None.
            geom (str | None, optional): A custom geometry.

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
            response: requests.Response = requests.get(url, timeout=3600)
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException("An unexpected exception occurred.") from e

        results: list = json.loads(response.content)
        statistics: list[HeatDemandStatistics] = []
        for res in results:
            statistic = HeatDemandStatistics(
                nuts_code=res["nuts_code"],
                heat_demand_mwh=res["heat_demand_MWh"],
            )
            statistics.append(statistic)
        return statistics

    def get_residential_heat_demand_statistics_by_building_characteristics(
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
            country (str | "", optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to "".
            construcion_year (str | None, optional): Construction year of
                building should be exactly the given year.        
            construcion_year_before (str | None, optional): Construction year of
                building should be before given year.
            construcion_year_after (str | None, optional): Construction year of
                building should be after given year.
            size_class (str | "", optional): Size class of building (SFH, MFH, TH, AB)
            refurbishment_state (str | None, optional): Refurbishment state of building 
                (1, 2, 3)
           
        Raises:
            ServerException: If an unexpected error occurrs on the server side.

        Returns:
            list[HeatDemandStatisticsByBuildingCharacteristics]: A list of objects per
                building parameter combination with statistical info about heat demand [MWh].
        """
        if construction_year and (construction_year_before or construction_year_after):
            raise ValueError("You cannot query for an exact construction year and a range at the same time.")
        
        statistics_url = self.RESIDENTIAL_HEAT_DEMAND_STATISTICS_BY_BUILDING_CHARACTERISTICS_URL
        
        construction_year_after_param = str(construction_year_after) if construction_year_after else ""
        construction_year_before_param = str(construction_year_before) if construction_year_before else ""
        construction_year_param = str(construction_year) if construction_year else ""

        query_params = f"?country={country}&construction_year__gt={construction_year_after_param}&construction_year={construction_year_param}&construction_year__lt={construction_year_before_param}&size_class={size_class}&refurbishment_state={refurbishment_state}"


        url: str = f"""{self.base_url}{statistics_url}{query_params}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600)
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException("An unexpected exception occurred.") from e

        results: list = json.loads(response.content)
        statistics: list[HeatDemandStatisticsByBuildingCharacteristics] = []
        for res in results:
            statistic = HeatDemandStatisticsByBuildingCharacteristics(
                country=res["country"],
                construction_year=res["construction_year"],
                size_class=res["size_class"],
                refurbishment_state=res["refurbishment_state"],
                heat_demand_mwh=res["heat_demand_MWh"],
            )
            statistics.append(statistic)
        return statistics

    def get_non_residential_energy_consumption_statistics(
        self,
        country: str = "",
        nuts_level: Optional[int] = None,
        nuts_code: Optional[str] = None,
        use: Optional[str] = None,
        geom: Optional[Polygon] = None,
    ) -> list[NonResidentialEnergyConsumptionStatistics]:
        """Get the energy consumption statistics [MWh] for the given nuts level or nuts
        code. Only one of nuts_level and nuts_code may be specified.

        Args:
            country (str | None, optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to None.
            nuts_level (int | None, optional): The NUTS level for which to retrieve the
                statistics. Defaults to None.
            nuts_code (str | None, optional): The NUTS code of the region for which to
                retrieve the statistics according to the 2021 NUTS code definitions.
                Defaults to None.
            geom (str | None, optional): A custom geometry.

        Raises:
            ValueError: If both nuts_level and nuts_code are given.
            ServerException: If an error occurrs on the server side.

        Returns:
            list[EnergyConsumptionStatistics]: A list of energy consumption statistics.
                If just one nuts_code is queried, the list will only contain one
                element.
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

        if use is not None:
            query_params += f"&use={use}"

        url: str = f"""{self.base_url}{statistics_url}{query_params}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600)
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException("An unexpected exception occurred.") from e

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

    def get_residential_energy_consumption_statistics(
        self,
        country: str = "",
        nuts_level: Optional[int] = None,
        nuts_code: Optional[str] = None,
        building_type: Optional[str] = None,
        use: Optional[str] = None,
        commodity: Optional[str] = None,
        geom: Optional[Polygon] = None,
    ) -> list[ResidentialEnergyConsumptionStatistics]:
        """Get the energy consumption statistics [MWh] for the given nuts level or nuts
        code. Only one of nuts_level and nuts_code may be specified.

        Args:
            country (str | None, optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to None.
            nuts_level (int | None, optional): The NUTS level for which to retrieve the
                statistics. Defaults to None.
            nuts_code (str | None, optional): The NUTS code of the region for which to
                retrieve the statistics according to the 2021 NUTS code definitions. Defaults to None.
            geom (str | None, optional): A custom geometry.

        Raises:
            ValueError: If both nuts_level and nuts_code are given.
            ServerException: If an error occurrs on the server side.

        Returns:
            list[EnergyConsumptionStatistics]: A list of energy consumption statistics.
                If just one nuts_code is queried, the list will only contain one element.
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
            statistics_url = self.RESIDENTIAL_ENERGY_CONSUMPTION_STATISTICS_BY_GEOM_URL
            query_params = f"?geom={geom.wkt}"
        else:
            statistics_url = self.RESIDENTIAL_ENERGY_CONSUMPTION_STATISTICS_URL
            query_params = f"?country={country}"
            if nuts_level is not None:
                query_params += f"&nuts_level={nuts_level}"
            elif nuts_code is not None:
                query_params += f"&nuts_code={nuts_code}"

        if building_type is not None:
            query_params += f"&type={building_type}"
        if use is not None:
            query_params += f"&use={use}"
        if commodity is not None:
            query_params += f"&commodity={commodity}"

        url: str = f"""{self.base_url}{statistics_url}{query_params}"""
        try:
            response: requests.Response = requests.get(url, timeout=3600)
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException("An unexpected exception occurred.") from e

        results: list = json.loads(response.content)
        statistics: list[ResidentialEnergyConsumptionStatistics] = []
        for res in results:
            statistic = ResidentialEnergyConsumptionStatistics(
                nuts_code=res["nuts_code"],
                solids_consumption_mwh=res["solids_consumption_MWh"],
                lpg_consumption_mwh=res["lpg_consumption_MWh"],
                gas_diesel_oil_consumption_mwh=res["gas_diesel_oil_consumption_MWh"],
                gas_consumption_mwh=res["gas_consumption_MWh"],
                biomass_consumption_mwh=res["biomass_consumption_MWh"],
                geothermal_consumption_mwh=res["geothermal_consumption_MWh"],
                derived_heat_consumption_mwh=res["derived_heat_consumption_MWh"],
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
            country (str | None, optional): The NUTS-0 code for the country, e.g. 'DE'
                for Germany. Defaults to None.
            nuts_level (int | None, optional): The NUTS level for which to retrieve the
                statistics. Defaults to None.
            nuts_code (str | None, optional): The NUTS code of the region for which to
                retrieve the statistics according to the 2021 NUTS code definitions.
                Defaults to None.
            commodity (str, optional): The commodity for which to get statistics

        Raises:
            ValueError: If both nuts_level and nuts_code are given.
            ServerException: If an error occurrs on the server side.
            geom (str | None, optional): A custom geometry.

        Returns:
            list[EnergyCommodityStatistics]: A list of building commodity statistics.
                If just one nuts_code is queried, the list will only contain one element.
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
            response: requests.Response = requests.get(url, timeout=3600)
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ServerException("An unexpected exception occurred.") from e

        results: list = json.loads(response.content)
        statistics: list[EnergyCommodityStatistics] = []
        for res in results:
            res_nuts_code: str = res["nuts_code"]
            res_commodity: str = res["commodity"]
            res_heating_commodity_count: int = int(
                res["commodity_count"]["heating_commodity_count"]
            )
            res_cooling_commodity_count: int = int(
                res["commodity_count"]["cooling_commodity_count"]
            )
            res_water_heating_commodity_count: int = int(
                res["commodity_count"]["water_heating_commodity_count"]
            )
            res_cooking_commodity_count: int = int(
                res["commodity_count"]["cooking_commodity_count"]
            )

            statistic = EnergyCommodityStatistics(
                nuts_code=res_nuts_code,
                commodity_name=res_commodity,
                building_count=CommodityCount(
                    heating_commodity_count=res_heating_commodity_count,
                    cooling_commodity_count=res_cooling_commodity_count,
                    water_heating_commodity_count=res_water_heating_commodity_count,
                    cooking_commodity_count=res_cooking_commodity_count,
                ),
            )
            statistics.append(statistic)

        return statistics
