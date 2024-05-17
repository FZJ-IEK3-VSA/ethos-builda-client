from abc import ABC

import requests

from builda_client.exceptions import ClientException, ServerException, UnauthorizedException

class BaseClient(ABC):
    def handle_exception(self, err: requests.exceptions.HTTPError):
            if err.response.status_code == 403:
                raise UnauthorizedException(
                    """You are not authorized to perform this operation. Perhaps wrong 
                    username and password given?"""
                )

            if err.response.status_code >= 400 and err.response.status_code <= 499:
                raise ClientException("A client side error occured", err) from err

            raise ServerException("An unexpected error occurred. Please contact administrator.", err) from err