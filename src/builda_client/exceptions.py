class ServerException(Exception):
    """Any exception that occurrs on the server side.
    """
    pass

class ClientException(Exception):
    """Generic client-side exception.
    """
    pass 

class MissingCredentialsException(ClientException):
    """Credentials missing where authentification is required.
    """
    pass

class UnauthorizedException(ClientException):
    """The client is not authorized to perform the operation, e.g. API token is invalid.
    """
    pass

class GeocodeException(ServerException):
    """Could not (reverse) geocode the data.
    """
    pass