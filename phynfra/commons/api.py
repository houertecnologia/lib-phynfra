import requests

from loguru import logger


class API:
    def __init__(self, method: str, headers: str, **kwargs) -> None:
        """
        API class, having functions to request a object from URL HTTP

        Args:
            method: Method for the new request object: GET, OPTIONS, HEAD, POST, PUT, PATCH OR DELETE
            headers: Directory of HTTP headers to send with the request
            kwargs: Optional arguments for the new request
        """
        self.method = method
        self.headers = headers
        self.params = kwargs

    def http_request(self, url: str) -> dict:
        """
        Request a object from URL HTTP

        Args:
            url: URL for the new request object
        """
        params = {"url": url, "method": self.method, "headers": self.headers, **self.params}

        logger.debug(f"Sending {self.method} HTTP request to {url}")
        try:
            response = requests.request(**params)
            response.raise_for_status()

            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"Error: {e}")
            raise e

    def get_file(self, url: str):
        """
        Get file from URL HTTP

        Args:
            url: URL for the new request object
        """
        params = {"url": url, "method": self.method, "headers": self.headers, "stream": True, **self.params}
        try:
            response = requests.request(**params).raw
            return response
        except FileNotFoundError:
            return False
