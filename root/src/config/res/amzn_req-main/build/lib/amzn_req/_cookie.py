import os
import tempfile
import http.cookiejar


import browser_cookie3

from ._constants import MIDWAY_COOKIE_FILENAME
from .exception import MidwayUnauthenticatedError


class Cookie:
    @staticmethod
    def chrome() -> http.cookiejar.CookieJar:
        return browser_cookie3.chrome()

    @staticmethod
    def firefox() -> http.cookiejar.CookieJar:
        return browser_cookie3.firefox()

    @staticmethod
    def edge() -> http.cookiejar.CookieJar:
        return browser_cookie3.edge()

    @staticmethod
    def _write_temp_file(line: str) -> None:
        if line.startswith("#HttpOnly_"):
            return line[10:]
        return line

    def mwinit(self) -> http.cookiejar.MozillaCookieJar:
        """
        Load Midway cookies from a local file.

        :return: cookie jar object loaded with Midway cookies
        :raises MidwayUnauthenticatedError: If there's an issue loading the cookies
        """
        try:
            with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
                with open(MIDWAY_COOKIE_FILENAME) as midway_file:
                    for line in midway_file:
                        data = self._write_temp_file(line)
                        temp_file.write(data)
                temp_file.flush()
                cookies: http.cookiejar.MozillaCookieJar = (
                    http.cookiejar.MozillaCookieJar(temp_file.name)
                )
                cookies.load(ignore_discard=True, ignore_expires=True)
        except Exception:
            raise MidwayUnauthenticatedError()
        else:
            return cookies
        finally:
            os.remove(temp_file.name)
