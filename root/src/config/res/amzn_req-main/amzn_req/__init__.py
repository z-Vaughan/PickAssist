import os
import warnings
import http.cookiejar
from http.cookiejar import CookieJar, Cookie
from typing import Union, List, Dict, Any

from bs4 import BeautifulSoup
import requests
from requests_kerberos import HTTPKerberosAuth, OPTIONAL

from ._cookie import Cookie as BrowserCookies
from ._task import Task
from .exception import MidwayUnauthenticatedError

warnings.simplefilter("ignore")
CookieJars = Union[http.cookiejar.CookieJar | http.cookiejar.MozillaCookieJar]


class AmznReq:
    """
    AmznReq is a class designed to facilitate communication with Amazon's web services.
    It combines Kerberos authentication with session-based cookie management to enable secure HTTP requests.
    By using this class, you can automate the authentication process for accessing web pages or APIs that require authentication,
    leveraging cookies exported from browsers or obtained from Selenium WebDriver.

    Features:
    - Secure request execution through Kerberos authentication
    - Cookie import functionality from mwinit
    - Cookie import functionality from browsers (Chrome, Firefox, Edge)
    - Cookie import functionality from Selenium WebDriver
    - Session cookie export functionality, with support for exporting in a format compatible with Selenium WebDriver
    - Midway authentication check and initialization of authentication state

    This class is particularly designed to be useful when accessing Amazon's internal web services,
    and is recommended for Amazon's internal developers or application developers utilizing Amazon's web services.
    """

    def __init__(self):
        self.cookie = BrowserCookies()
        self.taskkill = Task()
        self.session = requests.Session()
        self._cj: CookieJars = self.cookie.mwinit()

    def requests(
        self,
        url: str,
        method: str = "get",
        verify: bool = False,
        **kwargs,
    ) -> requests.Response:
        """
        Send an HTTP request using the specified method to the specified URL,
        including Kerberos authentication and the current session's cookies.
        This method allows for additional requests parameters to be passed via kwargs.

        Args:
            url (str): The URL to which the request is to be sent.
            method (str, optional): The HTTP method to be used for the request. Common methods are 'get', 'post', 'put', 'delete', etc. Defaults to "get".
            verify (bool, optional): Whether to verify the server's TLS certificate. Defaults to False.
            **kwargs: Additional keyword arguments passed to requests.request`
        Returns:
            requests.Response: The response to the request, including status, headers, and body content.

        Example usage:
            response = amzn_req.requests("https://example.com/api/data", method="post", json={"key": "value"})
        """
        kerberos_auth: HTTPKerberosAuth = HTTPKerberosAuth(
            mutual_authentication=OPTIONAL
        )
        return self.session.request(
            method=method,
            url=url,
            auth=kerberos_auth,
            cookies=self._cj,
            verify=verify,
            **kwargs,
        )

    def import_cookies(self, cj: CookieJars):
        """
        Imports cookies from a given CookieJar (or MozillaCookieJar) into the current session.

        This method allows for seamless integration of cookies obtained from different sources,
        such as web browsers or external HTTP clients, into the AmznReq session. This can be particularly
        useful for maintaining session continuity across different systems or for pre-loading cookies
        for authentication or session management purposes.
        Args:
            cj (CookieJars): An instance of CookieJar or MozillaCookieJar containing cookies to be imported.
                            The CookieJar (or MozillaCookieJar) is a collection of HTTP cookies that is
                            intended to be compatible with various HTTP client libraries. This union type
                            ensures that the method can accept either standard Python CookieJar objects
                            or MozillaCookieJar objects, which offer additional functionality like loading
                            cookies from a file that uses the Mozilla cookie file format.
        Example usage:
            # Assume 'browser_cookies' is a CookieJar instance containing cookies extracted from a web browser.
            amzn_req.import_cookies(browser_cookies)
        """
        self._cj = cj

    def export_cookies(self) -> CookieJars:
        """
        Exports the current session's cookies as a CookieJar (or MozillaCookieJar).

        This method provides the functionality to retrieve the session's cookies in a format that can be
        easily used or stored for future requests, or shared with different systems requiring the same
        cookie information. The returned object can be directly used with requests library or saved to a file
        for later use with other HTTP clients that support CookieJar or MozillaCookieJar formats.

        Returns:
            CookieJars: An instance of CookieJar or MozillaCookieJar containing the current session's cookies.
                        This allows for the flexibility of working with a standard Python CookieJar object or a
                        MozillaCookieJar object that offers additional functionality, such as saving the cookies
                        to a file in Mozilla's cookie file format.

        Example usage:
            # Export the current session's cookies.
            cookies = amzn_req.export_cookies()
            # Cookies can now be saved to a file, or used in another session or application.
        """
        return self._cj

    def import_cookies_from_selenium(self, selenium_cookies: List[dict]):
        """
        Imports cookies from a Selenium WebDriver into the current session's CookieJar.

        This method enables the seamless transition of cookies obtained from a Selenium browser session
        into the `requests` session managed by this class. It allows for web scraping and automation tasks
        that require maintaining session continuity between Selenium and `requests`.

        Args:
            selenium_cookies (list[dict]): A list of cookie dictionaries obtained from Selenium's `get_cookies` method.
                                        Each dictionary represents a cookie, and should contain keys like 'name',
                                        'value', 'domain', 'path', 'secure', 'expiry', and optionally 'httpOnly'.
                                        These cookies are then processed and added to the current session's CookieJar.

        Example usage:
            # Assuming 'driver' is a Selenium WebDriver instance that has already navigated to a website and collected cookies.
            selenium_cookies = driver.get_cookies()
            amzn_req.import_cookies_from_selenium(selenium_cookies)

        After execution, the cookies from the Selenium session will be available for use in requests made by this `AmznReq` instance.
        """
        new_cj = CookieJar()
        for selenium_cookie in selenium_cookies:
            cookie = Cookie(
                version=0,
                name=selenium_cookie["name"],
                value=selenium_cookie["value"],
                port=None,
                port_specified=False,
                domain=selenium_cookie["domain"],
                domain_specified=bool(selenium_cookie["domain"]),
                domain_initial_dot=False,
                path=selenium_cookie["path"],
                path_specified=bool(selenium_cookie["path"]),
                secure=selenium_cookie.get("secure", False),
                expires=selenium_cookie.get("expiry"),
                discard=False,
                comment=None,
                comment_url=None,
                rest={"HttpOnly": selenium_cookie.get("httpOnly", False)},
                rfc2109=False,
            )
            new_cj.set_cookie(cookie)
        self._cj = new_cj

    def export_cookies_for_selenium(
        self, exclude_unrelated_domain_cookies=True
    ) -> List[Dict[str, Any]]:
        """
        Exports the current session's cookies in a format compatible with Selenium WebDriver.

        This method facilitates the transfer of cookies from `requests` sessions to Selenium browser sessions.
        It converts cookies stored in the current session's CookieJar into a list of dictionaries. Each dictionary
        contains key-value pairs representing cookie attributes, compatible with Selenium's `add_cookie` method.

        Parameters:
            exclude_unrelated_domain_cookies (bool): If True, cookies that do not belong to the main domain of interest
                                                    ('midway-auth.amazon.com' in this example) are excluded from the export.
                                                    This helps to ensure that only relevant cookies are transferred to the
                                                    Selenium session. Defaults to True.

        Returns:
            List[Dict[str, Any]]: A list of cookie dictionaries suitable for use with Selenium's `add_cookie` method.
                                Each dictionary includes attributes such as 'name', 'value', 'domain', 'path', and 'expiry'.
                                This enables cookies from `requests` sessions to be seamlessly integrated into Selenium
                                browser sessions, maintaining session state across different web automation tools.

        Example usage:
            # Export cookies from `AmznReq` instance to a list of dictionaries.
            selenium_cookies = amzn_req.export_cookies_for_selenium()
            # Assuming 'driver' is a Selenium WebDriver instance.
            for cookie in selenium_cookies:
                driver.add_cookie(cookie)

        After execution, the `requests` session cookies will be available in the Selenium browser session, allowing for
        a unified web session across automation scripts.
        """
        cookie_list = []
        for cookie in self._cj:
            if (
                exclude_unrelated_domain_cookies
                and cookie.domain.strip(".") != "midway-auth.amazon.com"
            ):
                continue
            cookie_dict = {
                "name": cookie.name,
                "value": cookie.value,
                "path": cookie.path,
                "domain": cookie.domain,
            }
            if hasattr(cookie, "expires") and cookie.expires:
                cookie_dict["expiry"] = cookie.expires
            cookie_list.append(cookie_dict)
        return cookie_list

    def is_midway_authenticated(self, alias) -> bool:
        """
        Checks if the current session is authenticated with Midway.

        This method attempts to detect if the current session has successfully authenticated with the Midway
        authentication system by sending a request to a known URL and checking for the presence of a specific
        HTML element that indicates a successful authentication. This is useful for determining if the session
        needs to perform additional authentication steps or if it can proceed with actions that require authentication.

        Returns:
            bool: True if the session is authenticated with Midway, False otherwise. This determination is based on
                the presence of specific HTML elements in the response to a known URL, which typically indicates
                successful authentication.

        Example usage:
            # Check if the session is already authenticated with Midway.
            if amzn_req.is_midway_authenticated():
                print("Session is authenticated with Midway.")
            else:
                print("Session is not authenticated with Midway, performing authentication steps.")
        """
        url: str = "https://midway-auth.amazon.com/"
        r = self.requests(url)
        bs = BeautifulSoup(r.text, "html.parser")  # Ensure to specify the parser
        title_element = bs.select_one("h1.title")
        if title_element:
            title_text = title_element.get_text().strip()
            if title_text == f"Welcome {alias}!":
                return True
        else:
            return False
    



    def exec_mwinit(self, alias):
        """
        Executes the Midway initialization command to authenticate the session.

        This method checks if the current session is already authenticated with Midway. If not, it attempts
        to authenticate by executing the `mwinit -o` system command, which is typically used to obtain new
        authentication cookies from Midway. This is particularly useful for sessions that require authenticated
        access to resources that are protected by Midway authentication. If the authentication attempt fails
        (indicated by a non-zero return value from the `os.system` call), a `MidwayUnauthenticatedError` is raised
        to signal that the session could not be authenticated.

        Raises:
            MidwayUnauthenticatedError: If the `mwinit -o` command fails to authenticate the session. This exception
                                        indicates that the session remains unauthenticated and further action is required
                                        to obtain valid authentication cookies.

        Example usage:
            # Attempt to authenticate the session with Midway if it's not already authenticated.
            try:
                amzn_req.exec_mwinit()
                print("Session successfully authenticated with Midway.")
            except MidwayUnauthenticatedError:
                print("Failed to authenticate session with Midway.")
        """
        if not self.is_midway_authenticated(alias):
            if os.system("mwinit -s -o") != 0:
                raise MidwayUnauthenticatedError()

    def refresh_session(self):
        """
        Clears the headers of the current session.

        This method is used to remove all custom headers that have been added to the session. It's useful
        when you need to reset the session's state without creating a new session instance, particularly
        in scenarios where the session might have been modified with headers that are no longer needed or
        are incorrect for subsequent requests.

        Example usage:
            amzn_req.refresh_session()  # Clear all custom headers from the session
        """
        self.session.headers.clear()

    def close(self):
        """
        Closes the current session.

        This method gracefully closes the current session, releasing any resources that it holds. This includes
        terminating any underlying TCP connections in the connection pool. It is a good practice to call this
        method when you are done with a session, especially when working with a large number of requests, to
        ensure that resources are released properly.

        Example usage:
            amzn_req.close()  # Close the session and release resources
        """
        self.session.close()

    def new_session(self):
        """
        Closes the current session and starts a new session.

        This method is designed to completely reset the session by first closing the existing session to
        release any resources it holds, and then creating a new session instance. It's particularly useful
        when you need to ensure that no residual state (like cookies or custom headers) from the previous session
        affects subsequent requests. This can be important in scenarios where complete isolation between requests
        or sequences of requests is required.

        Example usage:
            amzn_req.new_session()  # Reset the session for a clean state
        """
        self.close()
        self.session = requests.Session()

    def set_chrome_cookie(self):
        """
        Sets the session cookies from Chrome, incorporating cookies directly into the current session.

        This method leverages the `chrome` function from the `Cookie` class to retrieve cookies stored in Chrome.
        It is particularly useful for scripts or applications that need to simulate a browser session previously established in Chrome,
        ensuring continuity in session state and authenticated contexts. Before retrieving Chrome cookies, it terminates any running
        Chrome processes to ensure the most current cookies are retrieved. The session is then refreshed to apply these new cookies.

        Example usage:
            amzn_req.set_chrome_cookie()
            # The session now contains cookies from Chrome, ready for authenticated requests as needed.
        """
        self.taskkill.chrome()
        self._cj = self.cookie.chrome()
        self.refresh_session()

    def set_firefox_cookie(self):
        """
        Imports Firefox cookies into the session, updating the session's state with cookies retrieved from Firefox.

        By executing `firefox` from the `Cookie` class, this method obtains cookies from a user's Firefox browser, enabling
        the session to mimic a browser session that was initiated in Firefox. This can be essential for accessing web resources
        that require authentication or for continuity in web scraping activities. Similar to Chrome, it terminates any Firefox
        processes before retrieving cookies to ensure accuracy and recency of the session cookies. The session is refreshed afterwards
        to include these cookies.

        Example usage:
            amzn_req.set_firefox_cookie()
            # Session is now augmented with Firefox cookies, facilitating authenticated web interactions.
        """
        self.taskkill.firefox()
        self._cj = self.cookie.firefox()
        self.refresh_session()

    def set_edge_cookie(self):
        """
        Updates the current session with cookies from Microsoft Edge, seamlessly integrating Edge browser cookies.

        Utilizing the `edge` method from the `Cookie` class, this method captures cookies from Edge to use within the session.
        This approach is advantageous for tasks that rely on the presence of specific cookies for authentication or session management,
        especially when transitioning between browser-based interactions and server-side requests. Edge processes are terminated prior
        to cookie retrieval to ensure the session cookies are current. Following cookie retrieval, the session is refreshed to activate
        these cookies for upcoming requests.

        Example usage:
            amzn_req.set_edge_cookie()
            # With Edge cookies now in the session, authenticated or cookie-dependent requests can be made seamlessly.
        """
        self.taskkill.edge()
        self._cj = self.cookie.edge()
        self.refresh_session()

    def set_mwinit_cookie(self):
        """
        Loads and sets Midway authentication cookies into the current session's cookie jar.

        This method utilizes the `mwinit` method of the `Cookie` class to load Midway authentication cookies
        from a local file specified by the MIDWAY_COOKIE_FILENAME constant. After successfully loading these
        cookies, it updates the session's cookie jar (`self._cj`) with the loaded cookies and then refreshes
        the session to ensure that subsequent HTTP requests will include these authentication cookies. This
        is crucial for performing authenticated actions or accessing resources that require Midway authentication.

        Raises:
            MidwayUnauthenticatedError: If there's an issue loading the Midway cookies, indicating that
                                        the session could not be authenticated with Midway due to missing,
                                        invalid, or expired cookies.

        Example usage:
            amzn_req.set_mwinit_cookie()
            # After this method call, the session is ready to perform authenticated requests using Midway cookies.
        """
        self._cj = self.cookie.mwinit()
        self.refresh_session()
