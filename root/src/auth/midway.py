from PySide6.QtCore import QObject, Signal

#Time
from datetime import datetime as dt
import time

# System and Misc
import getpass
import os
import sys

from requests.exceptions import ConnectionError

# Module Path Fix
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.utils.logger import CustomLogger
logger = CustomLogger.get_logger(__name__)
#logger.error(f"Some Error: {str(e)}")
#logger.info("Some Info")

from urllib3 import disable_warnings
disable_warnings()


#Library for AmznReq Midway
from amzn_req import AmznReq, MidwayUnauthenticatedError
# Lib wiki: https://w.amazon.com/bin/view/Users/mikohei/python/amzn_req/
try:
    ar = AmznReq()
except FileNotFoundError:
    os.system("mwinit -o")
    ar = AmznReq()
except MidwayUnauthenticatedError:
    os.system("mwinit -o")
    ar = AmznReq()


class MidwayAuth(QObject):
    MAX_RETRIES = 2
    RETRY_DELAY = 2  # seconds
    authentication_complete = Signal()

    class MidwayUnauthenticatedError(Exception):
        """Exception thrown when Midway authentication fails."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.alias = getpass.getuser()
        self.amzn_req = ar
        self.selenium_cookies = []
        self.parent_app = parent

    def invalidate_midway(self):
        """Invalidates the Midway cookie by deleting the cookie file."""
        cookie_path = os.path.join(os.path.expandvars('%USERPROFILE%'), '.midway', 'cookie')
        logger.info('Invalidating Midway Cookie...')
        self.amzn_req.new_session()
        os.system("mwinit -d")
        logger.info('Completed mwinit -d')
        if os.path.exists(cookie_path):
            try:
                os.remove(cookie_path)
                logger.info("Cookie invalidated manually.")
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error while deleting cookie: {str(e)}")

        else:
            logger.info("Cookie file does not exist.")


    def authenticate(self, new_session=False):

        """Authenticates the user with the Midway system."""

        self.amzn_req.new_session()
        logger.info('New Session Initiated...')
        
        max_retries = 2
        retry_count = 0
        try:
            auth_status = self.amzn_req.is_midway_authenticated(self.alias)
        except ConnectionError as e:
            logger.warning(f"ConnectionError occured in Midway.authenticate: {str(e)}")
            logger.info("Attempting to solve with reauth (mwinit -o)")
            os.system("mwinit -o")
            try:
                auth_status = self.amzn_req.is_midway_authenticated(self.alias)
            except ConnectionError as e:
                logger.error(f"Reauth did not solve ConnectionError: {str(e)}")
                sys.exit(1)

        while retry_count < max_retries:

            try:

                cookie_path = os.path.join(os.path.expandvars('%USERPROFILE%'), '.midway', 'cookie')

                auth_status = self.amzn_req.is_midway_authenticated(self.alias)
                logger.info('Auth Status: %s', auth_status)

                if os.path.exists(cookie_path):

                    cookie_expiration = self.get_cookie_expiration()

                    if cookie_expiration is not None and cookie_expiration < dt.now():

                        logger.info('Midway cookie expired. Invalidating...')
                        self.invalidate_midway()

                    else:

                        logger.info('Fresh Cookies!')

                if not auth_status or not os.path.exists(cookie_path):
                    
                    try:
                        logger.info('Executing MWINIT via CMD...')
                        self.amzn_req.exec_mwinit(self.alias)
                        max_wait_time = 5
                        start_time = time.time()
                    except MidwayUnauthenticatedError:
                        logger.error("Failed to authenticate session with Midway.")
                        retry_count += 1
                        if retry_count >= max_retries:

                            if hasattr(self, 'parent_app'):
                                try:
                                    if hasattr(self.parent, 'app'):
                                        self.parent.app.quit()
                                except:
                                    if hasattr(self.parent, 'app'):
                                        self.parent.app.quit()
                            sys.exit(0)
                            #raise MidwayUnauthenticatedError()
                        logger.info(f'Retrying authentication... Attempt {retry_count} of {max_retries}')
                        max_wait_time = 0
                        start_time = time.time()
                        continue



                    while not auth_status and time.time() - start_time < max_wait_time:

                        
                        logger.info('Refreshing Session')
                        self.amzn_req.refresh_session()
                        time.sleep(1)
                        auth_status = self.amzn_req.is_midway_authenticated(self.alias)
                        logger.info(f'Auth Status: {auth_status} // Time: {time.time() - start_time}')

                    if os.path.exists(cookie_path):
                        self.amzn_req.refresh_session()
                        auth_status = self.amzn_req.is_midway_authenticated(self.alias)
                        logger.info(f'New Auth Status: %s', auth_status)

                    else:
                        logger.info(f'OS Path Not Available: {cookie_path}')

                if not auth_status:
                    logger.info('Bad Auth. Invalidating Midway Cookie')
                    retry_count += 3
                    raise MidwayUnauthenticatedError()

                else:

                    self.selenium_cookies = self.amzn_req.export_cookies_for_selenium()
                    break

            except ConnectionError:

                logger.error('Connection Error. Closing..')
                auth_status = False
                return False

            except MidwayUnauthenticatedError:

                logger.error("Failed to authenticate session with Midway.")
                retry_count += 1
                logger.info(f'Retrying authentication... Attempt {retry_count} of {max_retries}')

        if retry_count >= max_retries:
            return False

        else:

            logger.info('Authenticated Successfully.')
            if self.parent_app:
                self.parent_app.auth_status = True
            self.authentication_complete.emit()


        return auth_status


    def get_cookie_expiration(self):
        """Retrieves the expiration date and time of a cookie stored in a file."""
        cookie_path = os.path.join(os.path.expandvars('%USERPROFILE%'), '.midway', 'cookie')
        if os.path.exists(cookie_path):
            with open(cookie_path, 'r') as cookie_file:
                cookie_data = cookie_file.read()
            for line in cookie_data.split('\n'):
                if line.startswith('#HttpOnly_'):
                    fields = line.split('\t')
                    if len(fields) >= 7:
                        expiration_timestamp = int(fields[4])
                        expiration_datetime = dt.fromtimestamp(expiration_timestamp)
                        return expiration_datetime
        return None