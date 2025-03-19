
from functools import lru_cache, wraps
from requests.exceptions import ConnectionError, RequestException
from typing import Any, Dict, Optional
from datetime import datetime as dt, timedelta as td
from urllib.parse import urlencode
import asyncio
import sys
import os
import time



from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.support.wait import WebDriverWait

#Library for AmznReq Midway
from amzn_req import AmznReq, MidwayUnauthenticatedError
# Lib wiki: https://w.amazon.com/bin/view/Users/mikohei/python/amzn_req/



# Module Path Fix
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.auth.midway import MidwayAuth
from src.config.chronos import TimeManager
from src.utils.logger import CustomLogger
logger = CustomLogger.get_logger(__name__)
#logger.error(f"Some Error: {str(e)}")
#logger.info("Some Info")


import logging
logger = logging.getLogger(__name__)

def build_urls ():
    time_manager = TimeManager.get_instance()
    SHIFT_INFO = time_manager.get_shift_info()
        
    # Use the information
    SITE_CODE = SHIFT_INFO['site_code']
    shift_start = SHIFT_INFO['shift_start']
    shift_end = SHIFT_INFO['shift_end']
    hours_remaining = SHIFT_INFO['hours_remaining']
    elapsed_time = SHIFT_INFO['elapsed_time']
    start_millis = SHIFT_INFO['start_millis']
    end_millis = SHIFT_INFO['end_millis']

    # Format dates properly for LPI
    start_date_formatted = shift_start.strftime('%Y/%m/%d')
    end_date_formatted = shift_end.strftime('%Y/%m/%d')
    start_week_formatted = (shift_start - td(days=7)).strftime('%Y/%m/%d')
    start_month_formatted = (shift_start - td(days=30)).replace(day=1).strftime('%Y/%m/%d')

    URLS_RAW = {
        "Workforce" : f'https://picking-console.na.picking.aft.a2z.com/api/fcs/{SITE_CODE}/workforce',
        "Process" : f'https://picking-console.na.picking.aft.a2z.com/api/fcs/{SITE_CODE}/process-paths/information',
        "LPI" : {
            "base" :'https://fclm-portal.amazon.com/ppa/inspect/process?',
            "params" : {
                'primaryAttribute' : 'WORK_FLOW',
                'secondaryAttribute' : 'PICKING_PROCESS_PATH',
                'nodeType' : 'FC',
                'warehouseId' : SITE_CODE,
                'processId' : '100115',
                'maxIntradayDays' : '1',
                'spanType' : 'Intraday',
                'startDateIntraday' : start_date_formatted,
                'startHourIntraday' : {shift_start.hour},
                'startMinuteIntraday' : {shift_start.minute},
                'endDateIntraday' : end_date_formatted,
                'endHourIntraday' : {shift_end.hour},
                'endMinuteIntraday' : {shift_end.minute},
            }
        },
        "LPI(Hist)" : {
            "base" :'https://fclm-portal.amazon.com/ppa/inspect/process?',
            "params" : {
                'primaryAttribute' : 'WORK_FLOW',
                'secondaryAttribute' : 'PICKING_PROCESS_PATH',
                'nodeType' : 'FC',
                'warehouseId' : SITE_CODE,
                'processId' : '100115',
                'maxIntradayDays' : '1',
                'spanType' : 'Week',
                'startDateWeek' : start_week_formatted,
            }
        },
        "Rodeo" : {
            "base" : f'http://rodeo-iad.amazon.com:80/{SITE_CODE}/ItemListCSV?',
            "params" : {
                'ShipmentId' : '',
                'ChargeRange.RangeStartMillis' : '',
                'ScannableId' : '',
                'ShipMethod' : '',
                'WorkPool' : 'PickingNotYetPicked',
                'EulerGroupType' : '',
                'FcSku' : '',
                'IsEulerPromiseMiss' : 'ALL',
                'GiftOption' : 'ALL',
                'NextDestination' : '',
                'shipmentType' :'TRANSSHIPMENTS',
                'ShipOption' : '',
                'Excel' : 'true',
                'ChargeRange.RangeEndMillis' : '',
                'FnSku' : '',
                '_enabledColumns' : 'on',
                'Condition' : '',
                'DwellTimeLessThan' : ['0', ''],
                f'ExSDRange.RangeStartMillis' : start_millis, #self.time_vars["milliStart"]
                'LastExSDRange.RangeStartMillis' : '',
                'DwellTimeGreaterThan' : ['0', ''],
                f'ExSDRange.RangeEndMillis' : end_millis, #self.time_vars["milliEnd"]
                'Fracs' : 'ALL',
                'PickBatchId' : '',
                'SortCode' : '',
                'DestinationWarehouseId' : '',
                'ProcessPath' : '',
                'IsReactiveTransfer' : 'ALL',
                'IsEulerUpgraded' : 'ALL',
                'OuterContainerType' : '',
                'PickPriority' : '',
                'enabledColumns' : ['OUTER_SCANNABLE_ID', 'OUTER_OUTER_SCANNABLE_ID'],
                'FulfillmentServiceClass' : 'ALL',
                'LastExSDRange.RangeEndMillis' : '',
                'IsEulerExSDMiss' : 'ALL',
                'FulfillmentReferenceId' : ''
            },
        },
    }

    URLS = {
        "Rodeo" : f"{URLS_RAW['Rodeo']['base']}{urlencode(URLS_RAW['Rodeo']['params'], doseq=True)}",
        "LPI" : f"{URLS_RAW['LPI']['base']}{urlencode(URLS_RAW['LPI']['params'], doseq=True)}",
        "LPI(Hist)" : f"{URLS_RAW['LPI(Hist)']['base']}{urlencode(URLS_RAW['LPI(Hist)']['params'], doseq=True)}",
        "Workforce" : URLS_RAW["Workforce"],
        "Process" : URLS_RAW["Process"]
    }
    for name, url in URLS.items():
        logger.info('URL: %s\n%s', name, url)
    return URLS

def retry_on_webdriver_error(max_attempts=3, delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except WebDriverException as e:
                    attempts += 1
                    # Add check for browsing context error
                    if ("browsing context has been discarded" in str(e).lower() or 
                        "Process unexpectedly closed with status 0" in str(e)):
                        if attempts == max_attempts:
                            logger.error(f"Failed after {max_attempts} attempts: {str(e)}")
                            raise  # Re-raise the last exception if all attempts failed
                        logger.warning(f"WebDriver failed, attempt {attempts} of {max_attempts}. Retrying...")
                        time.sleep(delay)  # Wait before retrying
                        
                        # Attempt to recreate the driver
                        if hasattr(args[0], 'driver'):
                            try:
                                args[0].driver.quit()
                            except:
                                pass
                            # Initialize new driver here if needed
                    else:
                        raise  # Re-raise if it's a different WebDriverException
            return None
        return wrapper
    return decorator


class WebDriverManager:
    @classmethod
    def initialize_webdriver(cls):
        """Initialize the Selenium WebDriver with appropriate options"""
        try:
            options = webdriver.FirefoxOptions()
            
            # Basic options
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            
            # Additional stability options
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-software-rasterizer')
            options.add_argument('--width=1920')
            options.add_argument('--height=1080')
            
            # Performance optimizations
            options.set_preference('browser.cache.disk.enable', False)
            options.set_preference('browser.cache.memory.enable', False)
            options.set_preference('browser.cache.offline.enable', False)
            options.set_preference('network.http.use-cache', False)
            
            # Reduce resource usage
            options.set_preference('browser.sessionhistory.max_entries', 1)
            options.set_preference('browser.sessionhistory.max_total_viewers', 1)

            # Create service with specific settings
            service = webdriver.firefox.service.Service(
                log_path='geckodriver.log',
                service_args=['--log', 'fatal']  # Enable detailed logging
            )
            
            # Initialize driver with both options and service
            driver = webdriver.Firefox(
                options=options,
                service=service
            )
            
            # Set timeouts
            driver.set_page_load_timeout(15)  # Reduced from 30
            driver.implicitly_wait(5)         # Reduced from 10
            driver.set_script_timeout(15)     # Reduced from 30
            
            logger.info("\nWebDriver initialized successfully\n")
            return driver
            
        except Exception as e:
            logger.error(f"Failed to initialize WebDriver: {str(e)}")
            return None

    @classmethod
    def cleanup_driver(cls, driver):
        """Safely clean up the WebDriver instance"""
        if driver:
            try:
                driver.quit()
            except Exception as e:
                logger.warning(f"Error while cleaning up driver: {e}")

    @classmethod
    @retry_on_webdriver_error(max_attempts=2, delay=1)
    def get_cookies_with_selenium(cls):
        """Get cookies using Selenium WebDriver with proper error handling"""
        driver = None

        time_manager = TimeManager.get_instance()
        shift_info = time_manager.get_shift_info()
        site_code = shift_info['site_code']

        try:
            driver = cls.initialize_webdriver()
            if not driver:
                return None
            wait = WebDriverWait(driver, 8, poll_frequency=0.1)
            
            target_url = f'https://picking-console.na.picking.aft.a2z.com/fc/{site_code}/pick-workforce'
            driver.get(target_url)
            wait.until(lambda d: d.execute_script('return document.readyState') == 'complete')
            
            cookies = driver.get_cookies()
            
            return cookies
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception as e:
                    logger.warning(f"Error during driver cleanup: {str(e)}")
                    # Force quit if normal quit fails
                    try:
                        driver.service.process.kill()
                    except:
                        pass


class AmznReqManager:
    _instance = None
    
    @classmethod
    @lru_cache(maxsize=1)
    def get_instance(cls):
        if cls._instance is None:
            try:
                cls._auth_status = MidwayAuth().authenticate()
                cls._instance = AmznReq()
            except FileNotFoundError:
                os.system("mwinit -o")
                cls._instance = AmznReq()
                selenium_cookies = WebDriverManager.get_cookies_with_selenium()
                cls._instance.import_cookies_from_selenium(selenium_cookies)
            except MidwayUnauthenticatedError:
                os.system("mwinit -o")
                try:
                    cls._instance = AmznReq()
                    selenium_cookies = WebDriverManager.get_cookies_with_selenium()
                    cls._instance.import_cookies_from_selenium(selenium_cookies)
                except MidwayUnauthenticatedError:
                    os.system("mwinit -o")
                    try:
                        cls._instance = AmznReq()
                        selenium_cookies = WebDriverManager.get_cookies_with_selenium()
                        cls._instance.import_cookies_from_selenium(selenium_cookies)
                    except MidwayUnauthenticatedError:
                        logger.error("Midway authentication failed after multiple attempts.")
                        raise
        else:
            selenium_cookies = WebDriverManager.get_cookies_with_selenium()
            cls._instance.import_cookies_from_selenium(selenium_cookies)
        return cls._instance

    @classmethod
    def refresh_instance(cls):
        """Force refresh the AmznReq instance"""
        cls._instance = None
        cls.get_instance.cache_clear()
        return cls.get_instance()

class AsyncRequestHandler:
    def __init__(self):
        self.amzn_req = AmznReqManager.get_instance()
        self.urls = {}
        self._cookie_refresh_lock = asyncio.Lock()
        self._cookies_last_refresh = None
        self._cookie_valid = False
        self.max_retries = 3
        self.base_delay = 1


    async def _make_request_with_retry(self, name: str, url: str, headers: Optional[Dict] = None) -> Dict[str, Any]:
        """Make request with retry logic and exponential backoff"""
        for attempt in range(self.max_retries):
            try:
                if headers:
                    response = await asyncio.to_thread(
                        lambda: self.amzn_req.requests(url, headers=headers)
                    )
                else:
                    response = await asyncio.to_thread(
                        lambda: self.amzn_req.requests(url, verify=False, allow_redirects=True)
                    )
                print(f"{name} Status Code: {response.status_code}")
                return {
                    'status_code': response.status_code,
                    'content': response.json() if response.headers.get('content-type') == 'application/json' 
                              else response.text
                }

            except (ConnectionError, RequestException) as e:
                delay = self.base_delay * (2 ** attempt)  # Exponential backoff
                logger.warning(f"Request failed for {name} (attempt {attempt + 1}/{self.max_retries}): {str(e)}")
                
                if attempt < self.max_retries - 1:
                    logger.info(f"Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)
                    
                    # Refresh cookies before retry if it's a Workforce or Process request
                    if name in ("Workforce", "Process"):
                        await self._ensure_valid_cookies(url)
                else:
                    logger.error(f"Max retries reached for {name}")
                    raise


    async def _ensure_valid_cookies(self, url: str) -> bool:
        """Ensures valid cookies exist, refreshing only if necessary"""
        async with self._cookie_refresh_lock:
            # Chack if cookies are still valid
            if self._cookies_last_refresh:
                cookie_age = dt.now() - self._cookies_last_refresh
                if cookie_age.total_seconds() > 3600:  # 1 hour expiration
                    self._cookie_valid = False

            
            if self._cookie_valid:
                return True

            # Test existing cookies
            try:
                test_result = await asyncio.to_thread(
                    lambda: self._test_workforce_cookies(url)
                )
                if test_result == 200:
                    self._cookie_valid = True
                    return True
            except Exception as e:
                logger.debug(f"Cookie test failed: {str(e)}")

            # If we reach here, we need new cookies
            try:
                logger.info("Refreshing cookies with Selenium...")
                selenium_cookies = await asyncio.to_thread(
                    WebDriverManager.get_cookies_with_selenium
                )
                
                if selenium_cookies:
                    await asyncio.to_thread(
                        lambda: self.amzn_req.import_cookies_from_selenium(selenium_cookies)
                    )
                    self._cookie_valid = True
                    self._cookies_last_refresh = dt.now()
                    logger.info("Cookie refresh successful")
                    return True
                else:
                    logger.error("Failed to obtain new cookies")
                    return False
            except Exception as e:
                logger.error(f"Cookie refresh failed: {str(e)}")
                return False

    async def stream_requests(self):
        """Stream responses as they become available"""
        try:
            self.urls = build_urls()
            # Create all request tasks immediately
            tasks = {
                asyncio.create_task(self._make_request(name, url)): name 
                for name, url in self.urls.items()
            }
            
            # Yield responses as soon as they complete
            while tasks:
                done, _ = await asyncio.wait(
                    tasks.keys(),
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                for task in done:
                    name = tasks[task]
                    try:
                        response = await task
                        yield name, response
                    except Exception as e:
                        logger.error(f"Request failed for {name}: {str(e)}")
                        yield name, None
                    finally:
                        tasks.pop(task)

        except Exception as e:
            logger.error(f"Error in stream_requests: {str(e)}")

    async def _make_request(self, name: str, url: str):
        """Handle individual requests using amzn_req's session"""
        try:
            time_manager = TimeManager.get_instance()
            shift_info = time_manager.get_shift_info()
            site_code = shift_info['site_code']

            logger.info(f'Making request for {name}...')
            print(f'Making request for {name}...')
            logger.info(f"URL: {url}")
            if name in ("Workforce", "Process"):
                # Ensure valid cookies exist before proceeding
                if not await self._ensure_valid_cookies(url):
                    raise Exception(f"Failed to ensure valid cookies for {name} request")

                headers = {
                    'Accept': 'application/json',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Connection': 'keep-alive',
                    'Content-Type': 'application/json',
                    'Host': 'picking-console.na.picking.aft.a2z.com',
                    'Referer': f'https://picking-console.na.picking.aft.a2z.com/fc/{site_code}',
                    'Sec-Fetch-Dest': 'empty',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Site': 'same-origin',
                    'TE': 'trailers',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0'
                }
                
                return await self._make_request_with_retry(name, url, headers)
            else:
                return await self._make_request_with_retry(name, url)

            """
                response = await asyncio.to_thread(
                    lambda: self.amzn_req.requests(url, headers=headers)
                )
            else:
                response = await asyncio.to_thread(
                    lambda: self.amzn_req.requests(url, verify=False, allow_redirects=True)
                )
                

            return {
                'status_code': response.status_code,
                'content': response.json() if response.headers.get('content-type') == 'application/json' 
                          else response.text
            }
            """

        except Exception as e:
            logger.error(f"Error in _make_request for {name}: {str(e)}")
            raise

    def _test_workforce_cookies(self, url: str) -> Optional[int]:
        """Test if current cookies are valid for workforce endpoint"""
        try:
            headers = {
                'Accept': 'application/json',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
            }
            
            response = self.amzn_req.requests(url, headers=headers)
            return response.status_code
            
        except Exception as e:
            logger.debug(f"Cookie test failed: {str(e)}")
            return None