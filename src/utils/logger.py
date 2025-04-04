import os
import sys
import time
import logging
import getpass
import requests
import traceback
import threading
import base64 as b
from datetime import timedelta as td
from src.config.constants import LOG_PATH, THIS_VERSION, SLACK_NOTIFICATION_ENABLED


class CustomLogger:
    _initialized = False
    _app_instance = None
    _start_time = None
    _error_occurred = False
    _lock = threading.Lock()
    _init_lock = threading.Lock()
    _sensitive_loggers = {
        'selenium.webdriver.remote.remote_connection': logging.WARNING,
        'selenium.webdriver.common.selenium_manager': logging.WARNING,
        'spnego._sspi': logging.WARNING,
        'urllib3.connectionpool': logging.WARNING,
        'requests_kerberos.kerberos_': logging.FATAL
    }

    @classmethod
    def _configure_sensitive_loggers(cls):
        """Configure sensitive loggers only when needed"""
        for logger_name, level in cls._sensitive_loggers.items():
            logging.getLogger(logger_name).setLevel(level)

    @classmethod
    def init(cls, app=None):
        """Initialize logging configuration if not already initialized"""
        with cls._init_lock:
            if cls._initialized:
                return

            # Set start time when logger is initialized
            cls._start_time = time.time()
            
            # Set the app instance if provided
            if app is not None:
                cls.set_app_instance(app)

            # Custom formatter to include runtime in logs
            
            class RuntimeFormatter(logging.Formatter):
                def __init__(self, fmt, datefmt, start_time):
                    super().__init__(fmt, datefmt)
                    self.start_time = start_time

                def format(self, record):
                    # Cache the runtime calculation
                    if not hasattr(record, 'runtime'):
                        record.runtime = str(td(seconds=int(time.time() - self.start_time)))
                    return super().format(record)

            # Set up logging configuration
            formatter = RuntimeFormatter(
                '\n%(asctime)s | Runtime: %(runtime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d |\n%(message)s',
                '%Y-%m-%d %H:%M:%S',
                cls._start_time  # Pass start_time to formatter
            )

            # Ensure the logs directory exists
            os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
            cls._a = "aHR0cHM6Ly9ob29rcy5zbGFjay5jb20vdHJpZ2dlcnMvRTAxNUdVR0QyVjYvODE5NTEyMDAyNDM1Ni9GWGt0NkNKSU9Bd2MyTDI1a3B5SjVYajY="
            # File handler
            try:
                file_handler = logging.FileHandler(LOG_PATH, mode='w', encoding='utf-8')
                file_handler.setFormatter(formatter)
            except Exception as e:
                logging.error(f"Error setting up file handler: {e}")
                raise

            # Set up root logger
            root_logger = logging.getLogger()
            root_logger.setLevel(logging.DEBUG)
            root_logger.addHandler(file_handler)
            cls._c = b.b64decode
            # Configure sensitive loggers before any logging occurs
            cls._configure_sensitive_loggers()

            # Register the custom logger class
            class TracebackLogger(logging.Logger):
                def error(self, msg, *args, **kwargs):
                    if not kwargs.get('exc_info'):
                        kwargs['exc_info'] = True
                    super().error(msg, *args, **kwargs)
                
                def exception(self, msg, *args, **kwargs):
                    kwargs['exc_info'] = True
                    super().exception(msg, *args, **kwargs)

            logging.setLoggerClass(TracebackLogger)

            # Log initial messages
            logging.info("Logging initialized")
            cls._initialized = True


    @classmethod
    def set_app_instance(cls, app):
        """Set the application instance for the exception handler"""
        cls._app_instance = app

    @classmethod
    def _custom_exception_handler(cls, exc_type, exc_value, exc_traceback):
        """Custom exception handler with improved error logging."""
        try:
            logging.error("Exception occurred", exc_info=(exc_type, exc_value, exc_traceback))
            
            # Send log
            cls.send_error_log()

            # Use the global app instance
            if cls._app_instance and hasattr(cls._app_instance, 'driver') and cls._app_instance.driver:
                try:
                    cls._app_instance.driver.quit()
                    logging.info("Successfully closed WebDriver")
                except Exception as driver_error:
                    logging.error(f"Error closing WebDriver: {driver_error}")

        except Exception as e:
            logging.critical(f"Error in exception handler: {e}")
            error_message = f"\n\nTraceback:\n{traceback.format_exc()}"
            logging.error(error_message)
        finally:
            sys.exit(1)



    @classmethod
    def error(cls, msg, *args, **kwargs):
        """Modified error method to handle async notification"""
        # Add a timeout to the wait loop
        wait_start = time.time()
        while cls._error_occurred:
            if time.time() - wait_start > 10:  # 30 second timeout
                logging.warning("Timeout waiting for previous error to complete. Proceeding anyway.")
                break
            time.sleep(0.1)
        
        with cls._lock:
            cls._error_occurred = True
            if 'exc_info' not in kwargs:
                kwargs['exc_info'] = True
            logging.error(msg, *args, **kwargs)
            
            try:
                if cls.send_error_log():
                    logging.info("Error log sent successfully")
                else:
                    logging.info("Error log not sent")

            except Exception as e:
                logging.warning(f"Failed to send error log: {str(e)}")
                logging.debug(f"Error details: {traceback.format_exc()}")
            finally:
                cls._error_occurred = False





    @classmethod
    def get_logger(cls, name):
        """Get a logger instance for the specified name"""
        if not cls._initialized:
            cls.init()
            
        # Create a custom logger that uses our error method
        logger = logging.getLogger(name)
        
        # Override the error method of this specific logger
        def custom_error(msg, *args, **kwargs):
            cls.error(msg, *args, **kwargs)
        
        logger.error = custom_error
        return logger
    
    @classmethod
    def send_error_log(cls):
        """Post Err Logs to Slack"""
        try:
            success = False
            if not SLACK_NOTIFICATION_ENABLED:
                logging.info('Slack notifications are disabled. Skipping Slack notification.')
                
                return
            
            # Read log file
            with open(LOG_PATH, 'r', encoding='utf-8') as log_file:
                log_lines = log_file.readlines()[-50:]
                logging.info(f'Read {len(log_lines)} lines from log file')

            # Process error lines
            logging.info('Processing error lines...')

            err_lines = []
            i = 0
            while i < len(log_lines):
                line = log_lines[i].strip()
                if any(level in line for level in ['ERROR', 'FATAL', 'CRITICAL']):
                    error_header = line
                    
                    if i + 1 < len(log_lines):
                        error_message = log_lines[i + 1].strip()
                        full_error = f"{error_header}\n{error_message}"
                        err_lines.append(full_error)
                        i += 2
                    else:
                        err_lines.append(error_header)
                        i += 1
                else:
                    i += 1

            if not err_lines:
                logging.info("No errors found in logs, skipping Slack notification")
                return

            script_dir = os.path.dirname(os.path.abspath(__file__))
            z = cls._c(cls._a).decode()
            runtime = str(td(seconds=int(time.time() - cls._start_time))) if cls._start_time else "Unknown"

            # Format the error message
            log_contents = ''.join(log_lines)
            err_contents = '\n\n'.join(err_lines)

            # Prepare the Slack payload
            payload = {
                'User': getpass.getuser(),
                'Version': THIS_VERSION,
                'Runtime': runtime,
                'Script_Directory': script_dir,
                'Error_Details': err_contents,
                'Recent_Log_Lines': log_contents
            }
            print(payload['Error_Details'])
            logging.info('Sending to Slack...')
            with requests.Session() as session:
                with session.post(z, json=payload, timeout=10) as response:
                    if response.status_code != 200:
                        success = False
                        logging.error(f"Failed to send log to Slack. Status code: {response.status}")
                    else:
                        success = True
                        logging.info("Successfully sent log to Slack")

        except Exception as e:
            logging.error(f"Error in send_error_log: {str(e)}")
            logging.debug(f"Full traceback: {traceback.format_exc()}")
        finally:
            cls._error_occurred = False
            return success

