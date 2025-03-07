
from datetime import datetime as dt
from datetime import timedelta as td
import os
import sys
import pytz
from dataclasses import dataclass
from typing import Optional

# Module Path Fix
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.utils.logger import CustomLogger
from src.config.constants import TZ_MAPPING

logger = CustomLogger.get_logger(__name__)
#logger.error(f"Some Error: {str(e)}")
#logger.info("Some Info")

@dataclass
class ShiftTime:
    start: dt
    end: dt
    current: dt
    timezone: str
    tz: str
    total_hours: td
    elapsed_time: td
    progress_percent: float
    progress: float
    hours_remaining: td
    formatted_time_remaining: str
    current_millis: int
    start_millis: int
    end_millis: int


class TimeManager:
    _instance = None
    _initialized = False
    _shift_time = None
    _tz = None
    site_code = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TimeManager, cls).__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls, new=False):
        """
        Get singleton instance of TimeManager.
        Args:
            new (bool): If True, creates a new instance regardless of existing one
        Returns:
            TimeManager: Instance of TimeManager
        """
        if new:
            # Reset initialization flag so __init__ will run again
            cls._initialized = False
            # Clear existing instance
            if cls._instance is not None:
                cls._instance = None
        
        if cls._instance is None:
            cls._instance = cls()
            
        return cls._instance
    
    @classmethod
    def update_shift(cls):
        """
        Updates the shift time based on the current time.
        Returns:
            ShiftTime: Updated shift time object
        """
        if cls._shift_time is None:
            logger.error("Shift time not initialized")
            return

        site_code = cls.site_code
        start_hour = cls._shift_time.start.hour
        end_hour = cls._shift_time.end.hour

        success = cls.setup_shift(site_code, start_hour, end_hour)
        if success:
            logger.info("Shift updated successfully")
        else:
            logger.error("Failed to update shift")


    @classmethod
    def setup_shift(cls, site_code, start_hour, end_hour):
        """
        Sets up the shift with automatic AM/PM determination.
        
        Args:
            site_code (str): Site code (e.g., "SAV7")
            start_hour (int): Hour to start shift (1-12)
            end_hour (int): Hour to end shift (1-12)
        
        Returns:
            bool: True if setup was successful, False otherwise
        """
        try:
            # Initialize site and timezone
            if not cls.initialize_site(site_code): 
                logger.error(f"Invalid site code: {site_code}")
                return False


            logger.info(f"Setting up shift for site {site_code} with start hour {start_hour} and end hour {end_hour}")
                
            # Set shift times
            cls._shift_time = cls.calculate_shift_times(
                start_hour, end_hour
            )
            
            if cls._shift_time:
                logger.info(f"Shift setup successful for site {site_code}")
                return True
            return False

        except ValueError as e:
            logger.error(f"Error setting up shift: {str(e)}")
            return False

    @classmethod
    def initialize_site(cls, site_code: str) -> bool:
        """Initialize the time manager with a site code"""
        cls.site_code = site_code
        timezone = cls.get_site_timezone()  
        
        if timezone:
            cls.set_timezone(timezone)  
            return True
        return False


    @classmethod
    def set_timezone(cls, timezone: str):
        """Set the timezone for all time calculations"""
        cls._tz = pytz.timezone(timezone)
        logger.info(f"CURRENT TIMEZONE: {timezone}")



    @classmethod
    def calculate_shift_times(cls, start_hour, end_hour):
        """
        Handles shift times using 24-hour format (0-23)
        
        Args:
            start_hour (int): Hour to start shift (0-23)
            end_hour (int): Hour to end shift (0-23)
        """
        tz = cls._tz
        now = dt.now(pytz.utc).astimezone(tz)

        # Create times for today
        start_time = now.replace(hour=start_hour, minute=0, second=0, microsecond=0)
        end_time = now.replace(hour=end_hour, minute=0, second=0, microsecond=0)

        # If end time is before start time, move it to next day
        if end_time <= start_time:
            end_time += td(days=1)


        # Ensure times are timezone-aware
        if not start_time.tzinfo:
            start_time = tz.localize(start_time)
        if not end_time.tzinfo:
            end_time = tz.localize(end_time)

        # Calculate times milliseconds
        current_millis = cls.convertToMilli(now, cls._tz)
        start_millis = cls.convertToMilli(start_time - td(hours=1), cls._tz)
        end_millis = cls.convertToMilli(end_time + td(hours=21), cls._tz)

        # Calculate times
        total_hours = end_time - start_time

        # Added safeguard to protect against 24+ hour edge case
        if total_hours.total_seconds() > 86400:  # 86400 seconds = 24 hours
            # Adjust the end_time to be same day as start_time
            end_time = end_time - td(days=1)
            total_hours = end_time - start_time
        elif total_hours.total_seconds() < 0:
            # If negative, end_time should be later same day
            end_time = end_time + td(days=1)
            total_hours = end_time - start_time

        elapsed_time = now - start_time
        hours_remaining = end_time - now

        formatted_time_remaining = cls.format_timedelta((total_hours - elapsed_time))

        total_seconds = total_hours.total_seconds()
        elapsed_seconds = elapsed_time.total_seconds()
        progress = (elapsed_seconds / total_seconds)
        progress_percent = progress * 100



        logger.info(f"Time Variables Calculated\nShift Times: {start_time.strftime("%H:%M:%S")} - {end_time.strftime("%H:%M:%S")}\nMillisecond Times: {start_millis} - {end_millis}\nTotal Hours: {total_hours}\nElapsed Time: {elapsed_time}\nHours Remaining: {hours_remaining}\nCurrent Time: {now.strftime("%H:%M:%S")}")
        print(f"\nTime Variables Calculated\nShift Times: {start_time.strftime("%H:%M:%S")} - {end_time.strftime("%H:%M:%S")}\nMillisecond Times: {start_millis} - {end_millis}\nTotal Hours: {total_hours}\nElapsed Time: {elapsed_time}\nHours Remaining: {hours_remaining}\nCurrent Time: {now.strftime("%H:%M:%S")}\n\n")

        return ShiftTime(
            start=start_time,
            end=end_time,
            current=now,
            timezone=cls._tz.zone,
            tz=cls._tz,
            total_hours=total_hours,
            elapsed_time=elapsed_time,
            progress_percent=progress_percent,
            progress=progress,
            hours_remaining=hours_remaining,
            formatted_time_remaining=formatted_time_remaining,
            current_millis=current_millis,
            start_millis=start_millis,
            end_millis=end_millis
        )

    
    @staticmethod
    def convertToMilli(dateTime, tz):
        """
        Converts a datetime object to milliseconds since a specific epoch.

        Args:
            dateTime (datetime): The datetime object to convert.

        Returns:
            int: The time in milliseconds since the epoch.
        """
        
        epoch = dt(1899, 12, 30, tzinfo=tz)
        delta = dateTime - epoch
        xldt = float(delta.days) + (float(delta.seconds) / 86400)
        milli = round((xldt * 24 * 60 * 60 * 1000) - 2209136340000)
        return milli
    
    @staticmethod
    def _convert_to_24hr(hour: int, am_pm: str) -> int:
        """Convert 12-hour format to 24-hour format"""
        if am_pm.lower() == 'pm' and hour != 12:
            return hour + 12
        elif am_pm.lower() == 'am' and hour == 12:
            return 0
        return hour

    @staticmethod
    def format_timedelta(td):
        """
        Format a timedelta object into a human-readable string.

        Args:
            td (timedelta): The timedelta object to format.

        Returns:
            str: The formatted timedelta string.
        """

        # Check if the timedelta is negative
        is_negative = td.total_seconds() < 0
        if is_negative:
            td = -td  # Make the timedelta positive for formatting

        # Calculate days, hours, and minutes
        total_seconds = td.total_seconds()
        days = int(total_seconds // (24 * 3600))
        hours = int((total_seconds % (24 * 3600)) // 3600)
        minutes = int((total_seconds % 3600) // 60)

        # Format the string with a negative sign if needed
        formatted_time = f"{days}d {hours}h {minutes}m"
        return f"-{formatted_time}" if is_negative else formatted_time


    @property
    def shift_time(cls) -> Optional[ShiftTime]:
        return cls._shift_time
    
    @classmethod
    def get_shift_info(cls) -> dict:
        """Get all current shift information in a dictionary format"""
        if not cls._shift_time:
            return {}
        
        # Create timezone-aware current time
        now = dt.now(pytz.UTC).astimezone(cls._tz)
        return {
            'site_code': cls.site_code,
            'timezone': cls._shift_time.timezone,
            'tz' : cls._tz,
            'shift_start': cls._shift_time.start,
            'shift_end': cls._shift_time.end,
            'now' : now,
            'current_time': cls._shift_time.current,
            'total_hours': cls._shift_time.total_hours,
            'elapsed_time': cls._shift_time.elapsed_time,
            'progress_percent': cls._shift_time.progress_percent,
            'progress': cls._shift_time.progress,
            'hours_remaining': cls._shift_time.hours_remaining,
            'formatted_time_remaining' : cls._shift_time.formatted_time_remaining,
            'start_millis' : cls._shift_time.start_millis,
            'end_millis' : cls._shift_time.end_millis
        }
    

    @classmethod
    def get_site_timezone(cls) -> Optional[str]:
        """Returns the timezone for the site based on site code"""
        timezone_mapping = TZ_MAPPING
        return timezone_mapping.get(cls.site_code)
