from collections import OrderedDict
import os
import sys
import json
import pandas as pd
import polars as pl
import traceback
from datetime import datetime as dt, timedelta as td




sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.config.constants import USER
from src.config.chronos import TimeManager
from src.config.res_finder import ResourceFinder
find_resource = ResourceFinder.find_resource
from src.utils.logger import CustomLogger
logger = CustomLogger.get_logger(__name__)
#logger.error(f"Some Error: {str(e)}")
#logger.info("Some Info")




class SiteBuilder:
    """Dependant on ETL Job:
    JOB: https://datacentral.a2z.com/dw-platform/servlet/dwp/template/EtlViewExtractJobs.vm/job_profile_id/13080516
    SHARE: \\ant\dept-na\KRB4\Public\SDC Pick Zones\SDCPickZones.txt
    """
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SiteBuilder, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        # Only initialize once
        if not SiteBuilder._initialized:
            try:
                self._time_manager = TimeManager.get_instance()
                self._shift_info = self._time_manager.get_shift_info()

                self._site_code = self._shift_info['site_code']
                self._timezone = self._shift_info['timezone']
                self._shift_start = self._shift_info['shift_start']
                self._total_hours = self._shift_info['total_hours']
                self._time_remaining = self._shift_info['hours_remaining']
                self._time_passed = self._shift_info['elapsed_time']
                
                self._pick_areas = None  # Will hold loaded site configuration
                self._plan_data = None  # Will hold loaded plan data
                
                self.runs = 0 # Will be used to force plan data refresh

                logger.info(f"SiteBuilder Initialized:\n- Site Code: {self._site_code}\n- Shift Start: {self._shift_start}")
                SiteBuilder._initialized = True
            except Exception as e:
                logger.error(f"Error initializing SiteBuilder: {str(e)}")
                raise

    @classmethod
    def get_instance(cls, new=False):
        """
        Get singleton instance of SiteBuilder.
        
        Args:
            new (bool): If True, creates a new instance regardless of existing one
            
        Returns:
            SiteBuilder: Instance of SiteBuilder
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



    def get_site_info(self, new=False):
        """Returns a dictionary containing current site information"""
        if new:
            self._plan_data = None

        if self._pick_areas is None:
            logger.info('No pick areas loaded, attempting to load')
            self.load_pick_areas()


        if self._plan_data is None:
            logger.info('No plan data loaded, attempting to load')
            self._plan_data = self.load_recent_plan()


        return {
            'site_code': self._site_code,
            'pick_areas': self._pick_areas,
            'plan_data': self._plan_data
        }


    def load_recent_plan(self):
        """
        Attempts to load the most recent plan file from the last hour before shift start.
        Updates instance plan_data and returns the loaded data.
        Returns None if no valid plan is found.
        """

        logger.info('=== Starting load_recent_plan ===')

        plan_data = None

        try:    
            site_code = self._site_code
            shift_start = self._shift_start
            logger.info(f'Initial shift_start: {shift_start}')
            logger.info(f'shift_start type: {type(shift_start)}')
            logger.info(f'shift_start tzinfo: {shift_start.tzinfo if shift_start else "None"}')

            if shift_start is None:
                shift_start = dt.now()
                logger.info(f'Shift Start was None, set to current time: {shift_start}')
            
            if shift_start.tzinfo is not None:
                one_hour_before = (shift_start.replace(tzinfo=None) - td(hours=1))
                logger.info(f'Shift Start had TZ, stripped and calculated one_hour_before: {one_hour_before}')
            else:
                one_hour_before = shift_start - td(hours=1)
                logger.info(f'Shift Start had no TZ, calculated one_hour_before: {one_hour_before}')
            
            # Log time window we're searching in
            logger.info(f'Searching for plans between: {one_hour_before} and {shift_start}')
            
            # Try network path first
            try:
                # Build and log path
                base_path = r"\\ant\dept-na\SAV7\Public"
                pickassist_path = os.path.join(base_path, "PickAssist")
                plan_dir = os.path.join(pickassist_path, "Plans", site_code)
                logger.info(f'Looking for plans in directory: {plan_dir}')
                
                if not os.path.exists(plan_dir):
                    logger.warning(f'Plan directory does not exist: {plan_dir}')
                    raise FileNotFoundError(f'Network directory not found: {plan_dir}')
                
                # Process files
                plan_files = []
                file_count = 0
                json_count = 0
                matching_count = 0
                

                logger.info('=== Starting file search ===')
                for file in os.listdir(plan_dir):
                    file_count += 1
                    if not file.endswith('.json'):
                        continue
                    
                    json_count += 1
                    file_path = os.path.join(plan_dir, file)
                    file_time = dt.fromtimestamp(os.path.getmtime(file_path))
                    
                    logger.debug(f'Processing file: {file}')
                    logger.debug(f'File modification time: {file_time}')
                    
                    # Ensure file_time is naive
                    if file_time.tzinfo is not None:
                        file_time = file_time.replace(tzinfo=None)
                        logger.debug('Stripped timezone from file_time')
                    
                    if one_hour_before <= file_time <= dt.now().replace(tzinfo=None):
                        plan_files.append((file_path, file_time))
                        matching_count += 1
                        logger.debug(f'File matches time window: {file}')
                
                logger.info(f'File search complete. Total files: {file_count}, JSON files: {json_count}, Matching files: {matching_count}')
                
                if not plan_files:
                    logger.warning('No matching plan files found in network location')
                    # Clean up old files
                    try:
                        files_to_delete = []
                        current_time = dt.now()
                        
                        # First identify files older than 24 hours
                        for file in os.listdir(plan_dir):
                            if file.endswith('.json'):
                                file_path = os.path.join(plan_dir, file)
                                file_time = dt.fromtimestamp(os.path.getmtime(file_path))
                                
                                # Only delete files older than 12 hours
                                if (current_time - file_time).total_seconds() > 43200:  # 12 hours in seconds
                                    files_to_delete.append(file_path)
                        
                        if files_to_delete:
                            logger.info(f"Found {len(files_to_delete)} files older than 12 hours")
                            
                            # Delete the identified files
                            deleted_count = 0
                            for file_path in files_to_delete:
                                try:
                                    os.remove(file_path)
                                    deleted_count += 1
                                    logger.info(f"Deleted old plan file: {os.path.basename(file_path)}")
                                except OSError as e:
                                    logger.error(f"Failed to delete {os.path.basename(file_path)}: {e}")
                            
                            logger.info(f"Cleaned up {deleted_count} old plan files from {plan_dir}")
                        else:
                            logger.info("No old files found to clean up")
                        
                    except Exception as e:
                        logger.error(f"Error while cleaning up plan directory: {e}\n\nTraceback:\n{traceback.format_exc()}")
                    
                    raise FileNotFoundError('No matching plan files found')
                
                # Get most recent file
                most_recent = max(plan_files, key=lambda x: x[1])[0]
                logger.info(f'Selected most recent file: {most_recent}')
                
                try:
                    with open(most_recent, 'r') as f:
                        logger.info(f'Reading plan file: {most_recent}')
                        plan_data = json.load(f)
                        
                        # Log key plan data fields
                        logger.info('=== Plan Data Summary ===')
                        if isinstance(plan_data, dict):
                            logger.info(f"Timestamp: {plan_data.get('timestamp', 'Not found')}")
                            logger.info(f"Submitted by: {plan_data.get('submitted_by', 'Not found')}")
                            logger.info(f"Site code: {plan_data.get('site_code', 'Not found')}")
                            if 'plan' in plan_data:
                                plan = plan_data['plan']
                                logger.info(f"Plan details - Volume: {plan.get('volume')}, Rate: {plan.get('rate')}, "
                                        f"Hours: {plan.get('hours')}, HC: {plan.get('hc')}")
                        
                        logger.info('=== Full Plan Data ===')
                        logger.info(f'Plan Data: {json.dumps(plan_data, indent=2)}')
                        
                        logger.info('Successfully loaded plan data from network')
                        #self.display_readonly_plan(plan_data)
                        
                        self._plan_data = plan_data
                        return plan_data
                        
                except Exception as e:
                    logger.error(f"Error reading network plan file: {str(e)}")
                    raise

            except Exception as e:
                logger.warning(f'Failed to access network path: {str(e)}')

                logger.info('Showing input fields')

                return None
                
        except Exception as e:
            logger.error(f"Error in load_recent_plan: {str(e)}")
            return None

        finally:
            # Update instance data when plan is loaded successfully
            if plan_data:
                self._plan_data = plan_data
                logger.info('Plan data successfully loaded and stored in instance')
            logger.info('=== Completed load_recent_plan ===')



 
    def load_pick_areas(self):
        """
        Loads site information from network path first, falling back to local storage.
        Converts pick_areas to DataFrame and sets self.pick_areas.
        Always ensures self.pick_areas is a DataFrame.
        
        Returns:
            bool: True if loading was successful from either location, False otherwise
        """
        success = False
        logger.info('=== Starting load_pick_areas ===')

        # Initialize empty DataFrame with expected columns
        self._pick_areas = pl.DataFrame()

        # Try network path first
        try:
            network_path = "//ant/dept-na/SAV7/Public/PickAssist/Areas"
            file_name = f"{self._site_code}_site_info.json"
            if os.path.exists(network_path):
                file_path = os.path.join(network_path, file_name)
                
                if os.path.exists(file_path):
                    # Sort files by modification time (newest first)
                    latest_file = file_path
                    
                    logger.info(f"Loading from network path: {latest_file}")
                    with open(latest_file, 'r') as f:
                        self.site_info = json.load(f)
                        
                        # Convert pick_areas dictionary to DataFrame
                        if self.site_info['pick_areas']:  # Only convert if there are pick areas
                            pick_areas_list = [
                                {'Name': name, **data} 
                                for name, data in self.site_info['pick_areas'].items()
                            ]
                            self._pick_areas = pl.DataFrame(pick_areas_list)
                            logger.info(f"Successfully loaded pick areas from network for {self._site_code}")
                            logger.info(self._pick_areas)

                            # Save to local file
                            try:
                                resource_path = find_resource(os.path.join("site_info", f"{self._site_code}_site_info.json"))
                                
                                # Ensure directory exists
                                os.makedirs(os.path.dirname(resource_path), exist_ok=True)
                                
                                # Write the site_info to local file
                                with open(resource_path, 'w') as f:
                                    json.dump(self.site_info, f, indent=4)
                                logger.info(f"Successfully saved network data to local file: {resource_path}")
                            
                            except Exception as e:
                                logger.error(f"Failed to save to local file: {str(e)}\n\nTraceback:\n{traceback.format_exc()}")
                                # Don't return False here as we still successfully loaded from network


                            success = True
                            logger.info('=== Completed load_pick_areas ===')
                            return success
                        else:
                            logger.info("No pick areas found in network file")
                else:
                    logger.warning(f"No JSON files found in network path: {network_path}")
                    
        except Exception as e:
            logger.error(f"Error loading from network: {str(e)}\n\nTraceback:\n{traceback.format_exc()}")
            logger.info("Falling back to local storage")

        # If network load failed, try local file
        try:
            resource_path = find_resource(os.path.join("site_info", f"{self._site_code}_site_info.json"))
            if os.path.exists(resource_path):
                logger.info(f"Loading from local path: {resource_path}")
                with open(resource_path, 'r') as f:
                    self.site_info = json.load(f)
                    
                    # Convert pick_areas dictionary to DataFrame
                    if self.site_info['pick_areas']:  # Only convert if there are pick areas
                        pick_areas_list = [
                            {'Name': name.upper(), **data} 
                            for name, data in self.site_info['pick_areas'].items()
                        ]
                        self._pick_areas = pl.DataFrame(pick_areas_list).with_columns([
                            pl.col('Start Aisle').cast(pl.Int64),
                            pl.col('End Aisle').cast(pl.Int64),
                            pl.col('Start Slot').cast(pl.Int64),
                            pl.col('End Slot').cast(pl.Int64)
                        ])
                        logger.info(f"Successfully loaded pick areas from local file for {self._site_code}")
                        logger.info(self._pick_areas)
                        success = True
                        logger.info('=== Completed load_pick_areas ===')
                        return success
                    else:
                        logger.info("No pick areas found in local file")
            else:
                logger.warning("No local file found")
                
        except Exception as e:
            logger.error(f"Error loading from local storage: {str(e)}")

        # If both loads failed or no pick areas found, ensure site_info exists
        if not success:
            self._pick_areas = pl.DataFrame()
            logger.warning("Using empty pick areas DataFrame as no data could be loaded")
            self.empty_pick_areas = True
        
        logger.info('=== Completed load_pick_areas ===')
        return success
    

    def get_site_timezone(self, site_code):
        """
        Returns the timezone for the site based on site code
        """
        timezone_mapping = {
            'AFT2': 'America/New_York',
            'ATL7': 'America/New_York',
            'AVP8': 'America/New_York',
            'FTW8': 'America/Chicago',
            'HGR5': 'America/New_York',
            'HWA4': 'America/Los_Angeles',
            'KRB1': 'America/Los_Angeles',
            'KRB2': 'America/New_York',
            'KRB3': 'America/Phoenix',
            'KRB4': 'America/Los_Angeles',
            'KRB6': 'America/Chicago',
            'KRB9': 'America/Phoenix',
            'MDT9': 'America/New_York',
            'QXX6': 'America/Chicago',
            'SAV7': 'America/New_York'
        }
        return timezone_mapping.get(site_code)

    def refresh_sites_info (self, local_dir=None):
        """
        Processes warehouse and pick area data from an Excel file and writes the information
        to JSON files for each warehouse.

        Args:
            local_dir (str): The local directory where the JSON files will be saved.

        Returns:
            dict: The data structure containing warehouse and pick area information.
        """

        try:
            # Network directory (Thanks @adnls)
            # https://datacentral.a2z.com/dw-platform/servlet/dwp/template/EtlViewExtractJobs.vm/job_profile_id/13080516
            input_dir = r"\\ant\dept-na\KRB4\Public\SDC Pick Zones"
            input_file = os.path.join(input_dir, "SDCPickZones.txt")
                
            # input_dir = r"/mnt/pickzones"
            # input_file = os.path.join(input_dir, "SDCPickZones.txt")

            print(f"Attempting to access input file: {input_file}")
                
            if not os.path.exists(input_file):
                raise FileNotFoundError(f"Input file not found: {input_file}")
            
            # Copy to PickAssist folder
            copy_dir = r"\\ant\dept-na\SAV7\Public\PickAssist\Areas\Master"

            # copy_dir = r"/mnt/pickassist/Areas/Master"                                                                                                                                                                                

            print(f"Checking/creating directory: {copy_dir}")
            
            try:
                if not os.path.exists(copy_dir):
                    os.makedirs(copy_dir, exist_ok=True)
                    print(f"Created directory: {copy_dir}")
                else:
                    print(f"Directory already exists: {copy_dir}")
            except PermissionError as pe:
                print(f"Permission denied when creating directory: {copy_dir}")
                raise pe
            except Exception as e:
                print(f"Error creating directory {copy_dir}: {str(e)}")
                raise e

            # Generate network filename with timestamp
            timestamp = dt.now().strftime('%Y%m%d_%H%M')
            network_filename = f"sites_{timestamp}.json"
            network_file_path = os.path.join(copy_dir, network_filename)
            print(f"Generated network file path: {network_file_path}")

            # Read the space-separated text file
            print("Attempting to read input file...")
            try:
                df = pd.read_csv(input_file, sep='\t', header=None, 
                                names=['warehouse_id', 'pick_area', 'aisle_number', 'min_bin_number', 'max_bin_number'])
                print(f"Successfully read input file. Found {len(df)} rows of data.")
            except pd.errors.EmptyDataError:
                print(f"Error: The file {input_file} is empty")
                raise
            except pd.errors.ParserError as pe:
                print(f"Error parsing file {input_file}: {str(pe)}")
                raise
            except Exception as e:
                print(f"Unexpected error reading file {input_file}: {str(e)}")
                raise

        except Exception as e:
            print(f"Critical error in file processing: {str(e)}")
            raise

        #df = pd.read_excel(input_file)

        # Group the data by warehouse_id and pick_area
        warehouse_data = {}

        # Process each warehouse and its pick areas
        for warehouse_id, warehouse_group in df.groupby('warehouse_id'):
            if warehouse_id != self._site_code:
                # Skip if warehouse_id does not match the site code
                continue
            # Initialize with ordered fields
            warehouse_data[warehouse_id] = OrderedDict([
                ("site_code", warehouse_id),
                ("timezone", self.get_site_timezone(warehouse_id)),
                ("pick_areas", {})
            ])
            
            print(f'Running for {warehouse_id}')
            # Process each pick area within the warehouse
            for pick_area, area_group in warehouse_group.groupby('pick_area'):
                # Convert to numeric and get min/max values
                print(f'- Running for {pick_area}')
                
                # Helper function to convert to integer if possible
                def safe_int_convert(value):
                    try:
                        # First convert to float to handle decimal strings, then to int
                        return str(int(float(value))) if value and str(value).strip() else 0
                    except (ValueError, TypeError):
                        return str(value)

                min_aisle = safe_int_convert(area_group['aisle_number'].min())
                max_aisle = safe_int_convert(area_group['aisle_number'].max())
                min_bin = safe_int_convert(area_group['min_bin_number'].min())
                max_bin = safe_int_convert(area_group['max_bin_number'].max())
                
                # Create pick area key (similar to paVNA01 format)
                area_key = f"{pick_area}"
                
                # Create pick area data structure
                warehouse_data[warehouse_id]["pick_areas"][area_key] = OrderedDict([
                    ("Start Aisle", min_aisle),
                    ("End Aisle", max_aisle),
                    ("Start Slot", min_bin),
                    ("End Slot", max_bin),
                    ("Cluster", "")
                ])

        # Dump the full data structure to the network directory
        with open(network_file_path, 'w') as f:
            json.dump(warehouse_data, f, indent=4)

        # Write separate JSON files for each warehouse in the local directory

        for warehouse_id, data in warehouse_data.items():
            if warehouse_id != self._site_code:
                # Skip if warehouse_id does not match the site code
                continue

            if local_dir is None:
                local_dir = r"\\ant\dept-na\SAV7\Public\PickAssist\Areas"
                #local_dir = r"/mnt/pickassist/Areas"

                if not os.path.exists(local_dir):
                    os.makedirs(local_dir, exist_ok=True)

                os.makedirs(local_dir, exist_ok=True)
                filename = os.path.join(local_dir, f"{warehouse_id}_site_info.json")
                with open(filename, 'w') as f:
                    json.dump(data, f, indent=4)

                # Reset To None
                local_dir = None
            else:
                os.makedirs(local_dir, exist_ok=True)
                filename = os.path.join(local_dir, f"{warehouse_id}_site_info.json")
                with open(filename, 'w') as f:
                    json.dump(data, f, indent=4)
        
        
        return warehouse_data