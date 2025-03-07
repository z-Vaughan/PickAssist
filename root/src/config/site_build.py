import os
import sys
import json
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
            self.load_recent_plan()


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
                            logger.info(f"Found {len(files_to_delete)} files older than 24 hours")
                            
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
                        return plan_data
                        
                except Exception as e:
                    logger.error(f"Error reading network plan file: {str(e)}")
                    raise

            except Exception as e:
                logger.warning(f'Failed to access network path: {str(e)}\n\nTraceback:\n{traceback.format_exc()}')
                logger.info('Attempting to load from local storage')
                try:
                    # Try to load from local storage
                    local_path = find_resource(f"{site_code}_shift_plan.json")
                    logger.info(f'Checking local path: {local_path}')
                    
                    if os.path.exists(local_path):
                        with open(local_path, 'r') as f:
                            plan_data = json.load(f)
                            logger.info('Successfully loaded plan from local storage')
                            logger.debug(f'Local plan data: {json.dumps(plan_data, indent=2)}')
                            #cls.display_readonly_plan(plan_data)
                            
                            return plan_data
                    else:
                        logger.warning('No local plan file found')
                        logger.info('Showing input fields')
                        #cls.display_plan_input_fields()
                        return None
                        
                except Exception as local_e:
                    logger.error(f'Error loading from local storage: {str(local_e)}')
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
    