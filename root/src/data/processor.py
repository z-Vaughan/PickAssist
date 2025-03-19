import os
import re
import sys
import pytz
import lxml
import json
import asyncio
import html5lib
import polars as pl
import pandas as pd
from datetime import datetime as dt
from typing import Dict, Any, List, Optional



# Module Path Fix
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.config.chronos import TimeManager
from src.data.areq import AsyncRequestHandler
from src.config.site_build import SiteBuilder
from src.utils.logger import CustomLogger
from src.config.constants import PAD_TIME

logger = CustomLogger.get_logger(__name__)
#logger.error(f"Some Error: {str(e)}")
#logger.info("Some Info")


class DataProcessor:
    _instance = None
    _initialized = False
    


    def __init__(self):
        self.shift_info = TimeManager.get_instance().get_shift_info()
        self.site_info = SiteBuilder.get_instance().get_site_info()

        self.site_code = self.shift_info['site_code']
        self.timezone = self.shift_info['timezone']
        self.pick_areas = self.site_info['pick_areas']
        
        self.request_handler = AsyncRequestHandler()
        self.processed_data = {}

        DataProcessor._initialized = True



    @classmethod
    def get_instance(cls) -> 'DataProcessor':
        """Get singleton instance of DataProcessor"""
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset_instance(cls):
        """Reset the singleton instance"""
        cls._instance = None
        cls._initialized = False



    def get_results(self) -> Dict[str, Any]:
        """
        Get the latest processed data as JSON object.

        Returns:
            Dict[]: A dictionary containing the processed data.

            {
                "LPI": {\n
                    "hov": {\n
                        "hov_rate": int,\n
                        "hov_vol": int,\n
                        "hov_hrs": int\n
                    },\n
                    "non_hov": {\n
                        "non_hov_rate": int,\n
                        "non_hov_vol": int,\n
                        "non_hov_hrs": int\n
                    },\n
                    "combined": {\n
                        "combined_rate": int,\n
                        "combined_vol": int,\n
                        "combined_hrs": int\n
                    },\n
                    "lpi_full": pl.DataFrame\n
                    "lpi_process_summary": pl.Dataframe\n
                },\n
                "Rodeo": {\n
                    "picks": {\n
                        "non_hov_picks_rem": int,\n
                        "hov_pick_rem": int,\n
                        "all_picks_rem": int\n
                    },\n
                    "rodeo_full": pl.DataFrame,\n
                    "cpt_summary": pl.DataFrame,\n
                    "cpt_process_summary": pl.DataFrame,\n
                    "cpt_process_area_summary": pl.DataFrame\n
                },\n
                "Workforce": {\n
                    "headcounts": {\n
                        "total_headcount": int,\n
                        "active_headcount": int\n
                    },\n
                    "workforce_full": pl.DataFrame\n
                },\n
                "Process": {\n
                    "process_full": pl.DataFrame\n
                },\n
                "combined_df": {\n
                    "cpt_level": str,\n
                    "process_level": pl.DataFrame,\n
                    "area_level": pl.DataFrame\n
                }\n
            }
        """
        return self.processed_data

    async def process_incoming_data(self):
        """Process data streams with maximum concurrency"""
        processing_tasks = {}
    

        try:
            # Start processing each response as soon as it arrives
            async for name, response in self.request_handler.stream_requests():
                if response is not None:
                    logger.info(f"Received {name} data :: Status Code: {response['status_code']}")
                    
                    # Create processing task immediately
                    processing_tasks[name] = asyncio.create_task(
                        self._route_processing(name, response['content'])
                    )
                else:
                    logger.warning(f"Received None response for {name}")

            # Wait for all processing to complete only at the end
            if processing_tasks:
                # Wait for all tasks to complete
                results = await asyncio.gather(
                    *processing_tasks.values(), 
                    return_exceptions=True
                )
                
                # Store results maintaining order
                for name, result in zip(processing_tasks.keys(), results):
                    if isinstance(result, Exception):
                        logger.error(f"Error processing {name}: {str(result)}")
                    else:
                        self.processed_data[name] = result
                        logger.info(f"Successfully processed {name} data")
                        
                        
                # Now that all data is processed, perform merge operations
                
                processing_tasks.clear()  # Remove all tasks from the dictionary

                try:
                    if all(key in self.processed_data for key in ['Workforce', 'Rodeo', 'LPI', 'Process']):
                        required_keys = {
                            'Workforce': 'workforce_full',
                            'Rodeo': 'rodeo_full',
                            'LPI': 'lpi_full',
                            'Process': 'process_full'
                        }
                        missing_keys = [key for key, sub_key in required_keys.items() if key not in self.processed_data or self.processed_data[key][sub_key].height < 1]

                        # Granularity: CPT[Process Path[Pick Area[Bin]]]

                        # Rodeo:
                            # Highest Level: CPT
                            # Lowest Level: Bin
                            # Focus: CPTs, Process/Case demand, and Density
                        # Workforce:
                            # Highest Level: Process Path
                            # Lowest Level: Pick Area
                            # Focus: Associate activity
                        # LPI:
                            # Highest Level: Process Path
                            # Lowest Level: Pick Area
                            # Focus: Volume, Labor, and Rates
                        # Process:
                            # Highest Level: Process Path
                            # Lowest Level: Process Path
                            # Focus: Process/Unit demand

                        # Best Case: All Info (CPT/Path/Area/Bin Level)
                            # AVAILABLE INFO:
                            # - CPTs, Process/Case Demand, Density, Process/Unit Demand,
                            #   Labor Hours, Processed Volume, Rates, Associate Activity

                        # No Rodeo: Workforce, LPI, and Process (Path/Area Level)
                            # - No CPTs, Case demand, or Density (out of work or external connection)
                            # AVAILABLE INFO:
                            # - Process/Unit demand, Labor Hours, Processed Volume, 
                            #   Rates, Associate Activity

                        # No Workforce: Rodeo, and LPI(CPT/Path/Area Level)
                            # - No Associate Activity (pre-shift or break)
                            # AVAILABLE INFO:
                            # - CPTs, Case Demand, Labor Hours, Processed Volume, Rates

                        # No LPI: Rodeo, Workforce, and Process (CPT/Path/Area/Bin Level)
                            # - No Labor Hours, Processed Volume, or Rates (pre-shift)
                            # AVAILABLE INFO:
                            # - CPTs, Process/Case Demand, Density, Process/Unit Demand,
                            #   Associate Activity

                        # No Process: Rodeo, Workforce, and LPI (Path/Area Level, Labor and Units)
                            # - No Process/Unit Demand (out of work or picking console down)
                            # AVAILABLE INFO:
                            # - CPTs, Case Demand, Labor Hours, Processed Volume, Rates,
                            #   Associate Activity

                        # No Rodeo = Out of Work or External Connection
                        # No Workforce = Pre-Shift or Break
                        # No LPI = Pre-Shift (rerun for historical?)
                        # No Process = Out of Work or Bad Connection
                        # No Workforce AND No LPI = Pre-Shift
                        
                        # Functional Cases
                         # - No LPI and No Workforce
                         # - No Workforce
                         # - No Rodeo
                        
                        elapsed_hours = self.shift_info['elapsed_time'].seconds / 3600

                        if not missing_keys:

                            logger.info("All required data is present")
                            process_level_merge = (
                                # Start with Rodeo data as base
                                self.processed_data['Rodeo']['cpt_process_summary']
                                # Join with Workforce data
                                .join(
                                    self.processed_data['Workforce']['process_summary'],
                                    on=['process_path'],
                                    how='left'
                                )
                                # Join with LPI data
                                .join(
                                    self.processed_data['LPI']['process_summary'],
                                    on=['process_path'],
                                    how='left'
                                )
                                .join(
                                    self.processed_data['LPI(Hist)']['process_summary_hist'].select(
                                        ['process_path', pl.col('avg_cph').alias('historical_cph')]
                                    ),
                                    on=['process_path'],
                                    how='left'
                                )
                                .with_columns([


                                    # Calculate Process-Level Alignments
                                    pl.struct(['total_hours', 'hours_remaining', 'total_cases', 'avg_cph'])
                                        .map_elements(lambda x: self._calculate_alignment(
                                            time_spent=x['total_hours'],
                                            time_passed=elapsed_hours,
                                            time_remaining=x['hours_remaining'],
                                            work_remaining=x['total_cases'],
                                            current_rate=x['avg_cph']
                                        ))
                                        .alias('projected_miss'),

                                   # Calculate Picker Rate Average (PRA)
                                    ((pl.when(pl.col('total_hours') > 0)
                                        .then((pl.col('total_cases') / pl.col('total_hours')) * pl.col('case_density'))
                                        .otherwise(0))
                                        .round(2)
                                        .alias('PRA'))
                                ])
                                .with_columns([
                                    # Calculate Target Unit Rate (TUR)
                                    (pl.when((pl.col('total_pickers') > 0) & (pl.col('PRA').is_not_null()))
                                        .then(pl.col('PRA') / pl.col('total_pickers'))
                                        .otherwise(0))
                                        .round(2)
                                        .alias('TUR')
                                ])
                                
                            )

                            # Pulled from .with_columns above
                            """# Calculate Process-Level Target Headcounts using coalesce
                                    (pl.col('total_cases') / (
                                        pl.when(pl.col('hours_remaining') - PAD_TIME > 0)
                                        .then(pl.col('hours_remaining') - PAD_TIME)
                                        .otherwise(pl.col('hours_remaining')) * 
                                        pl.coalesce([
                                            pl.col('avg_cph'),  # Try current CPH first
                                            pl.col('historical_cph'),  # Fall back to historical CPH
                                            pl.lit(40)  # Default value if both are null
                                        ])
                                    ))
                                    .round(2)
                                    .alias('target_hc'),"""


                            # Join with Process data
                            if 'Process' not in missing_keys:
                                process_level_merge = (
                                    process_level_merge.join(
                                    self.processed_data['Process']['process_full'],
                                    on=['process_path'],
                                    how='left'
                                    )
                                )

                            # Pick Area Level Merge
                            area_level_merge = (
                                # Start with Rodeo data as base
                                self.processed_data['Rodeo']['cpt_process_area_summary']
                                # Join with Workforce data
                                .join(
                                    self.processed_data['Workforce']['process_area_summary'],
                                    on=['process_path', 'pick_area'],
                                    how='left'
                                )
                                # Join with LPI data
                                .join(
                                    self.processed_data['LPI']['process_area_summary'],
                                    on=['process_path', 'pick_area'],
                                    how='left'
                                )
                            
                                .join(
                                    self.processed_data['LPI(Hist)']['process_area_summary_hist'].select(
                                        ['process_path', 'pick_area', pl.col('avg_cph').alias('historical_cph')]
                                    ),
                                    on=['process_path', 'pick_area'],
                                    how='left'
                                )
                                    
                            )

                            """.with_columns([
                                    # Calculate Process-Level Target Headcounts using coalesce
                                    (pl.col('total_cases') / (
                                        pl.col('hours_remaining') * 
                                        pl.coalesce([
                                            pl.col('avg_cph'),  # Try current CPH first
                                            pl.col('historical_cph'),  # Fall back to historical CPH
                                            pl.lit(40)  # Default value if both are null
                                        ])
                                    ))
                                    .round(2)
                                    .alias('target_hc')
                                ])"""
                            



                            # Store merged results
                            self.processed_data['combined_data'] = {
                                'cpt_level' : 'See Rodeo[cpt_summary]',
                                'process_level': process_level_merge,
                                'area_level': area_level_merge
                            }

                        else:
                            logger.warning(f"Missing data: {missing_keys}")
                            # Functional Cases
                            # - No LPI and No Workforce
                            # - No Workforce
                            # - No Rodeo
                                
                            if 'LPI' in missing_keys and 'Workforce' in missing_keys and 'Rodeo' not in missing_keys:
                                # - No LPI and No Workforce
                                logger.warning("No LPI and No Workforce data to merge\nPre-shift: Focusing Demand")

                                process_level_merge = (
                                    # Start with Rodeo data as base
                                    self.processed_data['Rodeo']['cpt_process_summary']
                                    # No data to join
                                    .with_columns([
                                        # Add null-filled rate columns
                                        pl.lit(None).cast(pl.Float64).alias('avg_cph'),
                                        pl.lit(None).cast(pl.Float64).alias('historical_cph'),
                                        pl.lit(0).cast(pl.Float64).alias('total_pickers'),
                                        pl.lit(0).cast(pl.Float64).alias('active_pickers'),
                                        pl.lit(0).cast(pl.Float64).alias('cases_picked'),
                                        pl.lit(0).cast(pl.Float64).alias('total_hours'),
                                        pl.lit(False).cast(pl.Boolean).alias('projected_miss'),
                                        pl.lit(0).cast(pl.Float64).alias('PRA'),
                                        pl.lit(0).cast(pl.Float64).alias('TUR')
                                    ])
                                    
                                )
                                """.with_columns([
                                        # Calculate Process-Level Target Headcounts using coalesce
                                        (pl.col('total_cases') / (
                                            pl.when(pl.col('hours_remaining') - PAD_TIME > 0)
                                            .then(pl.col('hours_remaining') - PAD_TIME)
                                            .otherwise(pl.col('hours_remaining')) * 
                                            pl.coalesce([
                                                pl.col('avg_cph'),  # Try current CPH first
                                                pl.col('historical_cph'),  # Fall back to historical CPH
                                                pl.lit(40)  # Default value if both are null
                                            ])
                                        ))
                                        .round(2)
                                        .alias('target_hc'),
                                    ])"""

                                # Join with Process data
                                if 'Process' not in missing_keys:
                                    process_level_merge = (
                                        process_level_merge.join(
                                        self.processed_data['Process']['process_full'],
                                        on=['process_path'],
                                        how='left'
                                        )
                                    )

                                # Pick Area Level Merge
                                area_level_merge = (
                                    # Start with Rodeo data as base
                                    self.processed_data['Rodeo']['cpt_process_area_summary']
                                    
                                    # No data to join
                                    .with_columns([
                                        # Add null-filled rate columns
                                        pl.lit(None).cast(pl.Float64).alias('avg_cph'),
                                        pl.lit(None).cast(pl.Float64).alias('historical_cph'),
                                        pl.lit(0).cast(pl.Float64).alias('area_hc'),
                                        pl.lit(0).cast(pl.Float64).alias('area_active_hc'),
                                        pl.lit(0).cast(pl.Float64).alias('PRA'),
                                        pl.lit(0).cast(pl.Float64).alias('TUR')
                                    ])
                                        
                                )

                                """.with_columns([
                                        # Calculate Process-Level Target Headcounts using coalesce
                                        (pl.col('total_cases') / (
                                            pl.col('hours_remaining') * 
                                            pl.coalesce([
                                                pl.col('avg_cph'),  # Try current CPH first
                                                pl.col('historical_cph'),  # Fall back to historical CPH
                                                pl.lit(40)  # Default value if both are null
                                            ])
                                        ))
                                        .round(2)
                                        .alias('target_hc')
                                    ])"""

                                # Store merged results
                                self.processed_data['combined_data'] = {
                                    'cpt_level' : 'See Rodeo[cpt_summary]',
                                    'process_level': process_level_merge,
                                    'area_level': area_level_merge
                                }
                            


                            elif 'Workforce' in missing_keys and 'LPI' not in missing_keys and 'Rodeo' not in missing_keys:
                                # - No Workforce
                                logger.warning("No Workforce data to merge\nBreak: Focusing Demand/Progress")
                                process_level_merge = (
                                    # Start with Rodeo data as base
                                    self.processed_data['Rodeo']['cpt_process_summary']

                                    # Join with LPI data
                                    .join(
                                        self.processed_data['LPI']['process_summary'],
                                        on=['process_path'],
                                        how='left'
                                    )
                                    .join(
                                        self.processed_data['LPI(Hist)']['process_summary_hist'].select(
                                            ['process_path', pl.col('avg_cph').alias('historical_cph')]
                                        ),
                                        on=['process_path'],
                                        how='left'
                                    )

                                    .with_columns([
                                        # Add null-filled rate columns
                                        pl.lit(0).cast(pl.Float64).alias('total_pickers'),
                                        pl.lit(0).cast(pl.Float64).alias('active_pickers'),
                                    ])

                                    .with_columns([

                                        # Calculate Process-Level Alignments
                                        pl.struct(['total_hours', 'hours_remaining', 'total_cases', 'avg_cph'])
                                            .map_elements(lambda x: self._calculate_alignment(
                                                time_spent=x['total_hours'],
                                                time_passed=elapsed_hours,
                                                time_remaining=x['hours_remaining'],
                                                work_remaining=x['total_cases'],
                                                current_rate=x['avg_cph']
                                            ))
                                            .alias('projected_miss'),

                                    # Calculate Picker Rate Average (PRA)
                                        ((pl.when(pl.col('total_hours') > 0)
                                            .then((pl.col('total_cases') / pl.col('total_hours')) * pl.col('case_density'))
                                            .otherwise(0))
                                            .round(2)
                                            .alias('PRA'))
                                    ])
                                    .with_columns([
                                        # Calculate Target Unit Rate (TUR)
                                        (pl.when((pl.col('total_pickers') > 0) & (pl.col('PRA').is_not_null()))
                                            .then(pl.col('PRA') / pl.col('total_pickers'))
                                            .otherwise(0))
                                            .round(2)
                                            .alias('TUR')
                                    ])
                                    
                                )
                                # Pulled from .with_columns above
                                """# Calculate Process-Level Target Headcounts using coalesce
                                        (pl.col('total_cases') / (
                                            pl.when(pl.col('hours_remaining') - PAD_TIME > 0)
                                            .then(pl.col('hours_remaining') - PAD_TIME)
                                            .otherwise(pl.col('hours_remaining')) * 
                                            pl.coalesce([
                                                pl.col('avg_cph'),  # Try current CPH first
                                                pl.col('historical_cph'),  # Fall back to historical CPH
                                                pl.lit(40)  # Default value if both are null
                                            ])
                                        ))
                                        .round(2)
                                        .alias('target_hc'),"""


                                # Join with Process data
                                if 'Process' not in missing_keys:
                                    process_level_merge = (
                                        process_level_merge.join(
                                        self.processed_data['Process']['process_full'],
                                        on=['process_path'],
                                        how='left'
                                        )
                                    )

                                # Pick Area Level Merge
                                area_level_merge = (
                                    # Start with Rodeo data as base
                                    self.processed_data['Rodeo']['cpt_process_area_summary']

                                    # Join with LPI data
                                    .join(
                                        self.processed_data['LPI']['process_area_summary'],
                                        on=['process_path', 'pick_area'],
                                        how='left'
                                    )
                                
                                    .join(
                                        self.processed_data['LPI(Hist)']['process_area_summary_hist'].select(
                                            ['process_path', 'pick_area', pl.col('avg_cph').alias('historical_cph')]
                                        ),
                                        on=['process_path', 'pick_area'],
                                        how='left'
                                    )

                                    .with_columns([
                                        # Add null-filled rate columns
                                        pl.lit(0).cast(pl.Float64).alias('area_hc'),
                                        pl.lit(0).cast(pl.Float64).alias('area_active_hc'),
                                    ])

                                )

                                """.with_columns([
                                        # Calculate Process-Level Target Headcounts using coalesce
                                        (pl.col('total_cases') / (
                                            pl.col('hours_remaining') * 
                                            pl.coalesce([
                                                pl.col('avg_cph'),  # Try current CPH first
                                                pl.col('historical_cph'),  # Fall back to historical CPH
                                                pl.lit(40)  # Default value if both are null
                                            ])
                                        ))
                                        .round(2)
                                        .alias('target_hc')
                                    ])"""
                                        
                                



                                # Store merged results
                                self.processed_data['combined_data'] = {
                                    'cpt_level' : 'See Rodeo[cpt_summary]',
                                    'process_level': process_level_merge,
                                    'area_level': area_level_merge
                                }



                            elif 'Rodeo' in missing_keys and 'LPI' not in missing_keys and 'Workforce' not in missing_keys:
                                # - No Rodeo
                                logger.warning("No Rodeo data to merge\nNodeo: Focusing Workforce")
                        
                                # Process Path Level Merge
                                process_level_merge = (
                                    # Start with Workforce
                                    self.processed_data['Workforce']['process_summary']
                                    # Join with LPI data
                                    .join(
                                        self.processed_data['LPI']['process_summary'],
                                        on=['process_path'],
                                        how='left'
                                    )
                                )
                                # Join with Process data
                                if 'Process' not in missing_keys:
                                    process_level_merge = (
                                        process_level_merge.join(
                                        self.processed_data['Process']['process_full'],
                                        on=['process_path'],
                                        how='left'
                                        )
                                    )
                                

                                # Pick Area Level Merge
                                area_level_merge = (
                                    # Start with Rodeo data as base
                                    self.processed_data['Workforce']['process_area_summary']
                                
                                    # Join with LPI data
                                    .join(
                                        self.processed_data['LPI']['process_area_summary'],
                                        on=['process_path', 'pick_area'],
                                        how='left'
                                    )
                                )

                            else:
                                # - No Workforce AND No LPI
                                logger.warning("Not enough information to merge")

                                # Process Path Level Merge
                                process_level_merge = pl.DataFrame()

                                # Pick Area Level Merge
                                area_level_merge = pl.DataFrame()

                            # Store merged results
                            self.processed_data['combined_data'] = {
                                'cpt_level' : 'No Rodeo',
                                'process_level': process_level_merge,
                                'area_level': area_level_merge
                            }

                        # Update last update timestamp
                        logger.info("Completed all data level merges")
                        return self.processed_data

                except Exception as e:
                    logger.error(f"Error merging datasets: {str(e)}")
                
        except Exception as e:
            logger.error(f"Error processing incoming data: {str(e)}")

            

    async def _route_processing(self, name: str, data: Any) -> Optional[pl.DataFrame]:
        """Route data to appropriate processor"""
        processors = {
            # Normalize the provided data to DataFrames
            "Workforce": self._normalize_workforce,
            "Process": self._normalize_process,
            "LPI": self._normalize_lpi,
            "LPI(Hist)": self._normalize_lpi_hist,
            "Rodeo": self._normalize_rodeo
        }
        
        processor = processors.get(name)
        if processor:
            try:
                return await processor(data)
            except Exception as e:
                logger.error(f"Error routing {name}: {str(e)}")
                return None
        return None


 #####    #####  
 ##   #  ##
 ##   #  ##
 #####   ##
 ##      ##
 ##       #####
#PICKING CONSOLE
    async def _normalize_workforce(self, data: Dict):
        """Process Workforce data concurrently"""
        if 'pickerStatusList' not in data or not data['pickerStatusList']:
            logger.warning("No workforce data available in response.")
            workforce = {"workforce_full" : pl.DataFrame()}
            return workforce
        try:
            # Create DataFrame processing task
            df_task = asyncio.create_task(asyncio.to_thread(
                lambda: pl.DataFrame(data)
                    .lazy()
                    .unnest('pickerStatusList')
                    .rename(self._get_column_renames('Workforce'))
                    .with_columns([
                        pl.col('process_path').str.to_uppercase(),
                        pl.col('pick_area').str.to_uppercase()
                    ])
                    .collect()
            ))

            # Get DataFrame result
            df = await df_task

            print(f"Workforce : Normalized Data")
            logger.info(f"Workforce : Normalized data")

            # Start grouping as soon as DataFrame is ready
            grouping_task = asyncio.create_task(self._group_workforce(df))
            
            # Get grouping results
            grouped_workforce = await grouping_task
            if grouped_workforce is not None:

                return grouped_workforce
                #self.processed_data.update(grouped_workforce)


        except Exception as e:
            logger.error(f"Workforce normalizing error: {str(e)}")

    async def _group_workforce(self, df: pl.DataFrame):
        """Group and aggregate Workforce data"""
        # TODO return headcounts object on fail
        # headcounts: {
        #    total_headcount: 0
        #    active_headcount: 0
        #}
        try:

            # Check for empty or missing data
            if df.height < 1:
                logger.warning("No workforce data available to group.")
                return pl.DataFrame()

            logger.debug(f"Workforce : Grouping Data")
            print(f"Workforce : Grouping Data")

            

            process_summary = df.group_by('process_path').agg([
                # Headcount metrics
                pl.col('employee_id').count().alias("total_pickers"),

                # Activity metrics
                pl.when(pl.col('active') == True)
                    .then(pl.col('employee_id'))
                    .count()
                    .alias("active_pickers"),
            ])

            # Add derived calculations
            process_summary = process_summary.with_columns([
                (pl.col("active_pickers") / pl.col("total_pickers") * 100)
                    .round(2)
                    .alias("active_percent"),
            ]).sort('process_path')


            # Second level grouping by Pick Area within Process Path
            logger.debug(f"Grouping by Pick Area within Process Path")
            process_area_summary = df.group_by([
                'process_path', 'pick_area'
            ]).agg([
                pl.col('employee_id').count().alias("area_hc"),
                pl.col('active').sum().alias("area_active_hc"),
            ]).sort('process_path', 'pick_area')

            # Add derived calculations
            process_area_summary = process_area_summary.with_columns([
                (pl.col("area_active_hc") / pl.col("area_hc") * 100)
                    .round(2)
                    .alias("active_percent"),
            ]).sort('process_path')

            total_headcount = process_summary.select('total_pickers').sum().item()
            active_headcount = process_summary.select('active_pickers').sum().item()

            print(f"Workforce : Grouped Data")
            logger.info(f"Workforce : Grouped data")
            workforce = {
                "headcounts" : {
                    "total_headcount" : total_headcount,
                    "active_headcount" : active_headcount
                },
                "workforce_full": df,
                "process_summary": process_summary,
                "process_area_summary": process_area_summary,
            }

            return workforce

        except Exception as e:
            logger.error(f"Error in grouping workforce data: {str(e)}\nDataFrame schema: {df.schema}\nDataFrame shape: {df.shape}")
            workforce = {
                "headcounts" : {
                    "total_headcount" : 0,
                    "active_headcount" : 0
                },
                "workforce_full": pl.DataFrame(),
            }
            return workforce
        

    async def _normalize_process(self, data: Dict) -> pl.DataFrame:
        """Process Process data"""
        try:
            # Check for empty or missing data
            if 'processPathInformationMap' not in data or not data['processPathInformationMap']:
                logger.warning("No process data available in response.")

                return {'process_full': pl.DataFrame()}

            # Create DataFrame from dictionary
            df = pl.DataFrame({
                'process_path': list(data['processPathInformationMap'].keys()),
                'data': list(data['processPathInformationMap'].values())
            })

            # Process the data with Polars expressions
            df = await asyncio.to_thread(
                lambda: df.lazy()
                    .with_columns([
                        pl.col('data').struct.field('Status').alias('status'),
                        pl.col('data').struct.field('PickerCount').alias('picker_count'),
                        pl.col('data').struct.field('UnitsInScanner').alias('units_in_scanner'),
                        pl.col('data').struct.field('UnitsPerHour').alias('units_per_hour'),
                        # Looks like they remembered camelCase for these 2
                        pl.col('data').struct.field('pickRateAverage').alias('pick_rate_average'),
                        pl.col('data').struct.field('unitRateTarget').alias('unit_rate_target'),
                        # Sum Prioritized Units
                        pl.col('data').struct.field('PrioritizedUnitsCounts')
                            .map_elements(lambda x: sum(x.values()) if isinstance(x, dict) else 0)
                            .alias('prioritized_units'),
                        # Sum Non-Prioritized Units
                        pl.col('data').struct.field('NonPrioritizedUnitsCounts')
                            .map_elements(lambda x: sum(x.values()) if isinstance(x, dict) else 0)
                            .alias('non_prioritized_units')
                    ])
                    # Filter for Active status
                    .filter(pl.col('status') == 'Active')
                    # Filter where Prioritized Units > 0
                    .filter(pl.col('prioritized_units') > 0)
                    # Sort by Prioritized Units descending
                    .sort('prioritized_units', descending=True)
                    # Select final columns
                    .select([
                        'process_path',
                        'status',
                        'prioritized_units',
                        'non_prioritized_units',
                        'picker_count',
                        'units_in_scanner',
                        'units_per_hour',
                        'pick_rate_average',
                        'unit_rate_target'
                    ])

                    .with_columns([
                        pl.col('process_path').str.to_uppercase()
                    ])

                    .collect()
            )
            print(f"Process: Normalized Data")
            logger.info(f"Process : Normalized data")
            process = {"process_full" : df}
            return process

        except Exception as e:
            logger.error(f"Process normalizing error: {str(e)}")
            return pl.DataFrame()



 ##      #####   ######
 ##      ##   #    ##
 ##      ##   #    ##
 ##      #####     ##
 ##      ##        ##
 ######  ##      ######
#LABOR  PROCESS  INSPECTOR
    async def _normalize_lpi(self, data: str) -> pl.DataFrame:
        """Process LPI data"""
        print(f"Processing LPI data")
        logger.info(f"Processing LPI data")

        try:
            # Extract JSON from LPI response if it's a string
            if isinstance(data, str):
                if 'filteredProductivityList = ' in data:
                    json_str = data.split('filteredProductivityList = ')[1].split(';')[0]
                    data = json.loads(json_str)
                else:
                    logger.error("LPI data doesn't contain expected string pattern\nNo Labor - Check shift times / Authentication")
                    print("LPI data doesn't contain expected string pattern\nNo Labor - Check shift times / Authentication")
                    lpi = {
                        "hov" : {
                            "hov_rate": 0,
                            "hov_vol": 0,
                            "hov_hrs": 0
                        },
                        "non_hov" : {
                            "non_hov_rate": 0,
                            "non_hov_vol": 0,
                            "non_hov_hrs": 0,
                        },
                        "combined" : {
                            "combined_rate": 0,
                            "combined_vol": 0,
                            "combined_hrs": 0
                        },
                        "lpi_full" : pl.DataFrame()}
                    return lpi

            df = pl.DataFrame(data)
            
            # Apply transformations and type casting
            df_task = asyncio.to_thread(
                lambda: df.lazy()
                    .unnest('processAttributes')  # Unnest process attributes
                    .unnest('attributes')         # Unnest the nested attributes
                    .explode('associateProductivityList')  # Explode the list of associates
                    .unnest('associateProductivityList')  # Unnest the associate data
                    .rename(self._get_column_renames('LPI'))  # Rename columns before casting
                    .with_columns(
                        self._get_column_type_casts('LPI')
                    )
                    .drop([
                        'processName','process_id','labor_tracking_type',
                        'gl_code','pick_path_group','work_flow','is_tokenized',
                        'availability','processAttributes'
                    ])

                    .with_columns([
                        # Convert Millis to Hours
                        (pl.col("time_millis") / 3600)
                        .round(2)
                        .alias("time_hours")
                    ])

                    .with_columns([
                        # Calculate Units per Hour
                        (pl.when(pl.col("time_hours") > 0)
                            .then(pl.col("unit_count") / pl.col("time_hours"))
                            .otherwise(0))
                        .round(2)
                        .alias("units_per_hr")
                    ])


                    .collect()
            )
            # Get DataFrame result
            df = await df_task

            print(f"LPI : Normalized Data")
            logger.info(f"LPI : Normalized data")

            # Start grouping as soon as DataFrame is ready
            grouping_task = asyncio.create_task(self._group_lpi(df))
    
            # Get grouping results
            grouped_lpi = await grouping_task
            if grouped_lpi is not None:
                return grouped_lpi
                
                   
        except Exception as e:
            logger.error(f"LPI normalizing error: {str(e)}\nTraceback: ", exc_info=True)
            return pl.DataFrame()

    async def _group_lpi(self, df: pl.DataFrame):
        """Group and aggregate LPI data"""
        #TODO return Non/HOV data on fail
        """ "hov" : {
                "hov_rate": 0,
                "hov_vol": 0,
                "hov_hrs": 0
            },
            "non_hov" : {
                "non_hov_rate": 0,
                "non_hov_vol": 0,
                "non_hov_hrs": 0,
            },
            "combined" : {
                "combined_rate": 0,
                "combined_vol": 0,
                "combined_hrs": 0
            },"""
        try:
            if df['process_path'].str.contains('PPHOVRESERVE').any():

                hov_rate = (
                    df.lazy()
                    .filter(pl.col('process_path').str.contains('PPHOVRESERVE'))
                    .select([
                        (pl.col('unit_count').sum() / pl.col('time_hours').sum()).round(2).alias("avg_cph")
                    ])
                    .collect()
                    .get_column('avg_cph')[0]
                )
                hov_vol = (
                    df.lazy()
                    .filter(pl.col('process_path').str.contains('PPHOVRESERVE'))
                    .select([pl.col('unit_count').sum().alias("vol")])
                    .collect()
                    .get_column('vol')[0]
                )
                hov_hrs = (
                    df.lazy()
                    .filter(pl.col('process_path').str.contains('PPHOVRESERVE'))
                    .select([pl.col('time_hours').sum().round(2).alias("hrs")])
                    .collect()
                    .get_column('hrs')[0]
                )

            else:

                hov_rate = 0
                hov_vol = 0
                hov_hrs = 0

            # Invert our filter with '~' to catch the other side
            non_hov_rate = (
                df.lazy()
                .filter(~pl.col('process_path').str.contains('PPHOVRESERVE'))
                .select([
                    (pl.col('unit_count').sum() / pl.col('time_hours').sum()).round(2).alias("avg_cph")
                ])
                .collect()
                .get_column('avg_cph')[0]
            )
            non_hov_vol = (
                df.lazy()
                .filter(~pl.col('process_path').str.contains('PPHOVRESERVE'))
                .select([pl.col('unit_count').sum().alias("vol")])
                .collect()
                .get_column('vol')[0]
            )
            non_hov_hrs = (
                df.lazy()
                .filter(~pl.col('process_path').str.contains('PPHOVRESERVE'))
                .select([pl.col('time_hours').sum().round(2).alias("hrs")])
                .collect()
                .get_column('hrs')[0]
            )


            combined_rate = (
                df.lazy()
                .select([
                    (pl.col('unit_count').sum() / pl.col('time_hours').sum()).round(2).alias("avg_cph")
                ])
                .collect()
                .get_column('avg_cph')[0]
            )
            combined_vol = (
                df.lazy()
                .select([pl.col('unit_count').sum().alias("vol")])
                .collect()
                .get_column('vol')[0]
            )
            combined_hrs = (
                df.lazy()
                .select([pl.col('time_hours').sum().round(2).alias("hrs")])
                .collect()
                .get_column('hrs')[0]
            )

            # First level grouping by Process Path
            logger.debug(f"LPI : Grouping by process path")
            process_summary = df.group_by('process_path').agg([
                # Volume / Hours / Rate
                pl.col('unit_count').sum().alias("cases_picked"),
                pl.col('time_hours').sum().alias("total_hours").round(2),

                # Method 1 (arithmetic mean of rates):
                # (100 + 100 + 100) / 3 = 100 units/hr
                pl.col('units_per_hr').mean().alias("mean_cph").round(2),

                # Method 2 (total units / total hours): *preferred*
                # (100 + 50 + 10) / (1 + 0.5 + 0.1) = 160/1.6 = 100 units/hr
                (pl.col('unit_count').sum() / pl.col('time_hours').sum()).round(2).alias("avg_cph")
            ])

            # Second level grouping by Pick Area within Process Path
            logger.debug(f"LPI : Grouping by Pick Area within Process Path")
            process_area_summary = df.group_by([
                'process_path', 'pick_area'
            ]).agg([
                # Volume / Hours / Rate
                pl.col('unit_count').sum().alias("cases_picked"),
                pl.col('time_hours').sum().alias("total_hours").round(2),
                pl.col('units_per_hr').mean().alias("mean_cph").round(2),
                (pl.col('unit_count').sum() / pl.col('time_hours').sum()).round(2).alias("avg_cph")
            ]).sort('process_path', 'pick_area')


            print(f"LPI : Grouped Data")
            logger.info(f"LPI : Grouped data")
            lpi = {
                "hov" : {
                    "hov_rate": hov_rate,
                    "hov_vol": hov_vol,
                    "hov_hrs": hov_hrs
                },
                "non_hov" : {
                    "non_hov_rate": non_hov_rate,
                    "non_hov_vol": non_hov_vol,
                    "non_hov_hrs": non_hov_hrs,
                },
                "combined" : {
                    "combined_rate": combined_rate,
                    "combined_vol": combined_vol,
                    "combined_hrs": combined_hrs
                },
                "lpi_full": df,
                "process_summary": process_summary,
                "process_area_summary": process_area_summary,

            }

            return lpi
        
        except Exception as e:
            logger.error(f"Error in grouping LPI data: {str(e)}\nDataFrame schema: {df.schema}\nDataFrame shape: {df.shape}")
            lpi = {
                "hov" : {
                    "hov_rate": 0,
                    "hov_vol": 0,
                    "hov_hrs": 0
                },
                "non_hov" : {
                    "non_hov_rate": 0,
                    "non_hov_vol": 0,
                    "non_hov_hrs": 0,
                },
                "combined" : {
                    "combined_rate": 0,
                    "combined_vol": 0,
                    "combined_hrs": 0
                },
                "lpi_full": pl.DataFrame(),
            }
            
            return lpi


    async def _normalize_lpi_hist(self, data: str) -> pl.DataFrame:
        """Process LPI data"""
        print(f"Processing Historical LPI data")
        logger.info(f"Processing Historical LPI data")

        try:
            # Extract JSON from LPI response if it's a string
            if isinstance(data, str):
                if 'filteredProductivityList = ' in data:
                    json_str = data.split('filteredProductivityList = ')[1].split(';')[0]
                    data = json.loads(json_str)
                else:
                    logger.error("LPI data doesn't contain expected string pattern\nNo Labor - Check shift times / Authentication")
                    print("LPI data doesn't contain expected string pattern\nNo Labor - Check shift times / Authentication")
                    lpi_hist = {
                        "lpi_full" : pl.DataFrame(),
                        "lpi_process_summary_hist": pl.DataFrame(),
                        "lpi_process_area_summary_hist": pl.DataFrame(),
                    }
                    return lpi_hist

            df = pl.DataFrame(data)
            
            # Apply transformations and type casting
            df_task = asyncio.to_thread(
                lambda: df.lazy()
                    .unnest('processAttributes')  # Unnest process attributes
                    .unnest('attributes')         # Unnest the nested attributes
                    .explode('associateProductivityList')  # Explode the list of associates
                    .unnest('associateProductivityList')  # Unnest the associate data
                    .rename(self._get_column_renames('LPI'))  # Rename columns before casting
                    .with_columns(
                        self._get_column_type_casts('LPI')
                    )
                    .drop([
                        'processName','process_id','labor_tracking_type',
                        'gl_code','pick_path_group','work_flow','is_tokenized',
                        'availability','processAttributes'
                    ])

                    .with_columns([
                        # Convert Millis to Hours
                        (pl.col("time_millis") / 3600)
                        .round(2)
                        .alias("time_hours")
                    ])

                    .with_columns([
                        # Calculate Units per Hour
                        (pl.when(pl.col("time_hours") > 0)
                            .then(pl.col("unit_count") / pl.col("time_hours"))
                            .otherwise(0))
                        .round(2)
                        .alias("units_per_hr")
                    ])


                    .collect()
            )
            # Get DataFrame result
            df = await df_task

            print(f"LPI Historical : Normalized Historical Data")
            logger.info(f"LPI Historical : Normalized Historical data")

            # Start grouping as soon as DataFrame is ready
            grouping_task = asyncio.create_task(self._group_lpi_hist(df))
    
            # Get grouping results
            grouped_lpi = await grouping_task
            if grouped_lpi is not None:
                return grouped_lpi
                
                   
        except Exception as e:
            logger.error(f"LPI Hist normalizing error: {str(e)}\nTraceback: ", exc_info=True)
            return pl.DataFrame()


    async def _group_lpi_hist(self, df: pl.DataFrame):
        """Group and aggregate LPI data"""
        try:

            # First level grouping by Process Path
            logger.debug(f"LPI : Grouping by process path")
            process_summary = df.group_by('process_path').agg([
                # Volume / Hours / Rate
                pl.col('unit_count').sum().alias("cases_picked"),
                pl.col('time_hours').sum().alias("total_hours").round(2),

                # Method 1 (arithmetic mean of rates):
                # (100 + 100 + 100) / 3 = 100 units/hr
                pl.col('units_per_hr').mean().alias("mean_cph").round(2),

                # Method 2 (total units / total hours): *preferred*
                # (100 + 50 + 10) / (1 + 0.5 + 0.1) = 160/1.6 = 100 units/hr
                (pl.col('unit_count').sum() / pl.col('time_hours').sum()).round(2).alias("avg_cph")
            ])

            # Second level grouping by Pick Area within Process Path
            logger.debug(f"LPI : Grouping by Pick Area within Process Path")
            process_area_summary = df.group_by([
                'process_path', 'pick_area'
            ]).agg([
                # Volume / Hours / Rate
                pl.col('unit_count').sum().alias("cases_picked"),
                pl.col('time_hours').sum().alias("total_hours").round(2),
                pl.col('units_per_hr').mean().alias("mean_cph").round(2),
                (pl.col('unit_count').sum() / pl.col('time_hours').sum()).round(2).alias("avg_cph")
            ]).sort('process_path', 'pick_area')


            print(f"LPI Historical : Grouped Data")
            logger.info(f"LPI Historical : Grouped data")
            lpi = {
                "lpi_full": df,
                "process_summary_hist": process_summary,
                "process_area_summary_hist": process_area_summary
            }

            return lpi
        
        except Exception as e:
            logger.error(f"Error in grouping LPI data: {str(e)}\nDataFrame schema: {df.schema}\nDataFrame shape: {df.shape}")
            return None





 #####    ####   
 ##   #  ##  ##     
 ##   #  ##  ##  
 #####   ##  ##  
 ## ##   ##  ##  
 ##  ##   ####   
#RODEO
    async def _normalize_rodeo(self, data: str) -> pl.DataFrame:
        """Process Rodeo data"""


        print(f"Processing Rodeo data")
        logger.info(f"Processing Rodeo data")

        # First, extract numeric values from Start/End Aisle columns
        # Ensure consistent integer types in pick areas
        pick_areas = (
            self.pick_areas
            .select([
                pl.col('Name'),
                pl.col('Start Aisle'),
                pl.col('End Aisle'),
                pl.col('Start Slot'),
                pl.col('End Slot')
            ])
            .with_columns([
                # Convert aisle strings to integers
                pl.col('Start Aisle').str.extract_all(r'\d+').list.first().cast(pl.Int64),
                pl.col('End Aisle').str.extract_all(r'\d+').list.first().cast(pl.Int64),
                # Convert slot strings to integers
                pl.col('Start Slot').cast(pl.Int64),
                pl.col('End Slot').cast(pl.Int64)
            ])
        )
        
        def extract_aisle(id_str: str) -> Optional[int]:
            if not isinstance(id_str, str) or not id_str.startswith('P-1-'):
                return None
            pattern = r'P-1-[A-Z](\d{3})'
            match = re.search(pattern, id_str)
            return int(match.group(1)) if match else None

        def extract_slot(id_str: str) -> Optional[int]:
            if not isinstance(id_str, str) or not id_str.startswith('P-1-'):
                return None
            pattern = r'P-1-[A-Z]\d{3}[A-Z](\d{2,3})'
            match = re.search(pattern, id_str)
            return int(match.group(1)) if match else None
            
        # Find the matching pick area
        def find_pick_area(aisle: Optional[int], slot: Optional[int]) -> Optional[str]:
            if aisle is None or slot is None:
                return None
            
            try:
                # Now comparing integers with integers
                matches = pick_areas.filter(
                    (pl.col('Start Aisle') <= aisle) & 
                    (pl.col('End Aisle') >= aisle) &
                    (pl.col('Start Slot') <= slot) &
                    (pl.col('End Slot') >= slot)
                )
                return matches['Name'][0] if matches.height > 0 else None
                
            except Exception as e:
                logger.error(f"Error in find_pick_area: {str(e)}")
                return None
            
        try:
            # Convert HTML to DataFrame
            df = await asyncio.to_thread(lambda: pl.from_pandas(pd.read_html(data)[0]))
            
            if df.height < 1:
                return {"rodeo_full": pl.DataFrame()}
            df_task = asyncio.to_thread(
                lambda: df.lazy()
                    .drop(['Status', 'Work Pool', 'FN SKU', 'Pick Priority', 'Container Type'])
                    # Extract and process locations - cast to Int64 immediately
                    .with_columns([
                        pl.col('Outer Scannable ID')
                            .map_elements(extract_aisle)
                            .cast(pl.Int64)
                            .alias('primary_aisle'),
                        pl.col('Outer Scannable ID')
                            .map_elements(extract_slot)
                            .cast(pl.Int64)
                            .alias('primary_slot'),
                        pl.col('Outer Outer Scannable ID')
                            .map_elements(extract_aisle)
                            .cast(pl.Int64)
                            .alias('secondary_aisle'),
                        pl.col('Outer Outer Scannable ID')
                            .map_elements(extract_slot)
                            .cast(pl.Int64)
                            .alias('secondary_slot')
                    ])
                    # Coalesce with proper null handling
                    .with_columns([
                        pl.coalesce([
                            pl.col('primary_aisle'),
                            pl.col('secondary_aisle')
                        ]).fill_null(-1).alias('Aisle'),
                        pl.coalesce([
                            pl.col('primary_slot'),
                            pl.col('secondary_slot')
                        ]).fill_null(-1).alias('Slot')
                    ])
                    .drop(['primary_aisle', 'primary_slot', 'secondary_aisle', 'secondary_slot'])

                    .with_columns([
                        pl.struct(['Aisle', 'Slot'])
                            .map_elements(lambda x: find_pick_area(x['Aisle'], x['Slot']))
                            .alias('Pick Area')
                    ])

                    # Process remaining columns
                    .with_columns([
                        pl.col('Process Path').str.to_uppercase(),
                        pl.col('Pick Area').str.to_uppercase()
                    ])


                    .sort('Need To Ship By Date')

                    .with_columns([
                        pl.col('Need To Ship By Date')
                            .str.strptime(pl.Datetime, format='%Y-%m-%d %H:%M:%S')
                            .dt.replace_time_zone(self.timezone)
                            .dt.strftime('%m-%d %H:%M')
                            .alias('CPT')
                    ])
                    .rename(self._get_column_renames('Rodeo'))

                    .with_columns([
                        pl.when(pl.col('process_path').str.contains('PPHOVRESERVE'))
                            .then(pl.lit('HOV'))
                            .otherwise(pl.col('cpt'))
                            .alias('cpt')
                    ])



                    .collect()
            )
            

            # Get DataFrame result
            df = await df_task
            if df.height < 1:
                logger.error("Rodeo data is empty")
                return {"rodeo_full": pl.DataFrame()}

            print(f"Rodeo : Normalized Data")
            logger.info(f"Rodeo : Normalized data")


            # Start grouping as soon as DataFrame is ready
            grouping_task = asyncio.create_task(self._group_rodeo(df))
    
            # Get grouping results
            grouped_rodeo = await grouping_task
            if grouped_rodeo is not None:

                return grouped_rodeo
                
                   
        except Exception as e:
            logger.error(f"Rodeo normalizing error: {str(e)}\nTraceback: ", exc_info=True)
            return {"rodeo_full": pl.DataFrame()}

    async def _group_rodeo(self, df: pl.DataFrame):
        """Group and aggregate Rodeo data"""
        def _calculate_hours_remaining(cpt_str, now):    
            # Parse the CPT string (assuming current year)
            current_year = now.year
            try:
                # Convert CPT to timezone aware datetime 
                naive_cpt = dt.strptime(f"{current_year}-{cpt_str}", "%Y-%m-%d %H:%M")
                cpt_time = pytz.timezone(self.timezone).localize(naive_cpt)
                # Calculate the time difference in hours
                time_diff = cpt_time - now
                hours_remaining = time_diff.total_seconds() / 3600
                
                return round(hours_remaining, 2)
            except ValueError as e:
                print(str(e))
                return None

        try:

            non_hov_picks_rem = (
                df.lazy()
                .filter(~pl.col('process_path').str.contains('PPHOVRESERVE'))
                .select([pl.col('transfer_request_id').count().alias("vol")])
                .collect()
                .get_column('vol')[0]
            )

            hov_picks_rem = (
                df.lazy()
                .filter(pl.col('process_path').str.contains('PPHOVRESERVE'))
                .select([pl.col('transfer_request_id').count().alias("vol")])
                .collect()
                .get_column('vol')[0]
            )

            all_picks_rem = (
                df.lazy()
                .select([pl.col('transfer_request_id').count().alias("vol")])
                .collect()
                .get_column('vol')[0]
            )


            # First level grouping by Process Path
            logger.debug(f"Rodeo : Grouping by picks in CPT")
            cpt_summary = df.group_by('cpt').agg([
                # Case / Unit counts
                pl.col('transfer_request_id').count().alias("total_cases"),
                pl.col('quantity').sum().alias("total_units"),
                pl.when(pl.col('process_path').str.contains('PPHOVRESERVE'))
                .then(pl.col('transfer_request_id'))
                .count()
                .alias('hov_cases')
            ])

            cpt_summary = cpt_summary.with_columns([
                # CPT-level hours remaining
                pl.struct(['cpt']).map_elements(
                    lambda x: _calculate_hours_remaining(
                        cpt_str=x['cpt'],
                        now=self.shift_info['now']
                    )
                ).alias('hours_remaining'),

                # Calculate density
                (pl.col('total_units') / pl.col('total_cases'))
                    .round(2)
                    .alias('case_density')
            ]).sort('cpt').with_columns([
                # Fill missing values with max
                pl.col('hours_remaining').fill_null(
                    pl.col('hours_remaining').max()
                ).alias('hours_remaining')
            ])


            # Second level grouping by Process Path within CPT
            logger.debug(f"Rodeo : Grouping by Process Path within CPT")

            cpt_process_summary = df.group_by([
                'cpt', 'process_path'
            ]).agg([
                # Case / Unit counts
                pl.col('transfer_request_id').count().alias("total_cases"),
                pl.col('quantity').sum().alias("total_units"),
            ]).sort('cpt', 'process_path')

            cpt_process_summary = cpt_process_summary.with_columns([
                # CPT-level hours remaining
                pl.struct(['cpt']).map_elements(
                    lambda x: _calculate_hours_remaining(
                        cpt_str=x['cpt'],
                        now=self.shift_info['now']
                    )
                ).alias('hours_remaining'),

                # Calculate density
                (pl.col('total_units') / pl.col('total_cases'))
                    .round(2)
                    .alias('case_density')
            ]).sort('cpt').with_columns([
                # Fill missing values with max
                pl.col('hours_remaining').fill_null(
                    pl.col('hours_remaining').max()
                ).alias('hours_remaining')
            ])


            # Third level grouping by Pick Area within Process Path within CPT
            logger.debug(f"Rodeo : Grouping by Pick Area within Process Path within CPT")
            cpt_process_area_summary = df.group_by([
                'cpt', 'process_path', 'pick_area'
            ]).agg([
                # Case / Unit counts
                pl.col('transfer_request_id').count().alias("total_cases"),
                pl.col('quantity').sum().alias("total_units"),
            ]).sort('cpt', 'process_path', 'pick_area')


            cpt_process_area_summary = cpt_process_area_summary.with_columns([
                # CPT-level hours remaining
                pl.struct(['cpt']).map_elements(
                    lambda x: _calculate_hours_remaining(
                        cpt_str=x['cpt'],
                        now=self.shift_info['now']
                    )
                ).alias('hours_remaining'),

                # Calculate density
                (pl.col('total_units') / pl.col('total_cases'))
                    .round(2)
                    .alias('case_density')
            ]).with_columns([
                # Fill missing values with max
                pl.col('hours_remaining').fill_null(
                    pl.col('hours_remaining').max()
                ).alias('hours_remaining')
            ])

            print(f"Rodeo : Grouped Data")
            logger.info(f"Rodeo : Grouped data")
            rodeo = {
                "picks" : {
                    "non_hov_picks_rem": non_hov_picks_rem,
                    "hov_picks_rem": hov_picks_rem,
                    "all_picks_rem": all_picks_rem
                },
                "rodeo_full": df,
                "cpt_summary": cpt_summary,
                "cpt_process_summary": cpt_process_summary,
                "cpt_process_area_summary": cpt_process_area_summary,
            }

            return rodeo

        except Exception as e:
            logger.error(f"Error in grouping Rodeo data: {str(e)}\nDataFrame schema: {df.schema}\nDataFrame shape: {df.shape}")
            return {"rodeo_full": pl.DataFrame()}
        


# Utility methods

    def _get_column_type_casts(self, name: str) -> List[pl.Expr]:

        if name == 'LPI':
            logger.info(f"LPI : Casting Types")
            return [

                # Attributes
                pl.col('pick_area').cast(pl.Utf8),
                pl.col('process_path').cast(pl.Utf8),
                pl.col('size_category').cast(pl.Utf8),
                
                # Associate data
                pl.col('employee_id').cast(pl.Utf8),
                pl.col('employee_name').cast(pl.Utf8),
                pl.col('manager_id').cast(pl.Utf8),
                pl.col('manager_name').cast(pl.Utf8),
                pl.col('unit_count').cast(pl.Int64),
                pl.col('each_count').cast(pl.Int64),
                pl.col('time_millis').cast(pl.Int64)
            ]
        
        if name == 'Process':
            logger.info(f"Process : Casting Types")
            
            # Basic fields
            casts = [
                pl.col('batch_count').cast(pl.Int64),
                pl.col('container_use_percent').cast(pl.Float64),
                pl.col('pick_process').cast(pl.Utf8),
                pl.col('picker_count').cast(pl.Int64),
                pl.col('status').cast(pl.Utf8),
                pl.col('tote_count').cast(pl.Int64),
                pl.col('units_in_scanner').cast(pl.Int64),
                pl.col('units_in_totes_count').cast(pl.Int64),
                pl.col('units_per_hour').cast(pl.Int64),
                pl.col('pick_rate_average').cast(pl.Float64),
                pl.col('unit_rate_target').cast(pl.Int64),
                pl.col('process_path_name').cast(pl.Utf8)
            ]

            # Priority types for both prioritized and non-prioritized
            priority_suffixes = [
                'fast_track', 'min_priority', 'premium', 
                'same_next', 'standard', 'super_savers'
            ]

            # Add casts for prioritized counts
            casts.extend([
                pl.col(f'prioritized_{suffix}').cast(pl.Int64)
                for suffix in priority_suffixes
            ])

            # Add casts for non-prioritized counts
            casts.extend([
                pl.col(f'non_prioritized_{suffix}').cast(pl.Int64)
                for suffix in priority_suffixes
            ])

            # Time blocks for EXSD
            time_blocks = [
                '0h_2h', '2h_4h', '4h_8h', '8h_16h',
                '16h_24h', '24h_48h', 'gt_48h', 'lt_0h'
            ]

            # Add casts for EXSD C29 counts
            for time_block in time_blocks:
                for suffix in priority_suffixes:
                    casts.append(
                        pl.col(f'exsd_c29_{time_block}_{suffix}').cast(pl.Int64)
                    )

            # Add casts for EXSD C4 counts
            for time_block in time_blocks:
                for suffix in priority_suffixes:
                    casts.append(
                        pl.col(f'exsd_c4_{time_block}_{suffix}').cast(pl.Int64)
                    )

            return casts


    def _get_column_renames(self, name: str) -> Dict[str, str]:
        """Get column rename mappings for different data sources"""
        if name == 'Workforce':
            logger.info(f"Workforce : Renaming columns")

            return {
                'active': 'active',
                'batchEarlierExSD': 'batch_earlier_ExSD', 
                'batchId': 'batch_id', 
                'employeeId': 'employee_id', 
                'lastActivityTime': 'last_activity', 
                'lastContainerId': 'last_container', 
                'lastSeenTime': 'last_seen_time', 
                'location': 'pick_location', 
                'manager': 'manager_name', 
                'name': 'aa_name', 
                'pickArea': 'pick_area', 
                'processPath': 'process_path', 
                'userId': 'user_id'
            }
        if name == 'Process':
            logger.info(f"Process : Renaming columns")
            
            # Basic fields
            renames = {
                'BatchCount': 'batch_count',
                'ContainerUsePercent': 'container_use_percent',
                'PickProcess': 'pick_process',
                'PickerCount': 'picker_count',
                'Status': 'status',
                'ToteCount': 'tote_count',
                'UnitsInScanner': 'units_in_scanner',
                'UnitsInTotesCount': 'units_in_totes_count',
                'UnitsPerHour': 'units_per_hour',
                'pickRateAverage': 'pick_rate_average',
                'unitRateTarget': 'unit_rate_target',
                'process_path_name': 'process_path_name',
            }

            # Priority fields (first unnest)
            priority_fields = {
                'fastTrack': 'prioritized_fast_track',
                'minPriority': 'prioritized_min_priority',
                'premium': 'prioritized_premium',
                'sameNext': 'prioritized_same_next',
                'standard': 'prioritized_standard',
                'superSavers': 'prioritized_super_savers'
            }
            renames.update(priority_fields)

            # Non-priority fields (second unnest)
            non_priority_fields = {
                f'{k}': f'non_prioritized_{v.split("prioritized_")[1]}' 
                for k, v in priority_fields.items()
            }
            renames.update(non_priority_fields)

            return renames
        if name == 'LPI':
            return {
                # Process attributes
                'processId': 'process_id',
                'laborTrackingType': 'labor_tracking_type',
                
                # Attributes
                'CONTAINER_TYPE': 'container_type',
                'GL_CODE': 'gl_code',
                'PICKING_PICK_AREA': 'pick_area',
                'PICKING_PROCESS_PATH': 'process_path',
                'PICK_PATH_GROUP': 'pick_path_group',
                'WORK_FLOW': 'work_flow',
                'SIZE_CATEGORY': 'size_category',
                
                # Associate data
                'employeeId': 'employee_id',
                'employeeName': 'employee_name',
                'managerId': 'manager_id',
                'managerName': 'manager_name',
                'isTokenized': 'is_tokenized',
                'unitCount': 'unit_count',
                'eachCount': 'each_count',
                'timeMillis': 'time_millis'
            }
        if name == 'Rodeo':
            return {
                'Transfer Request ID' : 'transfer_request_id',
                'Destination Warehouse' : 'destination_warehouse',
                'Need To Ship By Date' : 'need_to_ship_by_date',
                'Process Path' : 'process_path',
                'Scannable ID' : 'scannable_id',
                'Outer Scannable ID' : 'o_scannable_id',
                'Outer Outer Scannable ID' : 'o_o_scannable_id',
                'Quantity' : 'quantity',
                'Dwell Time (hours)' : 'dwell_time(hours)',
                'Aisle' : 'aisle',
                'Slot' : 'slot',
                'Pick Area' : 'pick_area',
                'CPT' : 'cpt',

            }
        return {}

    def _calculate_alignment(self, time_spent, time_passed, time_remaining, work_remaining, current_rate):
        """
        Calculate the expected time needed to complete a task vs the time alotted

        This method calculates the alignment based on the time spent, time passed, 
        time remaining, work remaining, and the current rate of work.

        Parameters:
        - time_spent (float): The total time spent on the task so far.
        - time_passed (float): The total time that has passed since the task started.
        - time_remaining (float): The estimated time remaining to complete the task.
        - work_remaining (float): The amount of work that still needs to be done.
        - current_rate (float): The current rate of work being completed (e.g., units per time).

        Returns:
        - float: The calculated alignment, which indicates the expected time needed 
                to complete the task based on the provided parameters.
        """

        try:
            # Handle edge cases
            if not time_spent or not time_passed:
                return True  # No time spent yet
            if not time_remaining:
                return True  # Past deadline
            if not work_remaining:
                return False # No work remaining = No Miss

            # Calculate average headcount with zero protection
            avg_hc = float(time_spent / time_passed) if time_passed > 0 else 0.0
            
            # Calculate alignment with zero protection for rate
            effective_rate = (avg_hc * current_rate) if current_rate else 0.0
            work_time_needed = work_remaining / effective_rate if effective_rate > 0 else 999.0
            
            # Return alignment rounded to 2 decimal places
            alignment = round(float(time_remaining - work_time_needed), 2)

            # Return boolean indicating if we'll miss the deadline
            return work_time_needed > time_remaining

        except Exception as e:
            logger.error(f"Error calculating alignment: {str(e)}")
            return True  # Default to showing warning on error


"""async def main():
    processor = DataProcessor()

    try:
        async with asyncio.timeout(30):  # 30 second timeout
            await processor.process_incoming_data()                    
        results = processor.get_results()

    except asyncio.TimeoutError:
        logger.error("Operation timed out")
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")

if __name__ == "__main__":
    # Setup shift information first
    time_manager = TimeManager.get_instance()
    success = time_manager.setup_shift(
        # Chornos assumes am/pm setting now
        site_code="SAV7",
        start_hour=6,
        #start_am_pm="am",
        end_hour=6,
        #end_am_pm="pm"
    )

    
    if success:
        asyncio.run(main())"""