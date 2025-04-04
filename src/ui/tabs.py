import math
import os
import re
import sys
import time
import requests
import polars as pl
import PySide6.QtGui as qtg
import PySide6.QtCore as qtc
import PySide6.QtWidgets as qtw
from PySide6.QtCore import QTimer
from datetime import timedelta as td
from PySide6.QtWidgets import QApplication
from functools import partial
from asyncio import create_task, get_event_loop


# Module Path Fix
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.config.constants import USER
from src.config.site_build import SiteBuilder
from src.config.chronos import TimeManager
from src.data.processor import DataProcessor
from src.config.res_finder import ResourceFinder
from src.ui.plan_tab import EnhancedPlanCalculator
find_resource = ResourceFinder().find_resource
#from tabs import OverviewTab, DetailsTab, PathsTab, PlanTab, SettingsTab
from src.utils.logger import CustomLogger
logger = CustomLogger.get_logger(__name__)
#logger.error(f"Some Error: {str(e)}")
# - automatically includes traceback and slack post
#logger.info("Some Info")


class CustomTableWidget(qtw.QTableWidget):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setContextMenuPolicy(qtc.Qt.CustomContextMenu)
        self.customContextMenuRequested.connect(self.show_context_menu)

    def show_context_menu(self, position):
        selected_items = self.selectedItems()
        if not selected_items:
            return

        try:
            # Calculate sum for Target HC column only
            selection_sum = sum(
                float(item.text()) for item in selected_items 
                if item.text() and item.column() == self.target_column
            )
            
            menu = qtw.QMenu()
            sum_action = menu.addAction(f"Selection Total: {selection_sum:.2f}")
            sum_action.setEnabled(False)  # Make it non-clickable
            menu.exec_(self.mapToGlobal(position))
            
        except (ValueError, AttributeError) as e:
            logger.error(f"Error calculating selection sum: {str(e)}")



class OverviewTab(qtw.QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.parent = parent
        layout = qtw.QVBoxLayout()
        self.setLayout(layout)



        """
        self.target_volume = int(self.target_volume) if self.target_volume else 0
        target_rate = self.target_rate if self.target_rate else 0
        target_hours = self.target_hours if self.target_hours else 0
        """
        

    def gather_overview(self):
        """
        Gather overview data from the database and update the UI.
        This method retrieves data from the database, calculates metrics,
        and populates the overview tab with the gathered information.
        """
        # Gather data from the database
        self.gather_data()
        # Populate the overview tab with gathered information
        self.populate_overview_ui()

    def gather_data(self):
        """
        Gather data from the database and update the UI.
        This method retrieves data from the database, calculates metrics,
        and populates the overview tab with the gathered information.
        """

        self.shift_info = TimeManager.get_instance().get_shift_info()
        self.site_info = SiteBuilder.get_instance().get_site_info()
        self.data = DataProcessor.get_instance().get_results()

        self.plan = EnhancedPlanCalculator.get_instance().get_plan()

        # Shift Info:
        self.timezone = self.shift_info['timezone']
        self.tz = self.shift_info['tz']
        self.shift_start = self.shift_info['shift_start'].hour
        self.shift_end = self.shift_info['shift_end'].hour
        self.elapsed_time = float(self.shift_info['elapsed_time'].seconds / 3600)
        self.shift_hours = float(self.shift_info['total_hours'].seconds / 3600)

        # Site Info:
        self.site_code = self.shift_info['site_code']
        self.pick_areas = self.site_info['pick_areas']

        if self.site_info['plan_data'] is not None:
            self.plan_data = self.site_info['plan_data']
        elif self.plan is not None:
            self.plan_data = self.plan
        else:
            self.plan_data = None
            
        data = self.data['combined_data']['process_level'][['cpt']]
        

        # Data: [Row, Column]
        if self.data['Rodeo'] is not None:
            self.this_cpt = self.data['Rodeo']['cpt_summary'][0,0] or 'None'
            self.picks_remaining = int(self.data['Rodeo']['cpt_summary'][0,1]) or 0
            self.pick_density = round(float(self.data['Rodeo']['cpt_summary'][0,5]),2) or 0

            data = self.data['combined_data']['process_level'][['cpt']]
            unique_cpts = data['cpt'].unique().sort()
            hov_cpts = unique_cpts.filter(unique_cpts == 'HOV')
            if len(hov_cpts) > 0:
                self.hov_picks_remaining = int(
                    self.data['Rodeo']['cpt_summary']
                    .filter(pl.col('cpt') == 'HOV')
                    .item(0, 3)
                ) or 0
            else:
                self.hov_picks_remaining = 0

            self.time_remaining = float(self.data['Rodeo']['cpt_summary'][0,4]) if self.data['Rodeo']['cpt_summary'][0,4] != 'HOV' else 0


            if self.data['Rodeo']['cpt_summary'].height > 1:
                self.next_cpt = self.data['Rodeo']['cpt_summary'][1,0] or 'None'
                self.picks_remaining2 = int(self.data['Rodeo']['cpt_summary'][1,1]) or 0
                self.pick_density2 = round(float(self.data['Rodeo']['cpt_summary'][1,5]),2) or 0 
                self.hov_picks_remaining2 = 0
            else:
                self.next_cpt = 'None'
                self.picks_remaining2 = 0
                self.pick_density2 = 0
                self.hov_picks_remaining2 = 0


        else:
            self.this_cpt = 'None'
            self.next_cpt = 'None'
            self.picks_remaining = 0
            self.picks_remaining2 = 0
            self.pick_density = 0
            self.pick_density2 = 0
            self.hov_picks_remaining = 0
            self.hov_picks_remaining2 = 0
            self.time_remaining = 0

        if self.data['LPI']['lpi_full'].height > 0:
            self.hov_hours = float(self.data['LPI']['hov']['hov_hrs']) or 0
            self.hov_volume = int(self.data['LPI']['hov']['hov_vol']) or 0
            self.hov_rate = float(self.data['LPI']['hov']['hov_rate']) or 0

            self.non_hov_hours = float(self.data['LPI']['non_hov']['non_hov_hrs']) or 0
            self.non_hov_volume = int(self.data['LPI']['non_hov']['non_hov_vol']) or 0
            self.non_hov_rate = float(self.data['LPI']['non_hov']['non_hov_rate']) or 0

            self.combined_rate = float(self.data['LPI']['combined']['combined_rate']) or 0
            self.combined_hours = float(self.data['LPI']['combined']['combined_hrs']) or 0
            self.combined_volume = int(self.data['LPI']['combined']['combined_vol']) or 0
            self.alignment = self.parent.calculate_alignment(
                self.combined_hours, self.elapsed_time, self.time_remaining,
                self.picks_remaining, self.combined_rate
            )
        else:
            self.hov_hours = 0
            self.hov_volume = 0
            self.hov_rate = 0
            self.non_hov_hours = 0
            self.non_hov_volume = 0
            self.non_hov_rate = 0
            self.combined_rate = 0
            self.combined_hours = 0
            self.combined_volume = 0
            self.alignment = 0

        self.progress_percent = round(float(self.shift_info['progress_percent'])) or 0
        
        self.pad_warn = self.time_remaining < 1.5 # 90 Minute Pad Time Warning
        self.in_pad_time = "⚠ INSIDE PAD TIME (90 min) ⚠" if self.pad_warn else None
        #(hours remaining - 1.5, or hours remaining (pad_warn=True))


        if self.data['Workforce']['workforce_full'].height > 0:
            self.current_hc = int(self.data['Workforce']['headcounts']['total_headcount']) or 0
            self.active_hc = int(self.data['Workforce']['headcounts']['active_headcount']) or 0
            self.hc_ratio = round(self.active_hc / self.current_hc, 2) or 0
        else:
            self.current_hc = 0
            self.active_hc = 0
            self.hc_ratio = 0



        if self.plan_data is not None:
            self.target_volume = self.plan_data['inputs']['target_volume']
            self.target_rate = self.plan_data['inputs']['target_rate']
            self.target_hours = self.plan_data['calculated']['target_hours']
            self.plan_hc = self.plan_data['calculated']['target_headcount']

            self.target_picks = int(
                self.target_volume - self.picks_remaining - self.combined_volume
                ) if self.target_volume > 1 else 'No Plan'
        else:
            self.target_volume = 0
            self.target_rate = 0
            self.target_hours = 0
            self.plan_hc = 0
            self.target_picks = 'No Plan'

        self.percent_to_plan = {'rate': 'N/A', 'headcount': 'N/A', 'volume': 'N/A'}

        """
        self.percent_to_plan.get('____', 'N/A')
        rate, headcount, hours, volume
        """


        
    def populate_overview_ui(self):
        """
        Populate the overview tab with current metrics and information.
        This method clears existing widgets in the overview tab and populates it with
        updated metrics such as rate, headcount, volume, and time until the next CPT.
        """
        # Clear existing widgets in the tab
        for widget in self.findChildren(qtw.QWidget):
            widget.deleteLater()  # Use deleteLater to safely remove widgets

        # Check if layout exists and delete it
        if self.layout() is not None:
            old_layout = self.layout()
            qtw.QWidget().setLayout(old_layout)

        # Set new layout
        layout = qtw.QGridLayout()
        layout.setVerticalSpacing(5)  # Reduce vertical gap between rows
        layout.setContentsMargins(5, 5, 5, 5)  # Reduce margins (left, top, right, bottom)
        self.setLayout(layout)


        for i in range(4):
            layout.setColumnStretch(i, 1)    
        # Create a container for headers and metrics
        metrics_container = qtw.QWidget()
        metrics_layout = qtw.QGridLayout(metrics_container)
        metrics_layout.setVerticalSpacing(2)  # Reduce spacing between metric rows
        metrics_layout.setContentsMargins(2, 5, 2, 5)  # Reduce margins
        metrics_layout.setSpacing(2)  # Reduce both vertical and horizontal spacing

        metrics_container.setLayout(metrics_layout)

        # Headers
        headers = ["Metric", "Plan", "Actual", "% To Plan"]
        for col, header in enumerate(headers):
            header_label = qtw.QLabel(header)
            header_label.setFont(qtg.QFont("Helvetica", 16, qtg.QFont.Bold))
            header_label.setAlignment(qtc.Qt.AlignCenter)
            metrics_layout.addWidget(header_label, 0, col)

        def calculate_percent_to_plan(actual, plan):
            """Calculate the percentage of the plan achieved."""
            if plan == 0:
                return "N/A"
            else:
                return ((actual / plan) * 100)


        if self.plan_data is not None:
            self.percent_to_plan = {
                'rate': calculate_percent_to_plan(self.combined_rate, self.target_rate),
                'headcount': calculate_percent_to_plan(self.current_hc, self.plan_hc),
                'hours': calculate_percent_to_plan(self.combined_hours, self.target_hours),
                'volume': calculate_percent_to_plan(self.combined_volume, self.target_volume)
            }

        def format_target(value):
            """Format the target value for display."""
            return "--" if value is None or value == 0 else f"{value:.2f}" if isinstance(value, float) else str(value)

        # Metrics
        metrics = [
            ("Rate", format_target(self.target_rate), f"{self.combined_rate:.2f}", self.percent_to_plan.get('rate', 'N/A')),
            ("Headcount", format_target(self.plan_hc), f"{self.current_hc} (Active: {self.active_hc})", self.percent_to_plan.get('headcount', 'N/A')),
            ("Labor Hrs", format_target(self.target_hours), f"{self.combined_hours:.2f}", self.percent_to_plan.get('hours', 'N/A')),
            ("Volume", format_target(self.target_volume), str(self.combined_volume), self.percent_to_plan.get('volume', 'N/A')),
        ]

        for idx, (metric, target, actual, percent) in enumerate(metrics, start=1):
            logger.info(f"Processing metric: {metric}")
            # Metric label
            metric_label = qtw.QLabel(metric)
            metric_label.setFont(qtg.QFont("Helvetica", 14, qtg.QFont.Bold))
            metric_label.setAlignment(qtc.Qt.AlignCenter)
            metrics_layout.addWidget(metric_label, idx, 0)

            # Target label
            target_label = qtw.QLabel(target)
            target_label.setFont(qtg.QFont("Helvetica", 14))
            target_label.setAlignment(qtc.Qt.AlignCenter)
            metrics_layout.addWidget(target_label, idx, 1)

            # Actual label
            if metric == "Headcount":
                color = "red" if self.hc_ratio > 1.75 else "yellow" if 1.25 < self.hc_ratio <= 1.74 else "green"
                actual_label = qtw.QLabel(f'<a href="https://picking-console.na.picking.aft.a2z.com/fc/{self.site_code}/pick-workforce" style="color: {color};">{actual}</a>')
                actual_label.setOpenExternalLinks(True)
            else:
                color = "black"
                actual_label = qtw.QLabel(actual)
                actual_label.setStyleSheet(f"color: {color};")

            actual_label.setFont(qtg.QFont("Helvetica", 14))
            actual_label.setAlignment(qtc.Qt.AlignCenter)
            metrics_layout.addWidget(actual_label, idx, 2)

            # Percent label
            tolerance = 5  # 5% of total shift length
            target = self.progress_percent
            lower_bound = target - tolerance
            upper_bound = target + tolerance

            percent_label = qtw.QLabel("N/A")  # Default value
            if metric == '':
                percent_text = ""
            elif self.shift_hours > 24:
                percent_text = "N/A"
                color = "white"
            else:
                if metric == 'Labor Hrs':
                    if percent == 'N/A':
                        percent_text = "N/A"
                        color = "white"
                    elif isinstance(percent, (int, float)):
                        percent_text = f"{percent:.2f}%"
                        color = "green" if lower_bound <= percent <= upper_bound else "red"
                        logger.info(f"Percent for {metric}: {percent_text}")
                        logger.info(f"Color for {metric}: {color}")
                    else:
                        percent_text = str(percent)
                        color = "white"
                elif metric == 'Volume':
                    if percent == 'N/A':
                        percent_text = "N/A"
                        color = "white"
                    elif isinstance(percent, (int, float)):
                        percent_text = f"{percent:.2f}%"
                        color = "green" if lower_bound <= percent <= upper_bound else "red"
                    else:
                        percent_text = str(percent)
                        color = "white"
                elif metric == 'HOV':
                    if percent != 'N/A':
                        percent_text = f"Rate: {self.hov_rate:.2f}"
                        color = "black"
                else:
                    if percent == 'N/A':
                        percent_text = "N/A"
                        color = "white"
                    elif isinstance(percent, (int, float)):
                        percent_text = f"{percent:.2f}%"
                        # Static values for real-time metrics
                        color = "green" if 95 <= percent <= 105 else "red"
                    else:
                        percent_text = str(percent)
                        color = "white"

            percent_label.setText(percent_text)
            percent_label.setFont(qtg.QFont("Helvetica", 14))
            percent_label.setAlignment(qtc.Qt.AlignCenter)
            percent_label.setStyleSheet(f"color: {color};")
            metrics_layout.addWidget(percent_label, idx, 3)



        # Add the metrics container to the main layout
        layout.addWidget(metrics_container, 0, 0, 1, 4)

        # Visual Spacer
        #layout.addItem(qtw.QSpacerItem(0, 10))


        # HOV and Non-HOV breakouts
        breakout_container = qtw.QWidget()
        breakout_layout = qtw.QVBoxLayout(breakout_container)
        breakout_layout.setSpacing(0)  # Minimize spacing between elements
        breakout_layout.setContentsMargins(0, 0, 0, 0)  # Remove margins
        breakout_container.setSizePolicy(qtw.QSizePolicy.Preferred, qtw.QSizePolicy.Fixed)  # Prevent vertical stretching

        # For the HOV label
        hov = f"HOV ||   Hours: {self.hov_hours:.2f}     Volume: {self.hov_volume}     Rate: {self.hov_rate:.2f} "
        non_hov = f"Non-HOV ||  Hours: {self.non_hov_hours:.2f}    Volume: {self.non_hov_volume}    Rate: {self.non_hov_rate:.2f}"
        hov_label = qtw.QLabel(f"{hov}\n\n{non_hov}")
        hov_label.setFont(qtg.QFont("Helvetica", 16))
        hov_label.setAlignment(qtc.Qt.AlignCenter)
        hov_label.setSizePolicy(qtw.QSizePolicy.Preferred, qtw.QSizePolicy.MinimumExpanding)  # Prevent label from stretching

        breakout_layout.addWidget(hov_label, alignment=qtc.Qt.AlignCenter)

        
        layout.addWidget(breakout_container, 1, 0, 1, 4, qtc.Qt.AlignCenter)



        # Create CPT information container
        cpt_container = qtw.QWidget()
        cpt_container.setObjectName("cpt_container")

        # Create main layout for CPT container
        cpt_main_layout = qtw.QVBoxLayout()
        cpt_main_layout.setSpacing(5)
        cpt_main_layout.setContentsMargins(5, 10, 10, 5)
        cpt_container.setLayout(cpt_main_layout)

        # Create horizontal layout for two columns
        columns_layout = qtw.QHBoxLayout()
        columns_layout.setSpacing(15)  # Space between columns

        # Left Column (cpt1)
        left_column = qtw.QWidget()
        left_layout = qtw.QVBoxLayout()
        left_layout.setSpacing(5)
        left_column.setLayout(left_layout)

        # Right Column (cpt2)
        right_column = qtw.QWidget()
        right_layout = qtw.QVBoxLayout()
        right_layout.setSpacing(5)
        right_column.setLayout(right_layout)

        # Populate Left Column
        left_header = qtw.QLabel(f"{self.this_cpt}")
        left_header.setFont(qtg.QFont("Helvetica", 12, qtg.QFont.Bold))
        left_layout.addWidget(left_header, alignment=qtc.Qt.AlignCenter)

        # Add CPT time info
        if self.in_pad_time:
            pad_label = qtw.QLabel(self.in_pad_time)
            pad_label.setStyleSheet("color: red;")
            left_layout.addWidget(pad_label, alignment=qtc.Qt.AlignCenter)

        picks_density_label = qtw.QLabel(
            f"Picks Remaining: {self.picks_remaining} (HOV: {self.hov_picks_remaining})\nPick Density: {self.pick_density:.2f}"
        )
        picks_density_label.setAlignment(qtc.Qt.AlignCenter)
        left_layout.addWidget(picks_density_label)

        time_align_label = qtw.QLabel(
            f"Time Remaining: {self.time_remaining}\nAlignment: {self.alignment:.2f} hours"
        )
        time_align_label.setAlignment(qtc.Qt.AlignCenter)
        left_layout.addWidget(time_align_label)

        # Populate Right Column
        # Just Use Second Row of CPT Summary
        
        cpt = self.next_cpt
        picks_remaining = self.picks_remaining2
        pick_density = self.pick_density2
        hov_picks_remaining = self.hov_picks_remaining2

        right_header = qtw.QLabel(f"{cpt}")
        right_header.setFont(qtg.QFont("Helvetica", 12, qtg.QFont.Bold))
        right_layout.addWidget(right_header, alignment=qtc.Qt.AlignCenter)

        picks_density_label2 = qtw.QLabel(
            f"Picks Remaining: {picks_remaining}\nPick Density: {pick_density:.2f}"
        )
        picks_density_label2.setAlignment(qtc.Qt.AlignCenter)
        right_layout.addWidget(picks_density_label2)

        target_picks_label = qtw.QLabel(
            f"Picks to Meet Plan: {self.target_picks}"
        )
        target_picks_label.setAlignment(qtc.Qt.AlignCenter)
        right_layout.addWidget(target_picks_label)

        # Add columns to the horizontal layout
        columns_layout.addWidget(left_column)
        columns_layout.addWidget(right_column)

        # Add columns layout to main CPT layout
        cpt_main_layout.addLayout(columns_layout)

        # Add the CPT container to the main tab layout
        cpt_position = len(metrics) + 1
        layout.addWidget(cpt_container, 2, 0, 1, 4)

        # Style the CPT container
        cpt_container.setStyleSheet("""
            #cpt_container {
                background-color: #f5f5f5;
                border: 1px solid #dcdcdc;
                border-radius: 10px;
                margin: 5px;
            }
            QLabel {
                background-color: transparent;
                padding: 2px;
            }
        """)


        layout.setRowStretch(0, 0)  # Metrics section
        layout.setRowStretch(1, 1)  # Breakout section
        layout.setRowStretch(2, 0)  # CPT section 
        # Log Update
        logger.info('Overview Populated.')


class DetailsTab(qtw.QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.parent = parent
        layout = qtw.QVBoxLayout()
        self.setLayout(layout)


    def gather_details(self):
        """
        Gather CPT details data from the database and update the UI.
        This method retrieves data from the database, calculates metrics,
        and populates the details tab with the gathered information.
        """
        # Gather data from the database
        self.gather_data()

        # Populate the overview tab with gathered information
        self.populate_details_ui()



    def gather_data(self):
        """
        Gather data from the database and update the UI.
        This method retrieves data from the database, calculates metrics,
        and populates the overview tab with the gathered information.
        """

        self.shift_info = TimeManager.get_instance().get_shift_info()
        self.site_info = SiteBuilder.get_instance().get_site_info()
        self.data = DataProcessor.get_instance().get_results()
        self.plan = EnhancedPlanCalculator.get_instance().get_plan()
        

        
        # Initialize both columns with zeros regardless of plan data availability
        self.data['combined_data']['process_level'] = (
            self.data['combined_data']['process_level']
            .with_columns([
                pl.lit(0.0).alias('planned_cases'),
                pl.lit(0.0).alias('target_hc')
            ])
        )

        # If no plan data is available, return here
        if self.site_info['plan_data'] is not None:
            self.plan_data = self.site_info['plan_data']
        elif self.plan is not None:
            self.plan_data = self.plan
        else:
            self.plan_data = None
            logger.warning("Plan data not available, columns initialized with zeros")
            return

        def get_planned_cases(row):
            try:
                cpt = row['cpt']
                path = row['process_path']
                all_picks = row['total_cases']
                
                # Check if any of the required values are None
                if any(v is None for v in [cpt, path, all_picks]):
                    return 0.0
                    
                # Check if the nested dictionary keys exist
                cpt_data = self.plan_data['cpt_breakdown'].get(cpt)
                if cpt_data is None:
                    return 0.0
                    
                path_data = cpt_data.get(path)
                if path_data is None:
                    return 0.0
                
                cases = path_data.get('calculated_cases', 0.0) - path_data.get('cases_picked', 0.0)
                # Get the calculated cases and ensure it's a float
                #percent_to_pick = path_data.get('percent_to_pick', 0.0)
                #cases = cases * percent_to_pick

                

                return round(float(cases),2) if cases is not None else 0.0
                
            except (KeyError, AttributeError) as e:
                logger.debug(f"No plan data found for CPT {row.get('cpt')} "
                            f"and path {row.get('process_path')}: {str(e)}")
                return 0.0
            except Exception as e:
                logger.error(f"Unexpected error in get_planned_cases: {str(e)}")
                return 0.0

        def calculate_target_hc(row):
            try:
                planned_cases = row['planned_cases']
                cpt = row['cpt']
                path = row['process_path']
                
                # Check if the nested dictionary keys exist
                cpt_data = self.plan_data['cpt_breakdown'].get(cpt)
                if cpt_data is None:
                    return 0.0
                    
                path_data = cpt_data.get(path)
                if path_data is None:
                    return 0.0
                 
                # If planned_cases is zero, return the existing target_hc value
                if planned_cases == 0:
                    return 0.0
                
                # Get the appropriate CPH value (avg_cph or historical_cph)
                cph = row['avg_cph'] if row['avg_cph'] is not None and row['avg_cph'] > 0 else row['historical_cph']
                
                # If we don't have a valid CPH value, return existing target_hc
                if cph is None or cph == 0:
                    #if the path string contains 'HOV' use the HOV CPH, otherwise non-HOV CPH
                    if 'HOV' in path:
                        cph = self.data['LPI']['hov']['hov_rate']
                    else:
                        cph = self.data['LPI']['non_hov']['non_hov_rate']
                
                # Get hours remaining from shift info

                # Connect to hours to complete from Plan
                hours_remaining = self.shift_info['hours_remaining'].seconds / 3600
                if hours_remaining == 0:
                    return 0.0
                
                hours_to_pick = cpt_data.get('hours_to_pick', 0.0)
                if hours_to_pick == 0:
                    return 0.0
                
                # Calculate new target_hc



                new_target_hc = (planned_cases / cph) / hours_to_pick
                
                return round(float(new_target_hc),2)
            
            except Exception as e:
                logger.error(f"Error calculating target_hc: {str(e)}")
                return row['target_hc']  # Return existing value if calculation fails

        try:
            # Update the main DataFrame with planned_cases and target_hc in one operation
            self.data['combined_data']['process_level'] = (
                self.data['combined_data']['process_level']
                .with_columns([
                    pl.struct(['cpt', 'process_path', 'total_cases']).map_elements(get_planned_cases).alias('planned_cases')
                ])

                .with_columns([
                    # Update avg_cph with appropriate CPH value, handling both invalid cases
                    pl.when(
                        (pl.col('avg_cph').is_null()) | 
                        (pl.col('avg_cph').is_infinite()) | 
                        (pl.col('avg_cph') <= 0)
                    ).then(
                        pl.when(
                            (pl.col('historical_cph').is_null()) |
                            (pl.col('historical_cph').is_infinite()) |
                            (pl.col('historical_cph') <= 0)
                        ).then(pl.lit(0))
                        .otherwise(pl.col('historical_cph'))
                    )
                    .otherwise(pl.col('avg_cph'))
                    .alias('avg_cph')
                ])

                .with_columns([
                    pl.struct(['cpt', 'process_path', 'planned_cases', 'avg_cph', 'historical_cph', 'target_hc'])
                        .map_elements(calculate_target_hc).alias('target_hc')
                ])
            )
            
            logger.info("Successfully updated planned_cases and target_hc columns")

        except Exception as e:
            print(f"Error updating process_level Dataframe: {str(e)}")
            logger.error(f"Error updating process_level DataFrame: {str(e)}")
            # If the complex mapping fails, fall back to adding zeros for planned_cases
            try:
                self.data['combined_data']['process_level'] = (
                    self.data['combined_data']['process_level']
                    .with_columns(planned_cases=pl.lit(0.0))  # Using 0.0 to ensure float type
                    .with_columns(target_hc=pl.lit(0.0))
                )
                logger.info("Added planned_cases column with default zeros after error")
            except Exception as fallback_error:
                logger.error(f"Failed to add fallback zero column: {str(fallback_error)}")




    def populate_details_ui(self):
        
        # Clear existing widgets
        logger.info('Clearing existing widgets in the Details tab.')
        
        for widget in self.findChildren(qtw.QWidget):
            widget.deleteLater()

        # Check if layout exists and delete it
        if self.layout() is not None:
            old_layout = self.layout()
            qtw.QWidget().setLayout(old_layout)

        # Set new layout
        self.setLayout(qtw.QGridLayout())
        self.layout().setColumnStretch(0, 1)
        self.layout().setRowStretch(1, 1)


        # Create a tabview for the details tab
        self.details_tabview = qtw.QTabWidget(self)
        self.layout().addWidget(self.details_tabview, 1, 0, alignment=qtc.Qt.AlignTop)


        # Get unique CPTs and create subtab for each
        if self.data['Rodeo'] is None:
            # Create tab for this No Data
            tab = qtw.QWidget()
            self.details_tabview.addTab(tab, 'No Data')
            tab.setLayout(qtw.QVBoxLayout())
            details_label = qtw.QLabel(f"No Rodeo Data Available")
            details_label.setFont(qtg.QFont('Helvetica', 16, qtg.QFont.Bold))
            tab.layout().addWidget(details_label)
            logger.error("No Rodeo data available.")
            return
        
        cpt_summary = self.data['Rodeo']['cpt_summary']
        for cpt in cpt_summary.get_column('cpt'):

            logger.info(f"Creating tab for CPT: {cpt}")

            # Create tab for this CPT
            tab = qtw.QWidget()
            self.details_tabview.addTab(tab, str(cpt))
            tab.setLayout(qtw.QVBoxLayout())

            # Filter data for this CPT
            cpt_data = self.data['combined_data']['process_level'].filter(pl.col('cpt') == cpt)

            top_level = self.data['Rodeo']['cpt_summary']
            if cpt == 'HOV':
                hours_remaining = round(self.shift_info['hours_remaining'].seconds / 3600, 2)
            else:
                hours_remaining = top_level.filter(pl.col('cpt') == cpt)['hours_remaining'].max()
            try:
                cpt_cph = round(float(cpt_data['cases_picked'].sum() / cpt_data['total_hours'].sum()),2)
            except ZeroDivisionError:
                cpt_cph = 0


            details_label = qtw.QLabel(f"[{cpt}] CPT by Process Path")
            details_label.setFont(qtg.QFont('Helvetica', 16, qtg.QFont.Bold))
            tab.layout().addWidget(details_label)

            time_remaining_label = qtw.QLabel(f"Current CPH: {cpt_cph} // Hours Remaining: {hours_remaining}")
            time_remaining_label.setFont(qtg.QFont('Helvetica', 14, qtg.QFont.Bold))
            tab.layout().addWidget(time_remaining_label)


            # Create a scrollable frame to display the data
            logger.info(f'Creating scrollable frame for CPT: {cpt}.')
            scrollable_frame = qtw.QScrollArea(tab)
            scrollable_frame.setWidgetResizable(True)

            # Create a container widget for the scrollable area
            scrollable_content = qtw.QWidget()
            scrollable_layout = qtw.QGridLayout(scrollable_content)

            # Configure scrollable frame to expand
            scrollable_frame.setWidget(scrollable_content)
            tab.layout().addWidget(scrollable_frame)

            # Add headers
            logger.info('Adding headers to the scrollable frame.')


            palatable_data = cpt_data.select(
                [
                    pl.col('process_path').alias('Process Path'),
                    pl.col('total_cases').alias('Picks'),
                    pl.col('case_density').alias('Density'),
                    pl.col('PRA').alias('PRA'),
                    pl.col('TUR').alias('TUR'),
                    pl.col('avg_cph').alias('CPH').fill_null(0),
                    pl.col('total_pickers').alias('Current HC').fill_null(0),
                    pl.col('active_pickers').alias('Active HC').fill_null(0),
                    pl.col('planned_cases').alias('Plan Picks'),
                    pl.col('target_hc').alias('Target HC'),
                    pl.col('projected_miss').alias('Projected Miss')
                ]
            )

            table = qtw.QTableWidget()
            table.setRowCount(len(palatable_data))
            table.setColumnCount(len(palatable_data.columns))
            table.setHorizontalHeaderLabels(palatable_data.columns)

            header_tooltips = {
                'Process Path': 'Pick Path Name',
                'Picks': 'Total Cases Remaining',
                'Density': 'Units Per Case',
                'PRA': 'Picker Rate Average\n(Cases Per Hour * Density)',
                'TUR': 'Target Unit Rate\n(PRA Per Picker)',
                'CPH': 'Cases Per Hour',
                'Current HC': 'Total Headcount',
                'Active HC': 'Total Active Pickers',
                'Target HC': 'Required Headcount\n(Picks / (Hours Remaining * Rate))\n\nIf no current "Rate" is available,\nPath-Specific Historical Rate is used (7-day)',
                'Projected Miss': 'Projected to Miss Deadline?\nTrue = Potential Miss, False = On Track\n\nMath:\nHC = labor hours / hours since SoS\nTotal Rate = hc * avg rate\nTime Need = picks remaining / total rate\nTime Remaining < Time Need ?'
            }

            # Set header tooltips
            for col_idx, col_name in enumerate(palatable_data.columns):
                header_item = qtw.QTableWidgetItem(col_name)
                if col_name in header_tooltips:
                    header_item.setToolTip(header_tooltips[col_name])
                table.setHorizontalHeaderItem(col_idx, header_item)

            # Configure column stretching with custom weights
            header = table.horizontalHeader()
            # Set Process Path column to stretch more
            header.setSectionResizeMode(0, qtw.QHeaderView.Stretch)  # Process Path column
            # Set other columns to be sized to content with minimum width
            for col in range(1, len(palatable_data.columns)):
                header.setSectionResizeMode(col, qtw.QHeaderView.ResizeToContents)
                # Set minimum width for numeric columns
                table.setColumnWidth(col, max(85, table.columnWidth(col)))



            # Populate table with data
            for row_idx, row in enumerate(palatable_data.iter_rows(named=True)):
                for col_idx, (col_name, value) in enumerate(row.items()):
                    # Create item and make it read-only
                    item = qtw.QTableWidgetItem(str(value))
                    item.setFlags(
                        qtc.Qt.ItemIsEnabled |  # Allows the item to be interacted with
                        qtc.Qt.ItemIsSelectable |  # Allows the item to be selected
                        qtc.Qt.ItemIsDropEnabled  # Allows copy/paste operations
                    )
                    item.setTextAlignment(qtc.Qt.AlignCenter)
                    

                    # Compare Total HC vs Target HC
                    if col_name == 'Current HC':
                        target_hc = float(row['Target HC'])
                        current_hc = float(value) if value is not None else 0

                        
                        # Calculate deviation percentage
                        if target_hc > 0:
                            if current_hc > 1 or current_hc == 0:
                                deviation = (current_hc - target_hc) / target_hc
                                
                                if deviation < -0.25:  # More than 25% understaffed
                                    item.setBackground(qtg.QColor('#ffcccc'))  # Light red
                                    item.setToolTip(f"Understaffed")
                                elif deviation > 0.25:  # More than 25% overstaffed
                                    item.setBackground(qtg.QColor('#ffcccc'))
                                    item.setToolTip(f"Overstaffed")
                                else:
                                    item.setBackground(qtg.QColor('#ccffcc'))  # Light green
                                    item.setToolTip("Staffing within target range")

                    if col_name == 'Active HC':
                        current_hc = float(row['Current HC'])
                        active_hc = float(value) if value is not None else 0
                        active_ratio = active_hc / current_hc if current_hc > 0 else 0

                        if active_ratio < 0.51:
                            item.setBackground(qtg.QColor('#ffcccc'))  # Light red
                            item.setToolTip(f"Active Ratio: {active_ratio:.2f}")

                    
                    # Compare Current CPH vs 7-Day CPH
                    elif col_name == 'Current CPH':
                        historical_cph = float(row['7-Day CPH']) if row['7-Day CPH'] else 0
                        current_cph = float(value) if value else 0

                    # Add tooltips based on column
                    if col_name == 'Projected Miss':
                        tooltip = f""
                        #item.setToolTip(tooltip)
                    elif col_name == 'PRA':
                        tooltip = f""
                        #item.setToolTip(tooltip)
                    elif col_name == 'TUR':
                        tooltip = f""
                        #item.setToolTip(tooltip)
                    elif col_name == 'Target HC':
                        tooltip = f""
                        #item.setToolTip(tooltip)

                    # Add conditional formatting based on column
                    if col_name == 'Projected Miss':
                        if value:  # if True (will miss)
                            item.setBackground(qtg.QColor('#ffcccc'))  # light red
                        else:
                            item.setBackground(qtg.QColor('#ccffcc'))  # light green
                    
                    table.setItem(row_idx, col_idx, item)

            # Auto-resize columns to content
            table.resizeRowsToContents()

            table.setHorizontalScrollBarPolicy(qtc.Qt.ScrollBarAsNeeded)
            table.setVerticalScrollBarPolicy(qtc.Qt.ScrollBarAsNeeded)
            # Add table to scrollable layout
            scrollable_layout.addWidget(table, 0, 0)
            scrollable_layout.setColumnStretch(0, 1)


class PathsTab(qtw.QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.parent = parent
        layout = qtw.QVBoxLayout()
        self.setLayout(layout)


    def gather_paths(self):
        """
        Gather CPT details data from the database and update the UI.
        This method retrieves data from the database, calculates metrics,
        and populates the details tab with the gathered information.
        """
        # Gather data from the database
        self.gather_data()
        # Populate the overview tab with gathered information
        self.populate_paths_ui()


    def gather_data(self):
        """
        Gather data from the database and update the UI.
        This method retrieves data from the database, calculates metrics,
        and populates the overview tab with the gathered information.
        """

        self.shift_info = TimeManager.get_instance().get_shift_info()
        self.site_info = SiteBuilder.get_instance().get_site_info()
        self.data = DataProcessor.get_instance().get_results()
        self.plan = EnhancedPlanCalculator.get_instance().get_plan()

        self.site_code = self.site_info['site_code']

        # Initialize both columns with zeros regardless of plan data availability
        self.data['combined_data']['area_level'] = (
            self.data['combined_data']['area_level']
            .with_columns([
                pl.lit(0.0).alias('planned_cases'),
                pl.lit(0.0).alias('target_hc')
            ])
        )
        

        # If no plan data is available, return here
        if self.site_info['plan_data'] is not None:
            self.plan_data = self.site_info['plan_data']
        elif self.plan is not None:
            self.plan_data = self.plan
        else:
            self.plan_data = None
            logger.warning("Plan data not available, columns initialized with zeros")
            return

        def get_planned_cases(row):
            try:
                cpt = row['cpt']
                path = row['process_path']
                #area = row['pick_area']
                all_picks = (row.get('total_cases') or 0) + (row.get('cases_picked') or 0)
                path_picks = self.plan_data['cpt_breakdown'][cpt][path]['calculated_cases']
                
                # Check if any of the required values are None
                if any(v is None for v in [cpt, path, all_picks]):
                    return 0.0
                
                # Check if the nested dictionary keys exist
                cpt_data = self.plan_data['cpt_breakdown'].get(cpt)
                if cpt_data is None:
                    return 0.0
                    
                path_data = cpt_data.get(path)
                if path_data is None:
                    return 0.0
                
                # Get the calculated cases and ensure it's a float
                percent_to_pick = path_data.get('percent_to_pick', 0.0)
                cases = all_picks * percent_to_pick
                cases = cases - (row.get('cases_picked') or 0)


                #cases = path_data.get('calculated_cases', 0.0)

                return round(float(cases),2) if cases is not None else 0.0
                
            except (KeyError, AttributeError) as e:
                logger.debug(f"No plan data found for CPT {row.get('cpt')} "
                            f"and path {row.get('process_path')}: {str(e)}")
                return 0.0
            except Exception as e:
                logger.error(f"Unexpected error in get_planned_cases: {str(e)}")
                return 0.0


        def calculate_target_hc(row):
            try:
                planned_cases = row['planned_cases']
                cpt = row['cpt']
                path = row['process_path']
                
                # Check if the nested dictionary keys exist
                cpt_data = self.plan_data['cpt_breakdown'].get(cpt)
                if cpt_data is None:
                    return 0.0
                    
                path_data = cpt_data.get(path)
                if path_data is None:
                    return 0.0
                 
                # If planned_cases is zero, return the existing target_hc value
                if planned_cases == 0:
                    return 0.0
                
                # Get the appropriate CPH value (avg_cph or historical_cph)
                cph = row['avg_cph'] if row['avg_cph'] is not None and row['avg_cph'] > 0 else row['historical_cph']
                
                # If we don't have a valid CPH value, return existing target_hc
                if cph is None or cph == 0:
                    #if the path string contains 'HOV' use the HOV CPH, otherwise non-HOV CPH
                    if 'HOV' in path:
                        cph = self.data['LPI']['hov']['hov_rate']
                    else:
                        cph = self.data['LPI']['non_hov']['non_hov_rate']
                    #self.data['LPI']['hov']['hov_rate']
                    #self.data['LPI']['hov']['non_hov_rate']
                    #return 0.0
                
                # Get hours remaining from shift info

                # Connect to hours to complete from Plan
                hours_remaining = self.shift_info['hours_remaining'].seconds / 3600
                if hours_remaining == 0:
                    return 0.0
                
                hours_to_pick = cpt_data.get('hours_to_pick', 0.0)
                if hours_to_pick == 0:
                    return 0.0
                
                # Calculate new target_hc



                new_target_hc = (planned_cases / cph) / hours_to_pick
                
                return round(float(new_target_hc),2)
                
            except Exception as e:
                logger.error(f"Error calculating target_hc: {str(e)}")
                return row['target_hc']  # Return existing value if calculation fails
            

        try:
            # Update the main DataFrame with planned_cases and target_hc in one operation
            self.data['combined_data']['area_level'] = (
                self.data['combined_data']['area_level']
                .with_columns([
                    pl.struct(['cpt', 'process_path', 'total_cases', 'cases_picked']).map_elements(get_planned_cases).alias('planned_cases')
                ])

                .with_columns([
                    pl.struct(['cpt', 'process_path', 'planned_cases', 'avg_cph', 'historical_cph', 'target_hc'])
                        .map_elements(calculate_target_hc).alias('target_hc')
                ])
            )
            
            logger.info("Successfully updated planned_cases and target_hc columns")

        except Exception as e:
            print(f"Error updating process_level Dataframe: {str(e)}")
            logger.error(f"Error updating process_level DataFrame: {str(e)}")
            # If the complex mapping fails, fall back to adding zeros for planned_cases
            try:
                self.data['combined_data']['area_level'] = (
                    self.data['combined_data']['area_level']
                    .with_columns(planned_cases=pl.lit(0.0))  # Using 0.0 to ensure float type
                    .with_columns(target_hc=pl.lit(0.0))
                )
                logger.info("Added planned_cases column with default zeros after error")
            except Exception as fallback_error:
                logger.error(f"Failed to add fallback zero column: {str(fallback_error)}")
        


    def populate_paths_ui(self):

        # Clear existing widgets
        logger.info('Clearing existing widgets in the Paths tab.')

        for widget in self.findChildren(qtw.QWidget):
            widget.deleteLater()

        # Check if layout exists and delete it
        if self.layout() is not None:
            old_layout = self.layout()
            qtw.QWidget().setLayout(old_layout)

            # Set new layout
            main_layout = qtw.QVBoxLayout()  # Change to QVBoxLayout for better vertical control
            self.setLayout(main_layout)

            # Create and add the CPT tabview with stretch
            self.cpt_tabview = qtw.QTabWidget(self)
            main_layout.addWidget(self.cpt_tabview)
            main_layout.setStretch(0, 1)  # Make tabview stretch to fill space
            



        # Get unique CPTs and create subtab for each

        if self.data['Rodeo'] is None:
            # Create tab for this No Data
            tab = qtw.QWidget()
            self.cpt_tabview.addTab(tab, 'No Data')
            tab.setLayout(qtw.QVBoxLayout())
            details_label = qtw.QLabel(f"No Rodeo Data Available")
            details_label.setFont(qtg.QFont('Helvetica', 16, qtg.QFont.Bold))
            tab.layout().addWidget(details_label)
            logger.error("No Rodeo data available.")
            return
        


        # Sort CPTs chronologically
        cpt_summary = self.data['Rodeo']['cpt_summary']
        for cpt in cpt_summary.get_column('cpt'):
            logger.info(f"Creating tab for CPT: {cpt}")
            try:
                # Create CPT level tab
                cpt_tab = qtw.QWidget()
                self.cpt_tabview.addTab(cpt_tab, str(cpt))

                # Use QVBoxLayout with stretch factor
                cpt_layout = qtw.QVBoxLayout()
                cpt_tab.setLayout(cpt_layout)

                # Create nested tabview for process paths
                path_tabview = qtw.QTabWidget()  # Create new tabview for each CPT
                cpt_layout.addWidget(path_tabview)
                cpt_layout.setStretch(0, 1)

                # Set margins and spacing to minimize gaps
                #cpt_layout.setContentsMargins(0, 0, 0, 0)
                #cpt_layout.setSpacing(0)

                # Filter data for this CPT
                cpt_data = self.data['combined_data']['area_level'].filter(pl.col('cpt') == cpt)

                # Get Unique Process Paths and create subtab for each
                unique_paths = cpt_data.get_column('process_path').unique().sort()
                for path in unique_paths:
                    try:
                        logger.info(f"Creating tab for Process Path: {path}")
                        setattr(self, f'missing_area_{cpt}_{path}', pl.DataFrame())
                        # Create path level tab with horizontal split layout
                        path_tab = qtw.QWidget()
                        path_tabview.addTab(path_tab, str(path))
                        
                        # Create horizontal split layout
                        split_layout = qtw.QHBoxLayout()
                        path_tab.setLayout(split_layout)

                        # Filter data for this path
                        path_data = cpt_data.filter(pl.col('process_path') == path)
                        # Narrow Data Scope
                        palatable_data = path_data.select(
                            [
                                pl.col('pick_area').alias('Pick Area'),
                                pl.col('total_cases').alias('Picks').fill_null(0),
                                pl.col('planned_cases').alias('Planned').fill_null(0),
                                pl.col('area_hc').alias('Total HC').fill_null(0),
                                pl.col('target_hc').alias('Target HC'),
                                pl.col('avg_cph').alias('CPH').fill_null(0),
                                pl.col('historical_cph').alias('7-Day CPH').fill_null(0)
                            ]
                        ).filter(pl.col('Pick Area').is_not_null()).sort('Pick Area')

                        try:
                            #logger.debug(f"Creating missing area dataframe for CPT: {cpt}, Path: {path}")
                            setattr(self, f'missing_area_{cpt}_{path}', self.data['Rodeo']['rodeo_full']
                                .filter(
                                    (pl.col('cpt') == cpt) & 
                                    (pl.col('process_path') == path) & 
                                    pl.col('pick_area').is_null()
                                ).select([
                                    'scannable_id',
                                    'o_scannable_id',
                                    'o_o_scannable_id'
                                ])
                            )
                            missing_area = getattr(self, f'missing_area_{cpt}_{path}')

                            #logger.info(f"Found {missing_area.height} rows with missing pick areas")
                            #logger.debug(f"Missing area dataframe shape: {missing_area.shape}")
                        except Exception as e:
                            logger.error(f"Error creating missing area dataframe: {str(e)}")
                            missing_area = pl.DataFrame()  # Return empty dataframe on error
                        

                        logger.info(f"Path-level Palatable Data Assembled: {palatable_data.head}")


                        # Calculate metrics
                        #logger.debug("Beginning metrics calculations")
                        try:
                            path_data = cpt_data.filter(pl.col('process_path') == path)
                            #logger.debug(f"Filtered data for path: {path}")

                            # Calculate basic metrics
                            total_picks = path_data.get_column('total_cases').sum()
                            planned_picks = path_data.get_column('planned_cases').sum()
                            total_hc = path_data.get_column('area_hc').sum()
                            active_hc = path_data.get_column('area_active_hc').sum()
                            target_hc = path_data.get_column('target_hc').sum()

                            #logger.debug(f"Basic metrics calculated - Total Picks: {total_picks}, Total HC: {total_hc}, Active HC: {active_hc}, Target HC: {target_hc}")

                            # Calculate current CPH
                            try:
                                cases_picked = int(path_data.get_column('cases_picked').sum())
                                total_hours = float(path_data.get_column('total_hours').sum())
                                #logger.debug(f"CPH calculation values - Cases Picked: {cases_picked}, Total Hours: {total_hours}")
                                
                                current_cph = float(cases_picked / total_hours)
                                logger.info(f"Current CPH calculated: {current_cph}")
                            except ZeroDivisionError:
                                logger.warning("Zero total hours, setting current_cph to 0")
                                current_cph = 0
                            except Exception as e:
                                logger.error(f"Error calculating current CPH: {str(e)}")
                                current_cph = 0

                            # Calculate historical CPH with null handling
                            try:
                                historical_cph = path_data.get_column('historical_cph').mean()
                                if historical_cph is None:
                                    logger.warning("Historical CPH is None, setting to 0.0")
                                    historical_cph = 0.0
                                logger.info(f"Historical CPH calculated: {historical_cph}")
                            except Exception as e:
                                logger.warning(f"Error calculating historical CPH: {str(e)}")
                                historical_cph = 0.0

                            logger.info(f"Calculated all metrics for {cpt} // {path}")
                            logger.debug(f"Metrics summary:\nTotal Picks: {total_picks}\nPlanned Picks: {planned_picks:.2f}\nTotal HC: {total_hc}\n"
                                        f"Active HC: {active_hc}\nTarget HC: {target_hc}\n"
                                        f"Current CPH: {current_cph}\nHistorical CPH: {historical_cph}")

                        except Exception as e:
                            logger.error(f"Error calculating metrics for path {path}: {str(e)}")
                            raise

                        # Left side - Table container
                        table_container = qtw.QWidget()
                        table_layout = qtw.QVBoxLayout()
                        table_container.setLayout(table_layout)

                        # Create and setup table
                        #table = qtw.QTableWidget()
                        table = CustomTableWidget()
                        table.setRowCount(len(palatable_data))
                        table.setColumnCount(len(palatable_data.columns))
                        table.setHorizontalHeaderLabels(palatable_data.columns)

                        # Get the column index for Target HC
                        table.target_column = palatable_data.columns.index('Target HC')




                        # Get the header and viewport width
                        header = table.horizontalHeader()
                        header.setSectionResizeMode(qtw.QHeaderView.Fixed)  # Start with fixed mode
                        header.setStretchLastSection(False)  # Disable automatic stretch of last section

                        # Set Pick Area column (index 0) to ResizeToContents
                        header.setSectionResizeMode(0, qtw.QHeaderView.ResizeToContents)
                        
                        # Distribute remaining space evenly among other columns
                        remaining_columns = range(1, len(palatable_data.columns))
                        for column in remaining_columns:
                            header.setSectionResizeMode(column, qtw.QHeaderView.Stretch)

                        # Configure the table to expand with its container
                        table.setSizePolicy(
                            qtw.QSizePolicy.Expanding,
                            qtw.QSizePolicy.Expanding
                        )

                        # Make sure the table fills its container
                        table_layout.setContentsMargins(0, 0, 0, 0)
                        table_layout.setSpacing(0)

                        header_tooltips = {
                            'Pick Area': 'Pick Area Name',
                            'Picks': 'Total Cases Remaining',
                            'Total HC': 'Total Headcount',
                            'Active HC': 'Total Active Pickers',
                            'Planned': 'Planned Cases Remaining\nUses % to Pick from Path-level Plan Data',
                            'Target HC': 'Required Headcount\n(Picks / (Hours * Rate))\n\nIf no current "Rate" is available,\nPath-Specific Historical Rate is used (7-day)\n\n\nSelect multiple cells in this column and right-click\nto show summed headcount values.',
                            'CPH': 'Cases Per Hour\n(If any)',
                            '7-Day CPH': '7-day Average Cases Per Hour\n(Used for Path-Specific Historical Rate)'
                        }

                        # Set header tooltips
                        for col_idx, col_name in enumerate(palatable_data.columns):
                            header_item = qtw.QTableWidgetItem(col_name)
                            if col_name in header_tooltips:
                                header_item.setToolTip(header_tooltips[col_name])
                            table.setHorizontalHeaderItem(col_idx, header_item)

                        # Populate table with data
                        logger.info(f"Building Table for {cpt} // {path}")
                        try:
                            for row_idx, row in enumerate(palatable_data.iter_rows(named=True)):
                                try:    
                                    #logger.debug(f"Processing row {row_idx}: {row}")
                                    for col_idx, (col_name, value) in enumerate(row.items()):
                                        #logger.debug(f"Processing column {col_idx}: {col_name} = {value}")
                                        if cpt == 'HOV':
                                            if col_name == 'Planned':
                                                continue
                                            if col_name == 'Target HC':
                                                continue


                                        # Create item and make it read-only
                                        item = qtw.QTableWidgetItem(str(value))
                                        item.setFlags(
                                            qtc.Qt.ItemIsEnabled |  
                                            qtc.Qt.ItemIsSelectable |  
                                            qtc.Qt.ItemIsDropEnabled  
                                        )
                                        item.setTextAlignment(qtc.Qt.AlignCenter)


                                        # Handle CPH column highlighting
                                        if col_name == 'CPH' and current_cph > 0:
                                            try:
                                                cell_cph = float(value) if value else 0
                                                if cell_cph <= (current_cph * 0.5) and cell_cph > 0:
                                                    item.setBackground(qtg.QColor('#ffcccc'))  # Light red background
                                                    item.setToolTip(f"CPH ({cell_cph:.2f}) is significantly below path average ({current_cph:.2f})")
                                            except (ValueError, TypeError) as e:
                                                logger.warning(f"Error processing CPH value in row {row_idx}: {str(e)}")
                
                                        
                                        if col_name == 'Pick Area' and cpt != 'HOV':
                                            logger.info(f"Processing Pick Area for row {row_idx}")
                                            # Gouping logic for pick areas
                                            for group_idx, (group_rows, color) in enumerate(self._group_pick_areas(palatable_data)):
                                                #logger.debug(f"Checking group {group_idx} with {len(group_rows)} rows")
                                                
                                                if row in group_rows:
                                                    logger.info(f"Row {row_idx} belongs to group {group_idx}")
                                                    item.setBackground(color)
                                                    
                                                    try:
                                                        group_sum = sum(float(r['Target HC']) for r in group_rows)
                                                        group_picks = sum(float(r['Picks']) for r in group_rows) or 0
                                                        #logger.debug(f"Group {group_idx} - Sum Target HC: {group_sum}, Total Picks: {group_picks}")
                                                    except ValueError as e:
                                                        logger.error(f"Error calculating group sums: {e}")
                                                        continue
                                                    
                                                    # Calculate weighted average CPH for the group
                                                    picks_and_cph = []
                                                    for group_row in group_rows:
                                                        try:
                                                            picks = float(group_row['Planned']) or 0
                                                            area_cph = float(group_row['CPH']) or 0
                                                            historical_cph = float(group_row['7-Day CPH']) or 0
                                                            
                                                            #logger.debug(f"Row data - Picks: {picks}, CPH: {area_cph}, Historical CPH: {historical_cph}")
                                                            
                                                            effective_cph = area_cph if area_cph > 0 else historical_cph
                                                            if effective_cph > 0:
                                                                picks_and_cph.append((picks, effective_cph))
                                                                #logger.debug(f"Added to picks_and_cph: ({picks}, {effective_cph})")
                                                        except ValueError as e:
                                                            logger.error(f"Error processing CPH calculations: {e}")
                                                            continue

                                                    if picks_and_cph:
                                                        total_picks = sum(picks for picks, _ in picks_and_cph)
                                                        #logger.debug(f"Total picks for group {group_idx}: {total_picks}")
                                                        
                                                        if total_picks > 0:
                                                            group_cph = sum(picks * cph for picks, cph in picks_and_cph) / total_picks
                                                            using_historical = any(float(r['CPH']) == 0 and float(r['7-Day CPH']) > 0 for r in group_rows)
                                                            logger.info(f"Group {group_idx} - Weighted CPH: {group_cph:.2f} (Using historical: {using_historical})")
                                                        else:
                                                            group_cph = 0
                                                            using_historical = False
                                                            logger.warning(f"Group {group_idx} has picks_and_cph data but total_picks is 0")
                                                    else:
                                                        group_cph = 0
                                                        using_historical = False
                                                        logger.warning(f"No valid picks_and_cph data for group {group_idx}")

                                                    # Process group areas and tooltips
                                                    try:
                                                        #logger.debug(f"Processing group areas for row {row_idx}")
                                                        group_areas = [str(r['Pick Area']) for r in group_rows if r['Pick Area'] is not None]
                                                        #logger.debug(f"Found group areas: {group_areas}")

                                                        if group_areas:
                                                            logger.info(f"Creating tooltip for group with {len(group_areas)} areas")
                                                            tooltip_text = [
                                                                f"Group Total HC: {group_sum:.2f}",
                                                                f"Areas: {', '.join(group_areas)}",
                                                            ]
                                                            #logger.debug(f"Initial tooltip text: {tooltip_text}")

                                                            # Calculate time to exhaust
                                                            try:
                                                                #logger.debug(f"Calculating time to exhaust - Initial values: group_sum={group_sum}, group_picks={group_picks}, group_cph={group_cph}")
                                                                
                                                                if 0 < group_sum < 1:
                                                                    logger.info(f"Adjusting group_sum from {group_sum} to 1 (minimum threshold)")
                                                                    group_sum = 1
                                                                
                                                                if group_cph > 0:
                                                                    time_to_exhaust = round(group_picks / (group_sum * group_cph), 2)
                                                                    logger.info(f"Calculated time to exhaust: {time_to_exhaust} hours")
                                                                else:
                                                                    time_to_exhaust = None
                                                                    logger.warning("Unable to calculate time to exhaust - group_cph is 0 or negative")
                                                                    
                                                            except ZeroDivisionError:
                                                                logger.warning(f"ZeroDivisionError in time to exhaust calculation: group_sum={group_sum}, group_cph={group_cph}")
                                                                time_to_exhaust = None
                                                            except Exception as e:
                                                                logger.error(f"Unexpected error calculating time to exhaust: {str(e)}")
                                                                time_to_exhaust = None

                                                            # Add time to exhaust to tooltip
                                                            if time_to_exhaust is not None:
                                                                tooltip_text.append(f"Time to Exhaust: {time_to_exhaust:.2f} hours")
                                                                #logger.debug(f"Added time to exhaust to tooltip: {time_to_exhaust:.2f} hours")
                                                            else:
                                                                tooltip_text.append("Time to Exhaust: No current rate available")
                                                                #logger.debug("Added 'No current rate available' message to tooltip")

                                                            if using_historical:
                                                                tooltip_text.append("Note: Some areas using 7-day historical rates")
                                                                #logger.debug("Added historical rates notice to tooltip")

                                                            try:
                                                                tooltip_final = '\n'.join(tooltip_text)
                                                                item.setToolTip(tooltip_final)
                                                                #logger.debug(f"Set tooltip for item: {tooltip_final}")
                                                            except Exception as e:
                                                                logger.error(f"Error setting tooltip: {str(e)}")

                                                            break
                                                        else:
                                                            logger.warning(f"No valid areas found for group in row {row_idx}")

                                                    except Exception as e:
                                                        logger.error(f"Error processing group areas and tooltips: {str(e)}")
                                                        continue

                                        try:
                                            #logger.debug(f"Setting table item at position ({row_idx}, {col_idx})")
                                            table.setItem(row_idx, col_idx, item)
                                            #logger.debug("Successfully set table item")
                                        except Exception as e:
                                            logger.error(f"Error setting table item at ({row_idx}, {col_idx}): {str(e)}")

                                except Exception as e:
                                    logger.error(f"Error processing row {row_idx}: {str(e)}")
                                    continue
                        except Exception as e:
                            logger.error(f"Error processing column {col_idx}: {str(e)}")
                            continue

                    except Exception as e:
                        logger.error(f"Error creating tab for Process Path {path}: {str(e)}")
                        continue

                    logger.info(f"Populated Table for {cpt} // {path}")

                    try:
                        #logger.debug("Configuring table display properties")
                        table.resizeRowsToContents()
                        table.setHorizontalScrollBarPolicy(qtc.Qt.ScrollBarAsNeeded)
                        table.setVerticalScrollBarPolicy(qtc.Qt.ScrollBarAsNeeded)

                        # Add table to table layout
                        #logger.debug("Adding table to table layout")
                        table_layout.addWidget(table)
                        table_layout.setContentsMargins(0, 0, 0, 0)
                        table_layout.setSpacing(0)

                        # Set size policy for table container
                        #logger.debug("Setting size policy for table container")
                        table_container.setSizePolicy(
                            qtw.QSizePolicy.Expanding,
                            qtw.QSizePolicy.Expanding
                        )
                    except Exception as e:
                        logger.error(f"Error configuring table display: {str(e)}")

                    logger.info(f"Building Key Metrics for {cpt} // {path}")
                    try:
                        # Right side - Key Metrics container
                        metrics_container = qtw.QWidget()
                        metrics_layout = qtw.QVBoxLayout()
                        metrics_container.setLayout(metrics_layout)

                        # Set a fixed width for the metrics container
                        metrics_container.setFixedWidth(200)  # Adjust this value as needed
                        # Set size policy to prevent horizontal expansion while allowing vertical
                        metrics_container.setSizePolicy(
                            qtw.QSizePolicy.Fixed,  # Changed from Expanding to Fixed
                            qtw.QSizePolicy.Expanding
                        )
                        

                        # Add key metrics title
                        #logger.debug("Setting up metrics title")
                        metrics_title = qtw.QLabel("Key Metrics")
                        metrics_title.setFont(qtg.QFont('Helvetica', 12, qtg.QFont.Bold))
                        metrics_layout.addWidget(metrics_title, alignment=qtc.Qt.AlignTop | qtc.Qt.AlignHCenter)


                        def create_metric_label(label: str, value: str) -> qtw.QLabel:
                            """Create a formatted metric label with bold description and centered text"""
                            try:
                                #logger.debug(f"Creating metric label: {label} = {value}")
                                label_widget = qtw.QLabel(f"<b>{label}:</b>")
                                if label == "Current (Active) HC":
                                        path_workforce_url = f"https://picking-console.na.picking.aft.a2z.com/fc/{self.site_code}/pick-workforce"
                                        value_widget = qtw.QLabel(f'<a href="{path_workforce_url}" style="color: blue;">{value}</a>')
                                        value_widget.setOpenExternalLinks(True) 
                                        value_widget.setTextInteractionFlags(qtc.Qt.TextBrowserInteraction)
                                else:
                                    value_widget = qtw.QLabel(f"{value}")

                                label_widget.setAlignment(qtc.Qt.AlignCenter)
                                value_widget.setAlignment(qtc.Qt.AlignCenter)

                                body_layout.addWidget(label_widget)
                                body_layout.addWidget(value_widget)

                                #logger.debug(f"Successfully created metric label for {label}")
                                return
                            except Exception as e:
                                logger.error(f"Error creating metric label for {label}: {str(e)}")
                                return

                        body_container = qtw.QWidget()
                        body_container.setObjectName("body_container")
                        body_layout = qtw.QVBoxLayout(body_container)
                        body_layout.setSpacing(3)

                        # Create metric labels
                        #logger.debug("Creating metric display labels")
                        #if planned_picks > 0:
                        create_metric_label("Planned Picks", f"{planned_picks:.2f}")
                        #else:
                        #    create_metric_label("Picks Remaining", str(total_picks))
                        create_metric_label("Current (Active) HC", f"{str(total_hc)} ({str(active_hc)})")
                        create_metric_label("Target HC", f"{target_hc:.2f}")
                        create_metric_label("Current CPH", f"{current_cph:.2f}")
                        create_metric_label("7-Day CPH", f"{historical_cph:.2f}")

                        # Calculate and display time to exhaust if possible
                        #logger.debug(f"Calculating final time to exhaust - total_hc: {total_hc}, current_cph: {current_cph}")
                        if total_hc > 0 and current_cph > 0:
                            try:
                                time_to_exhaust = planned_picks / (total_hc * current_cph)
                                logger.info(f"Final time to exhaust calculated: {time_to_exhaust:.2f} hours")
                                create_metric_label("Time to Exhaust", f"{time_to_exhaust:.2f} hours")
                            except Exception as e:
                                logger.error(f"Error calculating final time to exhaust: {str(e)}")
                        else:
                            logger.warning("Cannot calculate time to exhaust - insufficient data")


                        # Add the Missing Pick Area label if there are missing areas
                        if missing_area.height > 0:
                            missing_area_label = qtw.QPushButton(f"Missing Pick Area: {missing_area.height}")
                            
                            # Make it look like a button
                            missing_area_label.setStyleSheet("""
                                QLabel {
                                    color: white;
                                    background-color: #0066cc;
                                    padding: 8px 12px;
                                    border-radius: 4px;
                                }
                                QLabel:hover {
                                    background-color: #003d99;
                                }
                            """)
                            
                            missing_area_label.setFont(qtg.QFont("Helvetica", 9))
                            missing_area_label.clicked.connect(
                                lambda checked, c=cpt, p=path: self.show_missing_areas_dialog(c, p)
                            )
                            body_layout.addWidget(missing_area_label, alignment=qtc.Qt.AlignCenter)



                        metrics_layout.addWidget(body_container)
                        body_container.setStyleSheet("""
                                #body_container {
                                    background-color: #f5f5f5;
                                    border: 1px solid #dcdcdc;
                                    border-radius: 10px;
                                    margin: 5px;
                                }
                                QLabel {
                                    background-color: transparent;
                                    padding: 2px;
                                }
                            """)
                        # Finalize layout
                        #logger.debug("Finalizing metrics layout")
                        metrics_layout.addStretch()

                        # Add containers to split layout
                        #logger.debug("Setting up final split layout")
                        split_layout.addWidget(table_container, stretch=3)
                        split_layout.addWidget(metrics_container, stretch=1)

                        # Set margins and spacing
                        split_layout.setContentsMargins(5, 5, 5, 5)
                        split_layout.setSpacing(10)

                        logger.info(f"Populated Key Metrics for {cpt} // {path}")

                    except Exception as e:
                        error_msg = f"Error creating table for path {path}: {str(e)}"
                        print(error_msg)
                        logger.error(error_msg)
                        pass

                    finally:
                        logger.info(f"Completed processing for {cpt} // {path}")
            except Exception as e:
                logger.error(f"Error processing CPT {cpt}: {str(e)}")
                continue

    def _group_pick_areas(self, palatable_data):
        """Helper function to identify optimal groups based on geography and target HC"""
        groups = []
        current_group = []
        current_sum = 0
        sorted_data = palatable_data
        MAX_HC = 1.5 # Set maximum headcount tolerance
        MAX_FRACTION = 0.05 # Set maximum fraction tolerance

        # Predefined colors for groups
        group_colors = [
            qtg.QColor(200, 255, 200, 255, 80),  # Darker mint/green
            qtg.QColor(200, 200, 255, 255, 80),  # Darker periwinkle/blue
            qtg.QColor(255, 255, 180, 255, 80),  # Darker pastel yellow
            qtg.QColor(255, 200, 255, 255, 80),  # Darker lavender/purple
            qtg.QColor(180, 255, 255, 255, 80),  # Darker cyan
            qtg.QColor(255, 220, 180, 255, 80),  # Darker peach
            qtg.QColor(220, 208, 255, 255, 80),  # Darker lilac
            qtg.QColor(188, 232, 241, 255, 80),  # Light sky blue
            qtg.QColor(230, 230, 250, 255, 80),  # Light lavender
            qtg.QColor(255, 218, 185, 255, 80),  # Peach puff
            qtg.QColor(176, 224, 230, 255, 80),  # Powder blue
            qtg.QColor(221, 160, 221, 255, 80),  # Plum
            qtg.QColor(240, 230, 140, 255, 80)   # Khaki
        ]


        def are_adjacent(area1, area2):
            """Check if two pick areas are adjacent based on naming patterns"""
            try:
                # Handle None values or empty strings
                if not area1 or not area2:
                    return False
                
                # Convert areas to strings and uppercase for consistency
                area1, area2 = str(area1).upper(), str(area2).upper()
                
                # Extract the base name and number for each area
                def parse_area(area):
                    # Find the last sequence of digits in the string
                    match = re.search(r'([A-Z-]+)(\d+)$', area)
                    if not match:
                        print(f"Warning: Could not parse area {area}")
                        return None, None
                    return match.group(1), int(match.group(2))
                
                base1, num1 = parse_area(area1)
                base2, num2 = parse_area(area2)
                
                # If we couldn't parse either area, they're not adjacent
                if None in (base1, num1, base2, num2):
                    return False
                
                # Areas are adjacent if they share the same base name and numbers differ by 2 or less
                return base1 == base2 #and abs(num1 - num2) < 50
                    
            except Exception as e:
                logger.warning(f"Error comparing areas {area1} and {area2}: {str(e)}")
                return False

        def find_nearest_whole(value, max_hc=MAX_HC):
            """Find nearest whole number and determine if within tolerance"""
            if value > max_hc:
                return value, False 
            nearest = round(value)
            if nearest == 0:
                nearest = 1
            return nearest, abs(value - nearest) <= MAX_FRACTION

        # Process each row
        for row in sorted_data.iter_rows(named=True):
            target_hc = float(row['Target HC'])
                    
            # If we already have a group
            if current_group:
                new_sum = current_sum + target_hc
                nearest, is_close = find_nearest_whole(new_sum, max_hc=MAX_HC)  # Set max_hc as needed
                
                # Check if adding this area would:
                # 1. Exceed tolerance
                # 2. Break adjacency
                # 3. Exceed max headcount
                if (not is_close and new_sum > nearest) or \
                not are_adjacent(row['Pick Area'], current_group[-1]['Pick Area']) or \
                new_sum > MAX_HC:  # Explicit max HC check
                    # Save current group and start new one
                    if current_group:
                        color = group_colors[len(groups) % len(group_colors)]
                        groups.append((current_group, color))
                    current_group = []
                    current_sum = 0
                else:
                    current_sum = new_sum
                    current_group.append(row)
                    # If we've hit a good grouping point, save it
                    if is_close:
                        color = group_colors[len(groups) % len(group_colors)]
                        groups.append((current_group, color))
                        current_group = []
                        current_sum = 0
                    continue
            
            # Start new group
            current_group.append(row)
            current_sum = target_hc
            
            # Check if this single area is close to a whole number
            nearest, is_close = find_nearest_whole(current_sum)
            if is_close:
                color = group_colors[len(groups) % len(group_colors)]
                groups.append((current_group, color))
                current_group = []
                current_sum = 0
        
        # Add remaining items as a group if any exist
        if current_group:
            color = group_colors[len(groups) % len(group_colors)]
            groups.append((current_group, color))
        
        return groups


    def show_missing_areas_dialog(self, cpt, path):
        """Display missing pick areas data in a dialog window."""
        try:
            logger.debug(f"Attempting to show missing areas dialog for CPT: {cpt}, Path: {path}")
            attr_name = f'missing_area_{cpt}_{path}'
            
            if hasattr(self, attr_name):
                df = getattr(self, attr_name)
                logger.debug(f"Found missing areas dataframe with {df.height} rows")
                
                if df.height > 0:
                    dialog = qtw.QDialog(self)
                    dialog.setWindowTitle(f"Missing Pick Areas Details - {path}")
                    
                    # Create layout for the dialog
                    layout = qtw.QVBoxLayout(dialog)
                    
                    # Create table widget
                    table = qtw.QTableWidget()
                    table.verticalHeader().hide()

                    df = getattr(self, f'missing_area_{cpt}_{path}').sort('o_o_scannable_id')
                    
                    # Set table dimensions
                    table.setRowCount(len(df))
                    table.setColumnCount(len(df.columns))
                    
                    # Set headers
                    table.setHorizontalHeaderLabels(df.columns)
                    
                    # Populate table
                    for row_idx, row in enumerate(df.iter_rows(named=True)):
                        for col_idx, (col_name, value) in enumerate(row.items()):
                            item = qtw.QTableWidgetItem(str(value))
                            table.setItem(row_idx, col_idx, item)
                    
                    # First resize to get content widths
                    table.resizeColumnsToContents()

                    # Find the maximum width among all columns
                    max_width = 0
                    for col in range(table.columnCount()):
                        max_width = max(max_width, table.columnWidth(col))

                    # Set all columns to the maximum width
                    for col in range(table.columnCount()):
                        table.setColumnWidth(col, max_width)
                    
                    # Add copy button
                    copy_button = qtw.QPushButton("Copy to Clipboard")
                    def copy_to_clipboard():
                        # Create tab-separated string from table
                        header = '\t'.join(df.columns)
                        data = df.to_csv(index=False, sep='\t', header=False)
                        qtw.QApplication.clipboard().setText(f"{header}\n{data}")
                        
                    copy_button.clicked.connect(copy_to_clipboard)
                    
                    # Style the table
                    table.setStyleSheet("""
                        QTableWidget {
                            gridline-color: #d0d0d0;
                            border: 1px solid #d0d0d0;
                        }
                        QHeaderView::section {
                            background-color: #f0f0f0;
                            padding: 5px;
                            border: 1px solid #d0d0d0;
                            font-weight: bold;
                        }
                    """)
                    
                    # Calculate sizes
                    screen = qtw.QApplication.primaryScreen().geometry()
                    header_width = sum(table.columnWidth(i) for i in range(table.columnCount()))
                    content_height = (table.rowHeight(0) * (table.rowCount())) + table.horizontalHeader().height()
                    
                    # Set sizes with padding and limits
                    width = min(header_width + 25, screen.width() * 0.8)
                    height = screen.height() * 0.3
                    
                    # Add widgets to layout
                    layout.addWidget(table)
                    layout.addWidget(copy_button)
                    
                    # Set dialog size
                    dialog.resize(width, height)
                    
                    # Center the dialog on the screen
                    dialog.setModal(True)
                    dialog.exec()

                    logger.info(f"Successfully created and showed dialog for {cpt} // {path}")
                else:
                    logger.warning(f"No missing areas to display for {cpt} // {path}")
            else:
                logger.warning(f"Missing area attribute {attr_name} not found")
                
        except Exception as e:
            logger.error(f"Error showing missing areas dialog: {str(e)}")
            

## PLAN TAB MOVED TO INDEPENDANT MODULE ##


class SettingsTab(qtw.QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.parent = parent
        layout = qtw.QVBoxLayout()
        self.setLayout(layout)

        self.time_manager = TimeManager.get_instance()
        self.shift_info = self.time_manager.get_shift_info()
        self.site_info = SiteBuilder.get_instance().get_site_info()
        self.pick_areas = self.site_info['pick_areas']

        self.site_code = self.shift_info['site_code']
        self.timezone = self.shift_info['timezone']
        self.shift_start = self.shift_info['shift_start'].hour
        self.shift_end = self.shift_info['shift_end'].hour
        

        self.is_closing = False
        self.refresh_state = False
        self.populate_settings_tab()


    def populate_settings_tab(self):
        """
        Populates the 'Settings' tab with input fields for pick areas, buttons for adding/removing pick areas,
        and a display area for showing existing pick areas.
        """


        # TODO
        # Add a checkbox to enable/disable auto_refresh
        # Add an up/down selector box that adjusts the refresh rate
        # Add a checkbox to enable/disable tooltips

        # Pick Area Settings Container
        self.area_settings = qtw.QFrame(self)
        self.area_settings_layout = qtw.QHBoxLayout(self.area_settings)

        # Create the input frame
        self.input_frame = qtw.QFrame(self.area_settings)
        self.input_layout = qtw.QFormLayout(self.input_frame)

        self.input_layout.addItem(qtw.QSpacerItem(0, 20))

        
        # Create horizontal layout for refresh controls
        self.refresh_layout = qtw.QHBoxLayout()
        
        # Create auto refresh checkbox
        self.refresh_checkbox = qtw.QCheckBox("Auto Refresh")
        self.refresh_checkbox.setChecked(False)
        self.refresh_checkbox.setToolTip("Enable automatic refresh of data")
        self.refresh_checkbox.checkStateChanged.connect(self.handle_refresh_state)
        self.refresh_layout.addWidget(self.refresh_checkbox)
        
        # Create spinbox for refresh interval
        self.refresh_spinbox = qtw.QSpinBox()
        self.refresh_spinbox.setMinimum(2)  # Minimum 2 minutes
        self.refresh_spinbox.setMaximum(60)  # Maximum 60 minutes
        self.refresh_spinbox.setValue(10)    # Default 10 minutes
        self.refresh_spinbox.setSuffix(" minutes")
        self.refresh_spinbox.setToolTip("Set auto-refresh interval")
        self.refresh_spinbox.textChanged.connect(self.handle_refresh_state)
        self.refresh_layout.addWidget(self.refresh_spinbox)
        
        # Add refresh layout to main layout
        self.input_layout.addRow(self.refresh_layout)


        # Create horizontal layout for pad time controls
        self.pad_layout = qtw.QHBoxLayout()
        
        # Create pad time checkbox
        self.pad_checkbox = qtw.QCheckBox("Pad Time")
        self.pad_checkbox.setChecked(False)
        self.pad_checkbox.setToolTip("Enable Pad Time for processing")
        self.pad_checkbox.checkStateChanged.connect(self.handle_pad_state)
        self.pad_layout.addWidget(self.pad_checkbox)
        
        # Create spinbox for refresh interval
        self.pad_spinbox = qtw.QSpinBox()
        self.pad_spinbox.setMinimum(0)  # Minimum 0 minutes
        self.pad_spinbox.setMaximum(500)  # Maximum 60 minutes
        self.pad_spinbox.setValue(0)    # Default 0 minutes
        self.pad_spinbox.setSuffix(" minutes")
        self.pad_spinbox.setToolTip("Subtracts assigned value from time remaining")
        self.pad_spinbox.textChanged.connect(self.handle_pad_state)
        self.pad_layout.addWidget(self.pad_spinbox)
        
        # Add refresh layout to main layout
        self.input_layout.addRow(self.pad_layout)

        

        
        # Add tooltip checkbox
        #self.tooltip_checkbox = qtw.QCheckBox("Enable Tooltips")
        #self.tooltip_checkbox.setChecked(True)
        #self.tooltip_checkbox.setToolTip("Show detailed tooltips throughout the application")
        #self.tooltip_checkbox.checkStateChanged.connect(self.handle_tooltip_state)
        #self.input_layout.addWidget(self.tooltip_checkbox)
        
        
        #self.webhook_button = qtw.QPushButton("WebHook Settings")
        #self.webhook_button.clicked.connect(self.open_webhook_dialog)
        
        #self.input_layout.addWidget(self.webhook_button)

        self.area_settings_layout.addWidget(self.input_frame)

        # Create the display frame
        self.display_frame = qtw.QFrame(self.area_settings)
        self.display_layout = qtw.QVBoxLayout(self.display_frame)

        header_label = qtw.QLabel('Pick Areas')
        header_label.setFont(qtg.QFont("Helvetica", 14))
        header_label.setAlignment(qtc.Qt.AlignCenter)
        self.display_layout.addWidget(header_label)

        self.display_layout.addItem(qtw.QSpacerItem(0, 5))
        self.display_area = qtw.QListWidget()
        self.update_button = qtw.QPushButton("Update Pick Areas")
        self.update_button.clicked.connect(self.update_pick_areas)

        self.display_layout.addWidget(self.display_area)
        self.display_layout.addWidget(self.update_button)
        self.area_settings_layout.addWidget(self.display_frame)

        # Ensure the layout stretches to fill the entire tab
        self.area_settings_layout.setStretch(0, 1)
        self.area_settings_layout.setStretch(1, 1)

        # Add the area settings frame to the tab settings layout
        self.layout().addWidget(self.area_settings)


        """
        # Dropdown for color themes
        themes = ["dark"]  # Valid theme names
        self.selected_theme = qtw.QComboBox()
        self.selected_theme.addItems(themes)  # Add themes to the dropdown
        self.selected_theme.setCurrentText("blue")  # Default theme
        self.selected_theme.currentTextChanged.connect(self.change_theme)  # Connect to change_theme method
        self.layout().addWidget(self.selected_theme)
        """
        # Solidify initial states
        self.handle_refresh_state()
        self.handle_pad_state()
        #self.handle_tooltip_state()

        # Load saved pick areas
        self.update_pick_areas_display()


    def handle_refresh_state(self):
        """Handle state changes for auto-refresh checkbox."""
        self.refresh_state = self.refresh_checkbox.isChecked()
        logger.info(f"Refresh Check State: {self.refresh_state}")
        
        # Sync with parent
        self.parent.is_auto_refreshing = self.refresh_state
        
        if self.refresh_state:
            self.enable_refresh()
            self.refresh_spinbox.setEnabled(True)
        else:
            self.disable_refresh()
            self.refresh_spinbox.setEnabled(False)

    def handle_pad_state(self):
        """Handle state changes for pad_time checkbox."""
        self.pad_state = self.pad_checkbox.isChecked()
        logger.info(f"Pad Time Check State: {self.pad_state}")
        
        # Sync with parent
        self.parent.is_pad_set = self.pad_state
        
        if self.pad_state:
            self.enable_pad()
            self.pad_spinbox.setEnabled(True)
        else:
            self.disable_pad()
            self.pad_spinbox.setEnabled(False)

    def enable_pad(self):
        """Enable the auto-refresh functionality."""
        value = self.pad_spinbox.value() * 60 # in seconds
        seconds = int(value)
        microseconds = int((value - seconds) * 1000000)
        self.time_manager._shift_time.pad_time = td(seconds=seconds, microseconds=microseconds)
        logger.info("Pad Time enabled: {} minutes".format(self.pad_spinbox.value()))

    def disable_pad(self):
        """Disable the auto-refresh functionality."""
        self.time_manager._shift_time.pad_time = td(seconds=0, microseconds=0)
        logger.info("Pad Time disabled")

    def handle_tooltip_state(self):
        """Handle state changes for tooltip checkbox."""
        logger.info(f"Tooltip Check State: {self.tooltip_checkbox.isChecked()}")
        
        self.tooltip_state = self.tooltip_checkbox.isChecked()

        if self.tooltip_state:
            self.enable_tooltips()
        else:
            self.disable_tooltips()


    def enable_tooltips(self):
        """Enable tooltips for relevant widgets."""
        widgets_with_tooltips = [
            (self.refresh_checkbox, "Enable automatic refresh of data"),
            (self.refresh_spinbox, "Set auto-refresh interval"),
            (self.tooltip_checkbox, "Show detailed tooltips throughout the application"),
            #(self.shift_progress, "Percentage of total shift time that has passed.\nMain comparator for % To Plan values")
            # Add other widgets and their tooltips here
        ]

        # Add tooltips for header labels
        header_tooltips = {
            "Metric": "The type of metric being displayed",
            "Plan": "The planned value for the metric.\nSet these in the Plan tab",
            "Actual": "Current reported values. \n- Rate: Overall average since SoS (LPI)\n- Headcount: Current workforce (Picking Console)\n- Hours: Total tracked labor since SoS (LPI)\n- Volume: Cases picked since SoS (LPI)",
            "% To Plan": "Comparative value, measured against the submitted plan.\nColor coding is based on the shift progress percentage\n\ne.g. If shift progress is 20%, we'll see the following:\n- Rate: 104%, green\n-Headcount: 96%, green \n- Hours: 14.9%, red\n- Volume: 25.1%, red"
        }

        """
        for header, tooltip in header_tooltips.items():
            if header in self.overview_header_labels:
                self.overview_header_labels[header].setToolTip(tooltip)
                logger.info(f"Tooltip enabled for header: {header}\n{tooltip}")
                """

        for widget, tooltip in widgets_with_tooltips:
            widget.setToolTip(tooltip)
            logger.info(f"Tooltip enabled for widget: {widget}\n{tooltip}")
        logger.info("Tooltips enabled")

    def disable_tooltips(self):
        """Disable tooltips for relevant widgets."""
        widgets_with_tooltips = [
            self.refresh_checkbox,
            self.refresh_spinbox,
            self.tooltip_checkbox,
            # Add other widgets here
        ]
        for widget in widgets_with_tooltips:
            widget.setToolTip("")
        logger.info("Tooltips disabled")



    def enable_refresh(self):
        """Enable the auto-refresh functionality."""
        self.auto_refresh_interval = self.refresh_spinbox.value()
        # Pass with change awareness to prevent immediate run 
        self.start_auto_refresh(True) 

        logger.info("Auto-refresh enabled with interval: {} minutes".format(self.refresh_spinbox.value()))

    def disable_refresh(self):
        """Disable the auto-refresh functionality."""
        self.stop_auto_refresh()
        logger.info("Auto-refresh disabled")

    def start_auto_refresh(self, changed = False):
        """
        Starts the auto-refresh process with proper cleanup
        """
        if not changed:
            try:
                # Debug logger to track execution
                logger.info("Starting auto-refresh process")
                
                # Stop any existing refresh processes
                self.stop_auto_refresh()
                
                # Set state
                self.is_auto_refreshing = True
                
                # Run initial refresh only once
                #self.run_it()

                #self.display_staged_warnings()
                
                # Setup timer for next refresh
                if not hasattr(self, 'auto_refresh_timer') or self.auto_refresh_timer is None:
                    #Create new QTimer
                    self.auto_refresh_timer = QTimer()
                    self.auto_refresh_timer.setSingleShot(True)
                    
                    # Ensure we only connect the signal once
                    try:
                        self.auto_refresh_timer.timeout.disconnect()
                    except TypeError:
                        pass  # No existing connections
                    self.auto_refresh_timer.timeout.connect(self.run_auto_refresh)
                
                # Start timer
                logger.info("Starting timer")
                self.auto_refresh_timer.start(self.auto_refresh_interval * 60000)
                
                # Update button state
                self.parent.go_button.setText("Refresh")
                
            except Exception as e:
                print(f"Error in start_auto_refresh: {str(e)}")
                self.is_auto_refreshing = False
                self.stop_auto_refresh()

        else:
            try:
                # Debug logger to track execution
                logger.info("Refresh Timer Adjusted")
                
                # Stop any existing refresh processes
                self.stop_auto_refresh()
                
                # Set state
                self.is_auto_refreshing = True

                # Setup timer for next refresh
                if not hasattr(self, 'auto_refresh_timer') or self.auto_refresh_timer is None:
                    #Create new QTimer
                    self.auto_refresh_timer = QTimer()
                    self.auto_refresh_timer.setSingleShot(True)
                    
                    # Ensure we only connect the signal once
                    try:
                        self.auto_refresh_timer.timeout.disconnect()
                    except TypeError:
                        pass  # No existing connections
                    self.auto_refresh_timer.timeout.connect(self.run_auto_refresh)
                
                # Start timer
                logger.info("Starting timer")
                self.auto_refresh_timer.start(self.auto_refresh_interval * 60000)
                # Update button state
                self.parent.go_button.setText("Refresh")
                
            except Exception as e:
                print(f"Error in start_auto_refresh: {str(e)}")
                self.is_auto_refreshing = False
                self.stop_auto_refresh()

    def run_auto_refresh(self):
        """
        Handles a single refresh cycle
        """
        logger.info("Auto-refresh cycle started")
        
        if not self.is_auto_refreshing or self.is_closing:
            logger.info("Auto-refresh stopped: refresh disabled or application closing")
            return
            
        try:
            # Run the refresh
            self.parent.run_it()
            
            # Schedule next refresh only if still auto-refreshing
            if self.is_auto_refreshing and not self.is_closing:
                logger.info("Scheduling next auto-refresh")
                self.auto_refresh_timer.start(self.auto_refresh_interval * 60000)
                
        except Exception as e:
            logger.info(f"Error in run_auto_refresh: {str(e)}")
            self.stop_auto_refresh()

    def stop_auto_refresh(self):
        """
        Stops all auto-refresh processes
        """
        self.is_auto_refreshing = False
        
        # Stop QTimer if exists
        if hasattr(self, 'auto_refresh_timer') and self.auto_refresh_timer is not None:
            self.auto_refresh_timer.stop()
            try:
                self.auto_refresh_timer.timeout.disconnect()
            except TypeError:
                pass
            self.auto_refresh_timer = None
            logger.info("Auto-Refresh Timer Stopped")



    def open_webhook_dialog(self):
        """
        Creates a dialog for entering the webhook URL
        Webhook will update the slack_url.txt file with the entered webhook URL

        In slack, the hook will send messages to update when the plan is updated
        It will provide the following variables:
        - Site Code
        - Volume
        - Rate
        - Hours
        - Message
        - Submitted By
        - Timestamp
        """

        logger.info('=== Starting open_webhook_dialog ===')
        self.webhook_dialog = qtw.QDialog(self)
        self.webhook_dialog.setWindowTitle("Enter Webhook URL")
        self.webhook_dialog_layout = qtw.QVBoxLayout(self.webhook_dialog)
        self.webhook_dialog.setModal(True)
        self.webhook_dialog.setWindowFlags(
                self.webhook_dialog.windowFlags() |
                qtc.Qt.WindowStaysOnTopHint
            )

        self.webhook_url_entry = qtw.QLineEdit()
        self.webhook_dialog_layout.addWidget(self.webhook_url_entry)

        self.webhook_dialog_layout.addSpacing(10)

        # Explanatory text
        info_text = """This webhook will provide the following variables:
    - site_code : Site Code
    - volume : Planned Volume
    - rate : Planned Rate
    - hours : Planned Hours
    - message : Optional message
    - submitted_by : User who submitted the plan
    - timestamp : Timestamp of the plan submission
    - summary : Summary of the plan update"""
        
        info_label = qtw.QLabel(info_text)
        info_label.setStyleSheet("font-family: Consolas, monospace;") 
        self.webhook_dialog_layout.addWidget(info_label, alignment=qtc.Qt.AlignCenter)

        def save_webhook():
            """
            Saves the webhook URL to a network or local file and closes the dialog.
            """
            logger.info('=== Starting save_webhook ===')
            url = self.webhook_url_entry.text()
            # Try network path first
            try:
                base_path = r"\\ant\dept-na\SAV7\Public"
                pickassist_path = os.path.join(base_path, "PickAssist")
                plan_dir = os.path.join(pickassist_path, "Hooks", self.site_code)
                
                logger.info(f'Attempting to save to network path: {plan_dir}')

                # Create directories if they don't exist
                if not os.path.exists(plan_dir):
                    logger.info(f'Creating directory structure: {plan_dir}')
                    os.makedirs(plan_dir, exist_ok=True)

                # Generate network filename with timestamp
                network_filename = f"{self.site_code}_PlanHook.txt"
                network_path = os.path.join(plan_dir, network_filename)
                
                logger.info(f'Attempting to write to network file: {network_path}')
                with open(network_path, 'w') as f:
                    f.write(str(url))
                logger.info('Successfully saved to network location')

            except Exception as e:
                logger.warning(f'Failed to save to network location: {str(e)}')
                logger.warning('Falling back to local storage')
                
                # Fall back to local storage
                resource_path = self.find_resource(f"{self.site_code}_PlanHook.txt")
                logger.info(f'Using local resource path: {resource_path}')
                
                # Ensure local directory exists
                local_dir = os.path.dirname(resource_path)
                if not os.path.exists(local_dir):
                    logger.info(f'Creating local directory: {local_dir}')
                    os.makedirs(local_dir, exist_ok=True)
                
                # Save to local file
                with open(resource_path, 'w') as f:
                    f.write(str(url))
                logger.info('Successfully saved to local location')
            
            self.webhook_dialog.accept()  # Close the dialog after saving

        self.save_webhook_button = qtw.QPushButton("Save Webhook")
        self.save_webhook_button.clicked.connect(save_webhook)  # Connect without calling
        self.webhook_dialog_layout.addWidget(self.save_webhook_button)

        # Move to top and wait for the dialog to be closed
        self.webhook_dialog.activateWindow()
        self.webhook_dialog.raise_()
        self.webhook_dialog.exec_()  # Show the dialog

    def load_webhook(self):
        """
        Loads the webhook URL from network or local file and stores it in self.webhook_url.
        Returns True if successful, False if no webhook URL could be loaded.
        """
        logger.info('=== Starting load_webhook ===')
        self.webhook_url = None

        # Try network path first
        try:
            base_path = r"\\ant\dept-na\SAV7\Public"
            pickassist_path = os.path.join(base_path, "PickAssist")
            plan_dir = os.path.join(pickassist_path, "Hooks", self.site_code)
            network_filename = f"{self.site_code}_PlanHook.txt"
            network_path = os.path.join(plan_dir, network_filename)
            
            logger.info(f'Attempting to read webhook from network file: {network_path}')
            
            if os.path.exists(network_path):
                with open(network_path, 'r') as f:
                    self.webhook_url = f.read().strip()
                logger.info('Successfully loaded webhook URL from network location')
                logger.info(f'Webhook URL: {self.webhook_url}')
                return True
            else:
                logger.warning('Network webhook file does not exist')
                
        except Exception as e:
            logger.warning(f'Failed to read from network location: {str(e)}')
        
        logger.warning('Falling back to local storage')
        
        # Fall back to local storage
        try:
            resource_path = self.find_resource(f"{self.site_code}_PlanHook.txt")
            logger.info(f'Attempting to read from local path: {resource_path}')
            
            if os.path.exists(resource_path):
                with open(resource_path, 'r') as f:
                    self.webhook_url = f.read().strip()
                logger.info('Successfully loaded webhook URL from local location')
                logger.info(f'Webhook URL: {self.webhook_url}')
                return True
            else:
                logger.warning('Local webhook file does not exist')
                
        except Exception as e:
            logger.error(f'Failed to read from local location: {str(e)}')
        
        logger.warning('No webhook URL could be loaded')
        return False


    def post_plan_updates(self, plan):
        """
        Posts plan updates to the webhook URL.
        """
        logger.info('=== Starting post_plan_updates ===')
        if not self.webhook_url:
            logger.warning('No webhook URL available to post updates')
            return
        
        logger.info(f'Webhook URL: {self.webhook_url}')

        # Prepare the message
        summary = f"=== Plan Updated ===\n- Site Code: {plan['site_code']}\n- Volume: {plan['plan']['volume']}\n- Rate: {plan['plan']['rate']}\n- Hours: {plan['plan']['hours']}\n\nMessage: \n{plan['message']}\n\nSubmitted By: {plan['submitted_by']}\nTimestamp: {plan['timestamp']}"
        # Individual variables (BYO Summary)
        site_code = plan['site_code']
        volume = plan['plan']['volume']
        rate = plan['plan']['rate']
        hours = plan['plan']['hours']
        message = plan['message']
        submitted_by = plan['submitted_by']
        timestamp = plan['timestamp']


        # Prepare the payload
        payload = {
            "site": site_code,
            "volume": volume,
            "rate": rate,
            "hours": hours,
            "message": message,
            "submitted_by": submitted_by,
            "timestamp": timestamp,
            "summary": summary
        }

        # Send the POST request
        try:
            response = requests.post(self.webhook_url, json=payload)
            if response.status_code == 200:
                logger.info('Webhook notification sent successfully')
            else:
                logger.warning(f'Failed to send webhook notification: {response.status_code} - {response.text}')
        except Exception as e:
            logger.error(f'Error sending webhook notification: {str(e)}')



    def update_pick_areas(self):
        """Updates the pick areas from the SiteBuilder instance"""

        print("Updating pick areas...")        
        indicate_work = "QListWidget { background-color: #d0d0d0; }"
        self.display_area.setStyleSheet(indicate_work)
        self.update_button.setEnabled(False)
        QApplication.processEvents()

        site_builder = SiteBuilder.get_instance()
        site_builder.refresh_sites_info()
        self.pick_areas = site_builder.get_site_info()['pick_areas']
        self.update_pick_areas_display()

        indicate_done = "QListWidget { background-color: #ffffff; }"
        self.display_area.setStyleSheet(indicate_done)
        QApplication.processEvents()


    def update_pick_areas_display(self):
        """Updates the display area with pick areas from the Polars DataFrame"""
        self.display_area.clear()  # Clear the display area

        indicate_done = "QListWidget { background-color: #ffffff; }"
        self.display_area.setStyleSheet(indicate_done)
        QApplication.processEvents()

        self.site_info = SiteBuilder.get_instance().get_site_info()
        self.pick_areas = self.site_info['pick_areas']
        try:
            # Iterate through the Polars DataFrame rows
            for row in self.pick_areas.iter_rows(named=True):
                item_text = (f"{row['Name']}:\n"
                            f"Aisles: {row['Start Aisle']} - {row['End Aisle']}\n"
                            f"Slots: {row['Start Slot']} - {row['End Slot']}\n"
                            f"Cluster: {row['Cluster']}")
                list_item = qtw.QListWidgetItem(item_text)
                self.display_area.addItem(list_item)
        except KeyError as e:
            logger.error(f"Error updating display: {str(e)}")
            qtw.QMessageBox.critical(self, 'Error', 'Invalid Pick Area Format\nPick Areas Reset')


