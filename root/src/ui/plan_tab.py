import json

import time
import os
import sys
import polars as pl
import PySide6.QtGui as qtg
import PySide6.QtWidgets as qtw
from datetime import datetime as dt
from PySide6.QtCore import QTimer, Signal



# Module Path Fix
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.config.constants import USER
from src.config.site_build import SiteBuilder
from src.config.chronos import TimeManager
from src.data.processor import DataProcessor
from src.config.res_finder import ResourceFinder
find_resource = ResourceFinder().find_resource
from src.utils.logger import CustomLogger
import json
logger = CustomLogger.get_logger(__name__)
#logger.error(f"Some Error: {str(e)}")
# - automatically includes traceback and slack post
#logger.info("Some Info")




class EnhancedPlanCalculator:
    _instance = None
    _initialized = False

    def __init__(self):
        """
        Initialize with DataProcessor instance to access current metrics
        """
        self.processor = DataProcessor.get_instance()
        self.chronos = TimeManager.get_instance()
        
        self.plan_data = {
            'inputs': {
                'target_volume': 0,
                'target_rate': 0
            },

            'calculated': {
                'hours_remaining': 0, # from shift_info
                'remaining_volume': 0,
                'target_headcount': 0,
                'target_hours': 0,
            },

            'volume': {
                'picked': 0,
                'mandatory_remaining': 0,
                'flexible_remaining': 0,
                'planned': 0
            },

            'cpt_breakdown': {},
            

        }

        """
        CPT Breakdown Structure:
        {
            'CPT1': {
                'mandatory': True, # First CPT is always mandatory
                'hours_to_pick': 0,
                'process_path1': {
                    'total_cases': 0,
                    'avg_cph': 0,
                    'percent_to_pick': 1, # Manadataory CPTs are 100% picked
                    'calculated_cases': 0, # total_cases * percent_to_pick (as percentage)
                    'target_headcount': 0
                },
                'process_path2': {
                    'total_cases': 0,
                    'avg_cph': 0,
                    'percent_to_pick': 1,
                    'calculated_cases': 0,
                    'target_headcount': 0
                }
            },
            'CPT2': {
                'mandatory': False,
                'hours_to_pick': 0,
                'process_path1': {
                    'total_cases': 0,
                    'avg_cph': 0,
                    'percent_to_pick': 0.0,
                    'calculated_cases': 0,
                    'target_headcount': 0
                },
                'process_path2': {
                    'total_cases': 0,
                    'avg_cph': 0,
                    'percent_to_pick': 0.0,
                    'calculated_cases': 0,
                    'target_headcount': 0
                }
            }, # Continues for all CPTs
        }
        """
        EnhancedPlanCalculator._initialized = True



    @classmethod
    def get_instance(cls) -> 'EnhancedPlanCalculator':
        """Get singleton instance of EnhancedPlanCalculator"""
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset_instance(cls):
        """Reset the singleton instance"""
        cls._instance = None
        cls._initialized = False

    def get_plan(self):
        return self.plan_data


    def gather_givens(self):
        self.processor = DataProcessor.get_instance()
        self.chronos = TimeManager.get_instance()

        self.shift_info = self.chronos.get_shift_info()
        self.data = self.processor.get_results()

        if not self.data:
            return False

        self.hours_remaining = self.shift_info['hours_remaining'].seconds / 3600
        self.plan_data['calculated']['hours_remaining'] = self.hours_remaining

        self.picked_volume = self.data['LPI']['combined']['combined_vol']
        self.plan_data['volume']['picked'] = self.picked_volume
        
        # Get the DataFrame with required columns
        data = self.data['combined_data']['process_level'][['cpt', 'process_path', 'total_cases', 'avg_cph', 'historical_cph']]
        
        # Initial Values
        cpt_breakdown = {}
        self.mandatory_volume = 0
        self.flexible_volume = 0
        
        
        # Get unique CPTs and process paths
        unique_cpts = data['cpt'].unique().sort()
        non_hov_cpts = unique_cpts.filter(unique_cpts != 'HOV')
        print("Non-HOV CPTs:", non_hov_cpts)

        if len(non_hov_cpts) < 2:
            logger.info("Mandatory CPT completed")
            mandatory = False
        else:
            mandatory = True

        # Create the nested structure
        for cpt in unique_cpts:
            # Initialize or get existing CPT data
            cpt_breakdown[cpt] = self.plan_data.get('cpt_breakdown', {}).get(cpt, {})
            
            # Filter data for current CPT
            cpt_data = data.filter(pl.col('cpt') == cpt)

            # Always update hours_to_pick as it's a calculated value
            cpt_breakdown[cpt]['hours_to_pick'] = 0
            cpt_breakdown[cpt]['plan_picks'] = 0

            # Get unique process paths for this CPT
            unique_paths = cpt_data['process_path'].unique().sort()
            
            # Create inner dictionary for each process path
            for path in unique_paths:
                total_cases = cpt_data.filter(pl.col('process_path') == path)['total_cases'].sum()
                avg_cph = cpt_data.filter(pl.col('process_path') == path).select(
                    pl.coalesce('avg_cph', 'historical_cph')
                ).item()  # Use historical_cph if avg_cph is null     
                
                # Get existing path data if it exists
                existing_path_data = cpt_breakdown[cpt].get(path, {})
                
                if mandatory:
                    cpt_breakdown[cpt][path] = {
                        'total_cases': total_cases,
                        'avg_cph': avg_cph,
                        'percent_to_pick': 1,
                        'calculated_cases': total_cases,
                        'target_headcount': existing_path_data.get('target_headcount', 0)
                    }
                else:
                    cpt_breakdown[cpt][path] = {
                        'total_cases': total_cases,
                        'avg_cph': avg_cph,
                        'percent_to_pick': existing_path_data.get('percent_to_pick', 0.0),
                        'calculated_cases': existing_path_data.get('calculated_cases', 0),
                        'target_headcount': existing_path_data.get('target_headcount', 0)
                    }

            # Determine if the CPT is mandatory
            if mandatory:
                cpt_breakdown[cpt]['mandatory'] = True
                self.mandatory_volume = cpt_data['total_cases'].sum()
                self.plan_data['volume']['mandatory_remaining'] = self.mandatory_volume
                mandatory = False
            else:
                cpt_breakdown[cpt]['mandatory'] = False
                self.flexible_volume = cpt_data['total_cases'].sum()
                self.plan_data['volume']['flexible_remaining'] += self.flexible_volume
        
        # Add to plan_data
        self.plan_data['cpt_breakdown'] = cpt_breakdown

        return True

    def update_plan(self):
        # Gather Givens
        success = self.gather_givens()
        if not success:
            return
        
        target_volume = self.plan_data['inputs']['target_volume']
        target_rate = self.plan_data['inputs']['target_rate']
        shift_hours = self.shift_info['total_hours'].seconds / 3600

        if target_volume == 0 or target_rate == 0:
            return
        
        # Calculate Plan Picks Remaining
        remaining_volume = target_volume - self.picked_volume
        self.plan_data['calculated']['remaining_volume'] = remaining_volume
        self.plan_data['volume']['planned'] = 0
        
        # Calculate Target Hours
        self.target_hours = target_volume / target_rate
        self.plan_data['calculated']['target_hours'] = self.target_hours

        # Calculate Target Headcount
        self.target_headcount = self.target_hours / shift_hours
        self.plan_data['calculated']['target_headcount'] = self.target_headcount




        # Calculate Hours to Pick for each CPT
        # Phase 1: Calculate initial cases and picks for all CPTs
        for cpt in self.plan_data['cpt_breakdown']:
            cpt_data = self.plan_data['cpt_breakdown'][cpt]
            cpt_data['plan_picks'] = 0
            
            # Calculate cases for each path first
            for path, path_data in cpt_data.items():
                if path in ['hours_to_pick', 'mandatory', 'plan_picks']:
                    continue
                    
                total_cases = path_data['total_cases']
                percent_to_pick = path_data['percent_to_pick']
                path_data['calculated_cases'] = total_cases * percent_to_pick if percent_to_pick != 0 else 0
                
                # Sum up plan picks
                cpt_data['plan_picks'] += path_data['calculated_cases']
                
            # Override plan picks for mandatory CPTs
            if cpt_data['mandatory']:
                cpt_data['plan_picks'] = self.mandatory_volume

            self.plan_data['volume']['planned'] += cpt_data['plan_picks']


        # Phase 2: Calculate hours to pick for all CPTs
        planned_picks = self.plan_data['volume']['planned']
        for cpt in self.plan_data['cpt_breakdown']:
            cpt_data = self.plan_data['cpt_breakdown'][cpt]
            cpt_picks = cpt_data['plan_picks']
            
            if cpt_picks == 0 or planned_picks == 0:
                cpt_data['hours_to_pick'] = 0
                continue

            cpt_data['hours_to_pick'] = (cpt_picks / planned_picks) * self.hours_remaining

        # Phase 3: Calculate target headcount for each path
        for cpt in self.plan_data['cpt_breakdown']:
            cpt_data = self.plan_data['cpt_breakdown'][cpt]
            hours_to_pick = cpt_data['hours_to_pick']
            
            for path, path_data in cpt_data.items():
                if path in ['hours_to_pick', 'mandatory', 'plan_picks']:
                    continue
                    
                try:
                    percent_to_pick = path_data['percent_to_pick']
                    avg_cph = path_data['avg_cph']
                    
                    if percent_to_pick == 0 or avg_cph == 0 or hours_to_pick == 0:
                        path_data['target_headcount'] = 0
                    else:
                        path_data['target_headcount'] = (path_data['calculated_cases'] / avg_cph) / hours_to_pick
                        
                except Exception as e:
                    logger.error(f"Error calculating target HC for {cpt}-{path}: {str(e)}")
                    path_data['target_headcount'] = 0



        # Update the plan_data
        self.plan_data['calculated']['hours_remaining'] = self.hours_remaining
        self.plan_data['volume']['picked'] = self.picked_volume
        self.plan_data['volume']['mandatory_remaining'] = self.mandatory_volume
        self.plan_data['volume']['flexible_remaining'] = self.flexible_volume
        self.plan_data['calculated']['target_headcount'] = self.target_headcount
        self.plan_data['calculated']['target_hours'] = self.target_hours
        self.plan_data['calculated']['remaining_volume'] = remaining_volume

        # Return the updated plan_data
        return self.plan_data
    

class PlanTab(qtw.QWidget):

    plan_saved = Signal()
    refresh_overview = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)

        # Initialize calculator instance
        self.calculator = EnhancedPlanCalculator.get_instance()
        self.site_builder = SiteBuilder.get_instance()
        self.site_code = self.site_builder.get_site_info()['site_code']


        # Flag for loaded plan state
        self.loaded = False
        
        # Store spinbox values for each row
        self.spinbox_values = {}

        # Store widgets as instance variables
        self.volume_input = None
        self.rate_input = None
        self.planned_hours_label = None
        self.planned_hc_label = None
        self.table = None

        # Create a timer for debouncing
        self.update_timer = QTimer()
        self.update_timer.setSingleShot(True)  # Ensure timer only fires once
        self.update_timer.timeout.connect(self.delayed_update)
        
        # Store the last input values
        self.pending_updates = {}



    def display_plan_inputs(self):

        # Clear existing widgets
        for widget in self.findChildren(qtw.QWidget):
            widget.deleteLater()

        # Clear existing layout if there is one
        if self.layout() is not None:
            old_layout = self.layout()
            # Remove all widgets from the old layout
            while old_layout.count():
                item = old_layout.takeAt(0)
                if item.widget():
                    item.widget().deleteLater()
            # Remove the old layout
            qtw.QWidget().setLayout(old_layout)


        layout = qtw.QVBoxLayout(self)

        # Create horizontal layout for two columns
        columns_layout = qtw.QHBoxLayout()
        columns_layout.setSpacing(15)

        # Left Column (inputs)
        left_column = qtw.QWidget()
        left_layout = qtw.QVBoxLayout()
        left_layout.setSpacing(5)
        left_column.setLayout(left_layout)

        # Right Column (calculated values)
        right_column = qtw.QWidget()
        right_layout = qtw.QVBoxLayout()
        right_layout.setSpacing(5)
        right_column.setLayout(right_layout)


        # Add Save Button to columns layout
        save_button = qtw.QPushButton("Save Plan")
        save_button.setFixedHeight(100)
        save_button.setStyleSheet("font-size: 16px;")
        save_button.clicked.connect(self.save_plan)



        # Input section
        input_group = qtw.QGroupBox("Plan Inputs")  # Remove parent
        input_layout = qtw.QGridLayout(input_group)
        
        # Planned Volume Input
        volume_label = qtw.QLabel("Planned Volume:", input_group)
        self.volume_input = qtw.QLineEdit(input_group)
        self.volume_input.setValidator(qtg.QIntValidator())
        input_layout.addWidget(volume_label, 0, 0)
        input_layout.addWidget(self.volume_input, 0, 1)
        
        # Planned Rate Input
        rate_label = qtw.QLabel("Planned Rate:", input_group)
        self.rate_input = qtw.QLineEdit(input_group)
        self.rate_input.setValidator(qtg.QDoubleValidator())
        input_layout.addWidget(rate_label, 1, 0)
        input_layout.addWidget(self.rate_input, 1, 1)
        
        # Calculated values section
        calc_group = qtw.QGroupBox("Calculated Values")
        calc_layout = qtw.QGridLayout(calc_group)
        
        self.planned_hours_label = qtw.QLabel("Planned Hours: 0.0", calc_group)
        self.planned_hc_label = qtw.QLabel("Planned HC: 0.0", calc_group)
        
        calc_layout.addWidget(self.planned_hours_label, 0, 0)
        calc_layout.addWidget(self.planned_hc_label, 1, 0)

        # Add group boxes to their respective columns
        left_layout.addWidget(input_group)
        right_layout.addWidget(calc_group)

        # Add columns to the horizontal layout
        columns_layout.addWidget(left_column)
        columns_layout.addWidget(right_column)
        columns_layout.addWidget(save_button)
        
        # Add the columns layout to the main layout
        layout.addLayout(columns_layout)
        
        # Table
        self.table = qtw.QTableWidget(self)
        self.setup_table()
        
        # Set stretch factors for table
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.verticalHeader().setVisible(False)
        
        # Add table to main layout
        layout.addWidget(self.table)
        
        # Connect signals
        self._connect_signals()


    def _connect_signals(self):
        """Connect all signals to their slots"""
        if self.volume_input:
            self.volume_input.textChanged.connect(
                lambda text: self.queue_update('volume', text))
        if self.rate_input:
            self.rate_input.textChanged.connect(
                lambda text: self.queue_update('rate', text))
            
    def queue_update(self, input_type, value):
        """Queue an update with debouncing"""
        self.pending_updates[input_type] = value
        # Reset and start the timer (700ms delay)
        self.update_timer.stop()
        self.update_timer.start(700)

    def delayed_update(self, edit=True):
        """Process the delayed update"""
        self.loaded = True if self.site_builder._plan_data is not None else False
        if edit == False:
            if self.loaded:
                self.calculator.plan_data = self.site_builder._plan_data
                self.calculator.update_plan()
                self.display_read_only_plan()
                return
            else:
                self.display_plan_inputs()
        try:
            # Process volume update
            if 'volume' in self.pending_updates:
                volume_text = self.pending_updates['volume']
                volume = int(volume_text) if volume_text else 0
                self.calculator.plan_data['inputs']['target_volume'] = volume

            # Process rate update
            if 'rate' in self.pending_updates:
                rate_text = self.pending_updates['rate']
                rate = float(rate_text) if rate_text else 0.0
                self.calculator.plan_data['inputs']['target_rate'] = rate

            # Clear pending updates
            self.pending_updates.clear()
            
            # Trigger recalculation
            updated_data = self.calculator.update_plan()
            
            # Update UI
            self.update_calculated_values(updated_data)
            self.refresh_table()

        except Exception as e:
            logger.error(f"Error in delayed update: {str(e)}")


    def setup_table(self):
        """Initialize the table structure"""
        if not self.table:
            return

        headers = ["CPT", "Process Path", "Total Picks", "% To Pick", 
                  "Plan Picks", "Rate", "Hours to Complete", "Target HC"]
        
        self.table.setColumnCount(len(headers))
        self.table.setHorizontalHeaderLabels(headers)
        
        # Set column widths and properties
        self.table.horizontalHeader().setSectionResizeMode(
            qtw.QHeaderView.ResizeToContents)
        

    def refresh_table(self):
        """Refresh the table with current data"""
        if not self.table:
            return

        # Clear the table
        self.table.setRowCount(0)

        try:
            plan_data = self.calculator.plan_data
            row_count = 0
            
            # Count total rows needed
            for cpt in plan_data['cpt_breakdown']:
                for path in plan_data['cpt_breakdown'][cpt]:
                    if isinstance(plan_data['cpt_breakdown'][cpt][path], dict) \
                            and 'total_cases' in plan_data['cpt_breakdown'][cpt][path]:
                        row_count += 1
            
            self.table.setRowCount(row_count)
            current_row = 0
            
            # Populate table
            for cpt in plan_data['cpt_breakdown']:
                for path in plan_data['cpt_breakdown'][cpt]:
                    if isinstance(plan_data['cpt_breakdown'][cpt][path], dict) \
                            and 'total_cases' in plan_data['cpt_breakdown'][cpt][path]:
                        self.populate_table_row(current_row, cpt, path, plan_data)
                        current_row += 1

            plan_picks_sum = 0.0
            target_hc_sum = 0.0
            normal_font = qtg.QFont()
            bold_font = qtg.QFont()
            bold_font.setBold(True)
            
            # Get the target values for comparison
            target_volume = self.calculator.plan_data['inputs']['target_volume'] - self.calculator.plan_data['volume']['picked']
            target_headcount = self.calculator.plan_data['calculated']['target_headcount']

            # Sum up the columns
            for row in range(self.table.rowCount()):
                # Plan Picks column (index 3)
                plan_picks_item = self.table.item(row, 4)
                if plan_picks_item and plan_picks_item.text():
                    try:
                        plan_picks_sum += float(plan_picks_item.text())
                    except ValueError:
                        continue

                # Target HC column (index 6)
                target_hc_item = self.table.item(row, 7)
                if target_hc_item and target_hc_item.text():
                    try:
                        target_hc_sum += float(target_hc_item.text())
                    except ValueError:
                        continue

            # Check and color Plan Picks column
            for row in range(self.table.rowCount()):
                plan_picks_item = self.table.item(row, 4)
                if plan_picks_item:
                    if plan_picks_sum > target_volume:
                        plan_picks_item.setForeground(qtg.QColor('red'))
                        plan_picks_item.setFont(bold_font)
                    else:
                        plan_picks_item.setForeground(qtg.QColor('black'))
                        plan_picks_item.setFont(normal_font)

                # Check and color Target HC column
                target_hc_item = self.table.item(row, 7)
                if target_hc_item:
                    if target_hc_sum > target_headcount:
                        target_hc_item.setForeground(qtg.QColor('red'))
                        target_hc_item.setFont(bold_font)
                    else:
                        target_hc_item.setForeground(qtg.QColor('black'))
                        target_hc_item.setFont(normal_font)

            logger.info(f"Table refreshed successfully. Plan Picks Sum: {plan_picks_sum:.2f}, "
                    f"Target HC Sum: {target_hc_sum:.2f}")

        except Exception as e:
            logger.error(f"Error refreshing table: {str(e)}")

    def populate_table_row(self, row, cpt, path, plan_data):
        """Populate a single table row"""
        try:
            path_data = plan_data['cpt_breakdown'][cpt][path]
            if path_data['avg_cph'] == None:
                path_data['avg_cph'] = 1

            # Create unique key for this row
            row_key = f"{cpt}-{path}"
            
            # Create items for each column
            cpt_item = qtw.QTableWidgetItem(cpt)
            path_item = qtw.QTableWidgetItem(path)
            total_cases_item = qtw.QTableWidgetItem(str(path_data['total_cases']))
            plan_picks_item = qtw.QTableWidgetItem(str(round(path_data['calculated_cases'],2)))
            rate_item = qtw.QTableWidgetItem(str(round(path_data['avg_cph'],2)))
            hours_item = qtw.QTableWidgetItem(f"{plan_data['cpt_breakdown'][cpt]['hours_to_pick']:.2f}")
            target_hc_item = qtw.QTableWidgetItem(f"{path_data['target_headcount']:.2f}")

            # Set background color for mandatory CPTs
            if plan_data['cpt_breakdown'][cpt]['mandatory']:
                background_color = qtg.QColor(240, 220, 235)  # Light grey
                for item in [cpt_item, path_item, total_cases_item, plan_picks_item, 
                            rate_item, hours_item, target_hc_item]:
                    item.setBackground(background_color)

            # Set items in table
            self.table.setItem(row, 0, cpt_item)
            self.table.setItem(row, 1, path_item)
            self.table.setItem(row, 2, total_cases_item)
            
            # Create and set up spinbox
            spinbox = qtw.QSpinBox(self.table)
            spinbox.setRange(0, 100)
            
            # Set the value from stored state or from data
            if row_key in self.spinbox_values:
                value = self.spinbox_values[row_key]
            else:
                value = int(path_data['percent_to_pick'] * 100)
                self.spinbox_values[row_key] = value
            
            spinbox.setValue(value)
            
            # Connect spinbox signal with row key
            spinbox.valueChanged.connect(
                lambda value, key=row_key: self.update_row_calculations(key, value))
            
            self.table.setCellWidget(row, 3, spinbox)
            
            # Set remaining cells
            self.table.setItem(row, 4, plan_picks_item)
            self.table.setItem(row, 5, rate_item)
            self.table.setItem(row, 6, hours_item)
            self.table.setItem(row, 7, target_hc_item)

            # Check hours value and set background color if less than 1
            if plan_data['cpt_breakdown'][cpt]['hours_to_pick'] < 1:
                hours_item.setBackground(qtg.QColor(255, 200, 200))  # Light red background

        except Exception as e:
            logger.error(f"Error populating table row: {str(e)}")




    def update_calculated_values(self, updated_data):
        """Update the calculated values display"""
        if updated_data is None or 'calculated' not in updated_data:
            logger.warning("No calculated data available to update.")
            return
        if 'target_hours' not in updated_data['calculated'] \
            or 'target_headcount' not in updated_data['calculated']:
            logger.warning("No calculated data available to update.")
            return

        target_hours = updated_data['calculated']['target_hours']
        target_hc = updated_data['calculated']['target_headcount']
        
        self.planned_hours_label.setText(f"Planned Hours: {target_hours:.2f}")
        self.planned_hc_label.setText(f"Planned HC: {target_hc:.2f}")


    def update_row_calculations(self, row_key, value):
        """Update calculations when spinbox values change"""
        try:
            # Store the new value
            self.spinbox_values[row_key] = value
            
            # Parse the CPT and path from the row key
            # Split from the right side to handle CPTs containing hyphens
            parts = row_key.rsplit('-', 1)
            if len(parts) != 2:
                logger.error(f"Invalid row key format: {row_key}")
                return
                
            cpt, path = parts[0], parts[1]
            
            # Update the calculator with the new percentage
            percent = value / 100.0  # Convert percentage to decimal
            self.calculator.plan_data['cpt_breakdown'][cpt][path]['percent_to_pick'] = percent
            
            # Recalculate the cases for this path
            path_data = self.calculator.plan_data['cpt_breakdown'][cpt][path]
            path_data['calculated_cases'] = path_data['total_cases'] * percent
            
            # Recalculate target headcount for this path
            hours_to_pick = self.calculator.plan_data['cpt_breakdown'][cpt]['hours_to_pick']
            avg_cph = path_data['avg_cph']
            
            if hours_to_pick > 0 and avg_cph > 0:
                path_data['target_headcount'] = (path_data['calculated_cases'] / avg_cph) / hours_to_pick
            else:
                path_data['target_headcount'] = 0


            self.spinbox_values[row_key] = value
            self.pending_updates['spinbox'] = (row_key, value)
            self.update_timer.stop()
            self.update_timer.start(700)

            # Update the full plan
            #self.calculator.update_plan()
            
            # Refresh the table while maintaining spinbox values
            #self.refresh_table()

        except Exception as e:
            logger.error(f"Error updating row calculations: {str(e)}\nRow key: {row_key}, Value: {value}\nCurrent path_data: {path_data if 'path_data' in locals() else 'Not available'}")



    def save_plan(self):
        """Save the current plan"""
        try:
            # Get the current plan data
            plan_data = self.calculator.plan_data

            # Convert the plan data to a JSON string
            plan_json = json.dumps(plan_data)

            
            logger.info('Plan data constructed successfully')
            logger.debug(f'Full plan data: {json.dumps(plan_data, indent=2)}')

            # Try network path first
            try:
                #\\ant\dept-na\SAV7\Public\PickAssist\Plans\SITE
                base_path = r"\\ant\dept-na\SAV7\Public"
                pickassist_path = os.path.join(base_path, "PickAssist")
                plan_dir = os.path.join(pickassist_path, "Plans", self.site_code)
                
                logger.info(f'Attempting to save to network path: {plan_dir}')

                # Create directories if they don't exist
                if not os.path.exists(plan_dir):
                    logger.info(f'Creating directory structure: {plan_dir}')
                    os.makedirs(plan_dir, exist_ok=True)

                # Generate network filename with timestamp
                network_filename = f"{self.site_code}_{dt.now().strftime('%Y%m%d_%H%M')}.json"
                network_path = os.path.join(plan_dir, network_filename)
                
                logger.info(f'Attempting to write to network file: {network_path}')
                with open(network_path, 'w') as f:
                    json.dump(plan_data, f, indent=4)
                
                
                timeout = 10  # seconds
                start_time = time.time()
                while not os.path.exists(network_path):
                    logger.info(f'Waiting for file to appear at: {network_path}')
                    if time.time() - start_time > timeout:
                        logger.error(f'Timeout waiting for file to appear at: {network_path}')
                        raise TimeoutError(f'File not found after {timeout} seconds: {network_path}')
                    time.sleep(0.5)  # Short sleep to prevent excessive CPU usage

                logger.info(f'Successfully saved to {network_path}')

                self.site_builder = SiteBuilder.get_instance(new=True)
                self.site_builder.get_site_info(new=True)
                
                self.plan_saved.emit()
                self.refresh_overview.emit()
                self.delayed_update(edit=False)
                #self.post_plan_updates(plan_data)

            except Exception as e:
                logger.warning(f'Failed to save to network location: {str(e)}')
                  
            except Exception as e:
                logger.error(f'Failed to save simplified version: {str(e)}')

            logger.info('Plan file save operation completed successfully')
            return plan_data

        except Exception as e:
            logger.error = f"Error saving plan file: {str(e)}"
            return None

        finally:
            logger.info('=== Completed save_plan_file ===\n') 



    def display_read_only_plan(self):
        """Display the plan data in a read-only table"""

        # TODO CLEAR ALL WIDGETS AND RENAME SETUP UI
        plan_data = self.calculator.plan_data
        row_count = 0



        # Clear existing widgets
        for widget in self.findChildren(qtw.QWidget):
            widget.deleteLater()

        # Clear existing layout if there is one
        if self.layout() is not None:
            old_layout = self.layout()
            # Remove all widgets from the old layout
            while old_layout.count():
                item = old_layout.takeAt(0)
                if item.widget():
                    item.widget().deleteLater()
            # Remove the old layout
            qtw.QWidget().setLayout(old_layout)


        layout = qtw.QVBoxLayout(self)

        # Create horizontal layout for two columns
        columns_layout = qtw.QHBoxLayout()
        columns_layout.setSpacing(15)

        # Left Column (inputs)
        left_column = qtw.QWidget()
        left_layout = qtw.QVBoxLayout()
        left_layout.setSpacing(5)
        left_column.setLayout(left_layout)

        # Right Column (calculated values)
        right_column = qtw.QWidget()
        right_layout = qtw.QVBoxLayout()
        right_layout.setSpacing(5)
        right_column.setLayout(right_layout)


        # Add Save Button to columns layout
        edit_button = qtw.QPushButton("Edit Plan")
        edit_button.setFixedHeight(100)
        edit_button.setStyleSheet("font-size: 16px;")
        edit_button.clicked.connect(self.confirm_overwrite)



        # Input section
        input_group = qtw.QGroupBox("Plan Inputs")  # Remove parent
        input_layout = qtw.QGridLayout(input_group)
        
        # Planned Volume Input
        volume_label = qtw.QLabel(f"Planned Volume: {plan_data['inputs']['target_volume']}", input_group)
        input_layout.addWidget(volume_label, 0, 0)
        
        # Planned Rate Input
        rate_label = qtw.QLabel(f"Planned Rate: {plan_data['inputs']['target_rate']}", input_group)
        input_layout.addWidget(rate_label, 1, 0)

        
        # Calculated values section
        calc_group = qtw.QGroupBox("Calculated Values")
        calc_layout = qtw.QGridLayout(calc_group)
        
        self.planned_hours_label = qtw.QLabel(f"Planned Hours: {plan_data['calculated']['target_hours']:.2f}", calc_group)
        self.planned_hc_label = qtw.QLabel(f"Planned HC: {plan_data['calculated']['target_headcount']:.2f}", calc_group)
        
        calc_layout.addWidget(self.planned_hours_label, 0, 0)
        calc_layout.addWidget(self.planned_hc_label, 1, 0)

        # Add group boxes to their respective columns
        left_layout.addWidget(input_group)
        right_layout.addWidget(calc_group)

        # Add columns to the horizontal layout
        columns_layout.addWidget(left_column)
        columns_layout.addWidget(right_column)
        columns_layout.addWidget(edit_button)
        
        # Add the columns layout to the main layout
        layout.addLayout(columns_layout)
        
        # Table
        self.table = qtw.QTableWidget(self)
        self.setup_table()
        
        # Set stretch factors for table
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.verticalHeader().setVisible(False)
        
        # Add table to main layout
        layout.addWidget(self.table)



        def populate_read_only_row(row, cpt, path, plan_data):
            """Populate a single read-only table row"""
            try:
                path_data = plan_data['cpt_breakdown'][cpt][path]

                # Create items for each column
                cpt_item = qtw.QTableWidgetItem(cpt)
                path_item = qtw.QTableWidgetItem(path)
                total_cases_item = qtw.QTableWidgetItem(str(path_data['total_cases']))
                percent_item = qtw.QTableWidgetItem(f"{path_data['percent_to_pick'] * 100}%")
                plan_picks_item = qtw.QTableWidgetItem(str(round(path_data['calculated_cases'], 2)))
                rate_item = qtw.QTableWidgetItem(str(round(path_data['avg_cph'], 2)))
                hours_item = qtw.QTableWidgetItem(f"{plan_data['cpt_breakdown'][cpt]['hours_to_pick']:.2f}")
                target_hc_item = qtw.QTableWidgetItem(f"{path_data['target_headcount']:.2f}")

                # Set background color for mandatory CPTs
                if plan_data['cpt_breakdown'][cpt]['mandatory']:
                    background_color = qtg.QColor(240, 220, 235)  # Light grey
                    for item in [cpt_item, path_item, total_cases_item, percent_item,
                                plan_picks_item, rate_item, hours_item, target_hc_item]:
                        item.setBackground(background_color)

                # Set items in table
                self.table.setItem(row, 0, cpt_item)
                self.table.setItem(row, 1, path_item)
                self.table.setItem(row, 2, total_cases_item)
                self.table.setItem(row, 3, percent_item)
                self.table.setItem(row, 4, plan_picks_item)
                self.table.setItem(row, 5, rate_item)
                self.table.setItem(row, 6, hours_item)
                self.table.setItem(row, 7, target_hc_item)

            except Exception as e:
                print(f"Error populating read-only table row: {str(e)}")
                logger.error(f"Error populating read-only table row: {str(e)}")


        if not self.table:
            return

        try:

            # Count total rows needed
            for cpt in plan_data['cpt_breakdown']:
                for path in plan_data['cpt_breakdown'][cpt]:
                    if isinstance(plan_data['cpt_breakdown'][cpt][path], dict) \
                            and 'total_cases' in plan_data['cpt_breakdown'][cpt][path]:
                        row_count += 1

            self.table.setRowCount(row_count)
            current_row = 0

            # Populate table
            for cpt in sorted(plan_data['cpt_breakdown'].keys()):  # Sort the CPTs
                paths = [path for path in plan_data['cpt_breakdown'][cpt].keys() 
                        if path not in ['hours_to_pick', 'mandatory', 'plan_picks']
                        and isinstance(plan_data['cpt_breakdown'][cpt][path], dict)
                        and 'total_cases' in plan_data['cpt_breakdown'][cpt][path]]
                
                for path in sorted(paths):  # Sort the filtered paths
                    populate_read_only_row(current_row, cpt, path, plan_data)
                    current_row += 1

        except Exception as e:
            print(f"Error displaying read-only plan: {str(e)}")
            logger.error(f"Error refreshing table: {str(e)}")


    def confirm_overwrite(self):
        """
        Shows a confirmation dialog before allowing plan overwrite.
        """
        reply = qtw.QMessageBox.question(
            self,
            'Confirm Overwrite',
            'Are you sure you want to overwrite the current plan?',
            qtw.QMessageBox.Yes | qtw.QMessageBox.No,
            qtw.QMessageBox.No
        )

        if reply == qtw.QMessageBox.Yes:
            network_path = f"\\\\ant\\dept-na\\SAV7\\Public\\PickAssist\\Plans\\{self.site_code}"
            recent_network_files_exist = False
            network_error = None
            
            try:
                if os.path.exists(network_path):
                    current_time = dt.now()
                    try:
                        for file in os.listdir(network_path):
                            file_path = os.path.join(network_path, file)
                            if file.endswith('.json'):
                                try:
                                    file_time = dt.fromtimestamp(os.path.getmtime(file_path))
                                    time_difference = current_time - file_time
                                    
                                    if time_difference.total_seconds() < 3600:
                                        recent_network_files_exist = True
                                        message = f"Recent plan files (less than 1 hour old) exist in the network directory!"
                                        break
                                except OSError as e:
                                    logger.warning(f"Could not get modification time for file {file}: {str(e)}")

                                    continue
                    except OSError as e:
                        network_error = f"Could not read directory contents: {str(e)}"
                        logger.error(network_error)

            except Exception as e:
                network_error = f"Could not access network path: {str(e)}"
                logger.error(network_error)


            
            if recent_network_files_exist:
                reply = qtw.QMessageBox.warning(
                self,
                'Confirm Overwrite',
                'Plan was updated recently (less than 1 hour ago).\nAre you sure?',
                qtw.QMessageBox.Yes | qtw.QMessageBox.No,
                qtw.QMessageBox.No
            )
                if reply == qtw.QMessageBox.Yes:
                    self.display_plan_inputs()
                    logger.info(f"Overwrite confirmed by {USER}.")
            elif network_error:
                logger.info("Overwrite could not check network directory for recent files.")
                logger.warning(f"Error:\n{network_error}")
                # Network file not avaiable, proceed with overwrite
                self.display_plan_inputs()

            else:
                # Network file not outdated, proceed with overwrite
                self.display_plan_inputs()