import os
import sys
import PySide6.QtCore as qtc
import PySide6.QtGui as qtg
import PySide6.QtWidgets as qtw

# Module Path Fix
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.config.chronos import TimeManager
from src.config.constants import TZ_MAPPING
from src.utils.logger import CustomLogger
from src.config.res_finder import ResourceFinder
logger = CustomLogger.get_logger(__name__)
#logger.error(f"Some Error: {str(e)}")
# - automatically includes traceback and slack post
#logger.info("Some Info")


class InputDialog:
    def __init__(self):
        self.time_manager = TimeManager.get_instance()
        self.dialog_result = None

    def show_input_dialog(self):
        """Display dialog for collecting site code and shift times"""
        # Create the dialog window
        self.dialog = qtw.QDialog()
        dialog = self.dialog
        dialog.setWindowTitle("Input Required")
        dialog.setModal(True)
        dialog.setWindowFlags(dialog.windowFlags() | qtc.Qt.WindowStaysOnTopHint)
        dialog.setWindowIcon(qtg.QIcon(ResourceFinder.find_resource("ballac.png")))

        # Create layout
        layout = qtw.QVBoxLayout(dialog)
        form_layout = qtw.QFormLayout()
        
        # Site Code input
        self.site_code_input = qtw.QLineEdit()
        form_layout.addRow("Site Code:", self.site_code_input)

        # Shift times input
        self.start_hour_input = qtw.QSpinBox()
        self.start_hour_input.setRange(0, 23)
        form_layout.addRow("Start Hour (0-24):", self.start_hour_input)

        self.end_hour_input = qtw.QSpinBox() 
        self.end_hour_input.setRange(0, 23)
        form_layout.addRow("End Hour (0-24):", self.end_hour_input)

        # Add form layout
        layout.addLayout(form_layout)

        # Error label
        self.error_label = qtw.QLabel()
        self.error_label.setStyleSheet("color: red")
        layout.addWidget(self.error_label)

        # Submit button
        submit_btn = qtw.QPushButton("Submit")
        submit_btn.clicked.connect(self.validate_and_submit)
        layout.addWidget(submit_btn)

        dialog.closeEvent = self.handle_close
        self.dialog_result = dialog.exec_()

        # Check if dialog was cancelled
        if not self.dialog_result:
            logger.info("Dialog closed without input - terminating application")
            sys.exit()

    def handle_close(self, event):
        """Handle dialog close event"""
        logger.info("Dialog closed - terminating application")
        sys.exit()

    def validate_and_submit(self):
        """Validate inputs and save if valid"""
        site_code = self.site_code_input.text().upper()
        start_hour = self.start_hour_input.value()
        end_hour = self.end_hour_input.value()

        if self.validate_input(site_code, start_hour, None, end_hour, None):
            success = self.time_manager.setup_shift(
                site_code=site_code,
                start_hour=start_hour, 
                end_hour=end_hour
            )
            if success:
                self.site_code = site_code
                self.start_hour = start_hour
                self.end_hour = end_hour
                self.dialog.accept()
            else:
                self.error_label.setText("Failed to setup shift. Please try again.")
        else:
            self.error_label.setText("Invalid input. Please check your entries.")

    def validate_input(self, input_value, start_hour, start_am_pm, end_hour, end_am_pm):
        """Validate the input values"""
        #pattern = r'^[a-zA-Z]{3}\d$'  <-- Doesn't Accept 'X' for some reason.
        site_code_valid = input_value in TZ_MAPPING
        valid_hours = (0 <= start_hour <= 24) and (0 <= end_hour <= 24)
        return site_code_valid and valid_hours

    async def initial_run(self):
        """run on startup"""
        pass


class ShiftTimeDialog:
    def __init__(self):
        self.time_manager = TimeManager.get_instance()
        self.dialog_result = None
        self.start_hour = None
        self.end_hour = None

    def show_input_dialog(self):
        """Display dialog for updating shift times"""
        # Create the dialog window
        self.dialog = qtw.QDialog()
        dialog = self.dialog
        dialog.setWindowTitle("Update Shift Times")
        dialog.setModal(True)
        dialog.setWindowFlags(dialog.windowFlags() | qtc.Qt.WindowStaysOnTopHint)
        dialog.setWindowIcon(qtg.QIcon(ResourceFinder.find_resource("ballac.png")))


        # Set the minimum width
        dialog.setMinimumWidth(200)
        # Create layout
        layout = qtw.QVBoxLayout(dialog)
        form_layout = qtw.QFormLayout()

        # Shift times input
        self.start_hour_input = qtw.QSpinBox()
        self.start_hour_input.setRange(0, 23)
        self.start_hour_input.setValue(self.time_manager._shift_time.start.hour)  # Set current value
        form_layout.addRow("Start Hour (0-23):", self.start_hour_input)

        self.end_hour_input = qtw.QSpinBox()
        self.end_hour_input.setRange(0, 23)
        self.end_hour_input.setValue(self.time_manager._shift_time.end.hour)  # Set current value
        form_layout.addRow("End Hour (0-23):", self.end_hour_input)

        # Add form layout
        layout.addLayout(form_layout)

        # Error label
        self.error_label = qtw.QLabel()
        self.error_label.setStyleSheet("color: red")
        layout.addWidget(self.error_label)

        # Submit button
        submit_btn = qtw.QPushButton("Update Shift Times")
        submit_btn.clicked.connect(self.validate_and_submit)
        layout.addWidget(submit_btn)

        dialog.closeEvent = self.handle_close
        self.dialog_result = dialog.exec_()

    def handle_close(self, event):
        """Handle dialog close event"""
        logger.info("Shift time update dialog closed - continuing with existing values")
        self.dialog_result = False
        event.accept()

    def validate_and_submit(self):
        """Validate inputs and save if valid"""
        start_hour = self.start_hour_input.value()
        end_hour = self.end_hour_input.value()

        if self.validate_input(start_hour, end_hour):
            self.start_hour = start_hour
            self.end_hour = end_hour
            self.dialog.accept()
        else:
            self.error_label.setText("Invalid input. Hours must be between 0 and 23.")

    def validate_input(self, start_hour, end_hour):
        """Validate the input values"""
        return (0 <= start_hour <= 23) and (0 <= end_hour <= 23)
