import os
import sys
import asyncio
import webbrowser
from PIL import Image
import PySide6.QtGui as qtg
import PySide6.QtCore as qtc
import PySide6.QtWidgets as qtw
from PySide6.QtCore import QTimer, Signal, QThread, QDateTime

# Module Path Fix
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.config.constants import USER, THIS_VERSION, SITES
from src.config.versioning import VersionHandler
from src.ui.input_dialog import InputDialog
from src.ui.tabs import OverviewTab, DetailsTab, PathsTab, PlanTab, SettingsTab
from src.config.res_finder import ResourceFinder
from src.data.processor import DataProcessor
from src.config.chronos import TimeManager
from src.config.site_build import SiteBuilder

from src.utils.logger import CustomLogger

logger = CustomLogger.get_logger(__name__)
#logger.error(f"Some Error: {str(e)}")
# - automatically includes traceback and slack post
#logger.info("Some Info")


class DataProcessingThread(QThread):
    finished = Signal(dict)
    error = Signal(str)

    def run(self):
        """Execute data processing in separate thread"""
        try:
            # Create event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Run async processing
            processor = DataProcessor.get_instance()
            try:
                loop.run_until_complete(processor.process_incoming_data())
                results = processor.get_results()
                self.finished.emit(results)
            finally:
                loop.close()
        except Exception as e:
            self.error.emit(str(e))

class SDCPickAssistApp(qtw.QMainWindow):
    def __init__(self):
        super().__init__()

        self.version = THIS_VERSION
        self.alias = USER
        try:
            self.outdated_version, self.latest_version = VersionHandler().check_version()
        except Exception as e:
            logger.error(f"Error checking version: {e}")
            self.outdated_version = False
            self.latest_version = THIS_VERSION

        self.time_manager = TimeManager.get_instance()
        self.processing_thread = None


        # Create and show dialog
        dialog = InputDialog()
        dialog.show_input_dialog()


        self.shift_info = TimeManager.get_instance().get_shift_info()
        self.site_info = SiteBuilder.get_instance().get_site_info()

        self.site_code = self.shift_info['site_code']
        self.timezone = self.shift_info['timezone']
        self.shift_start = self.shift_info['shift_start'].hour
        self.shift_end = self.shift_info['shift_end'].hour
        self.pick_areas = self.site_info['pick_areas']

        
        self.last_update = "Never"
        self.formatted_hours_left_in_shift = self.shift_info['formatted_time_remaining']

        # Application Variables
        self.auto_refresh_interval = 600  # 10 minutes in seconds
        self.auto_refresh_timer = None  # Timer instance variable
        self.is_auto_refreshing = False # Refresh Flag
        self.is_closing = False # Closing Flag
        self.retry_count = 0 # Iterative Recursion Barrier
        self.is_initial_startup = True # Initial Startup Flag
        self.overview_header_labels = {}


        # Initialize UI
        self.initialize_main_window()

        # Connect PlanCalculator's refresh signal to OverviewTab's gather method
        self.tab_plan.calculator.refresh_overview.connect(
            self.tab_overview.gather_overview
        )


    def run_it(self):
        """Non-blocking main data processing method"""
        self.go_button.setEnabled(False)
        self.go_button.setText("Processing...")
        self.go_button.setStyleSheet("""
            QPushButton {
                    background-color: #2F4F4F;
                    color: white;  /* White text color */
                    padding: 8px 16px;  /* Reduced padding */
                    border: none;  /* Remove button border */
                    border-radius: 4px;  /* Add rounded corners */
                    font-size: 16px;  /* Increase font size */
                    margin-top: 2px;  /* Reduced top margin */
                    margin-bottom: 2px;  /* Reduced bottom margin */
            }
        """)
        #self.async_handler.start_processing()

        # Create new thread instance
        self.processing_thread = DataProcessingThread()
        self.processing_thread.finished.connect(self.on_processing_complete)
        self.processing_thread.error.connect(self.on_processing_error)
        
        # Start processing in background
        self.processing_thread.start()

    def on_processing_complete(self, results):
        """Handle completed processing"""
        self.results = results
        logger.info("Data processing completed successfully")
        self.go_button.setEnabled(True)
        self.go_button.setText("Refresh")
        self.go_button.setStyleSheet("""
            QPushButton {
                    background-color: #4CAF50;
                    color: white;  /* White text color */
                    padding: 8px 16px;  /* Reduced padding */
                    border: none;  /* Remove button border */
                    border-radius: 4px;  /* Add rounded corners */
                    font-size: 16px;  /* Increase font size */
                    margin-top: 2px;  /* Reduced top margin */
                    margin-bottom: 2px;  /* Reduced bottom margin */
            }
        """)
        self.last_update = QDateTime.currentDateTime().toString('yyyy-MM-dd hh:mm:ss')
        self.last_update_label.setText(f"Last Update: {self.last_update}")
        self.tab_overview.gather_overview()
        self.tab_details.gather_details()
        self.tab_paths.gather_paths()
        self.tab_plan.gather_plan(update=True)
        
        # Clean up thread
        if self.processing_thread:
            self.processing_thread.quit()
            self.processing_thread.wait()
            self.processing_thread = None

    def on_processing_error(self, error_msg):
        """Handle processing errors"""
        logger.error(f"Error in run_it: {error_msg}")
        self.go_button.setEnabled(True)
        self.go_button.setText("Refresh")
        self.go_button.setStyleSheet("""
            QPushButton {
                    background-color: #4CAF50;
                    color: white;  /* White text color */
                    padding: 8px 16px;  /* Reduced padding */
                    border: none;  /* Remove button border */
                    border-radius: 4px;  /* Add rounded corners */
                    font-size: 16px;  /* Increase font size */
                    margin-top: 2px;  /* Reduced top margin */
                    margin-bottom: 2px;  /* Reduced bottom margin */
            }
        """)
        self.show_timed_warning(f"Error during refresh: {error_msg}")


    def initialize_main_window(self):
        """
        Initializes the main window of the application, setting up its size, position,
        and various UI components such as frames, tabs, and scrollable areas.
        """
        logger.info('Site: %s', self.site_code)
        
        if self.site_code is None:
            sys.exit()

        # Configure window
        self.setWindowTitle("SDC Pick Assist")
        self.setWindowIcon(qtg.QIcon(ResourceFinder.find_resource("ballac.png")))
        
        # Get screen dimensions using QScreen
        screen = qtw.QApplication.primaryScreen()
        screen_geometry = screen.geometry()
        screen_width = screen_geometry.width()
        screen_height = screen_geometry.height()
        
        # Calculate window size (40% of screen size)
        window_width = int(screen_width * 0.6)
        window_height = int(screen_height * 0.9)
        
        # Calculate position for center of the screen
        x = (screen_width - window_width) // 2
        y = (screen_height - window_height) // 2  # Center vertically
        
        # Set window size and position
        self.setGeometry(x, y, window_width, window_height)
        
        # Create a scrollable area using QScrollArea
        # Central widget
        self.scroll_area = qtw.QScrollArea(self)
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setVerticalScrollBarPolicy(qtc.Qt.ScrollBarAsNeeded)

        # Create a container widget for the scroll area
        self.scrollable_frame = qtw.QWidget()
        self.scrollable_layout = qtw.QVBoxLayout(self.scrollable_frame)

        # Remove any default margins and spacing
        self.scrollable_layout.setContentsMargins(5, 5, 5, 5)
        self.scrollable_layout.setSpacing(2)

        # Add the scrollable frame to the scroll area
        self.scroll_area.setWidget(self.scrollable_frame)
        self.setCentralWidget(self.scroll_area)

        # Create main frames
        self.frame0 = qtw.QFrame(self.scrollable_frame)
        self.frame0.setStyleSheet("border-radius: 10px;")
        self.scrollable_layout.addWidget(self.frame0)
        
        # Add shift progress bar
        self.shift_progress = qtw.QProgressBar(self.scrollable_frame)
        self.shift_progress.setValue(0)
        self.scrollable_layout.addWidget(self.shift_progress)

        self.update_shift_progress_bar()

        # Create tab widget
        self.tabview = qtw.QTabWidget(self.scrollable_frame)
        self.scrollable_layout.addWidget(self.tabview)

        # Set stretch factors for the frames and tabview
        self.scrollable_layout.setStretchFactor(self.frame0, 0)  # Minimum space
        self.scrollable_layout.setStretchFactor(self.shift_progress, 0)  # Minimum space
        self.scrollable_layout.setStretchFactor(self.tabview, 1)  # Take all remaining space

        # Create main tabs within the tab widget

        # OVERVIEW
        self.tab_overview = OverviewTab(self)
        self.tabview.addTab(self.tab_overview, "Overview")
        self.tab_overview.setLayout(qtw.QGridLayout())


        # DETAILS
        self.tab_details = DetailsTab(self)
        self.tabview.addTab(self.tab_details, "Details")
        self.tab_details.setLayout(qtw.QGridLayout())
        


        self.tab_paths = PathsTab(self)
        self.tabview.addTab(self.tab_paths, "Paths")
        self.tab_paths.setLayout(qtw.QGridLayout())

        # Initialize tab_paths_tabview
        self.tab_paths_tabview = qtw.QTabWidget(self.tab_paths)
        self.tab_paths.layout().addWidget(self.tab_paths_tabview)


        # Plan Tab
        self.tab_plan = PlanTab(self)
        self.tabview.addTab(self.tab_plan, "Plan")
        self.tab_plan.setLayout(qtw.QGridLayout())  

        # Settings Tab
        self.tab_settings = SettingsTab(self)
        self.tabview.addTab(self.tab_settings, "Settings")

        self.setStyleSheet("""
            QToolTip {
                background-color: transparent;
                color: rgba(255, 255, 255, 0.9);
                border: 1px solid rgba(118, 118, 118, 0.6);
                border-radius: 5px;
                padding: 4px;
                font-size: 12px;
                font-family: Helvetica;
            }
            QToolTip > * {
                background-color: rgba(43, 43, 43, 0.8);
                border-radius: 4px;
            }
        """)

        # Create widgets
        self.create_top_frame()
        #self.populate_plan_tab()

        logger.info('GUI Initialized Successfully.')

    def create_top_frame(self):
        """
        Creates the top frame of the GUI, including labels, buttons, and a progress bar.
        Configures the grid layout and adds widgets to the frame.
        """
        
        # Configure grid weights for frame0
        self.frame0.setLayout(qtw.QGridLayout())

        self.frame0.layout().setColumnStretch(1, 1)  # Set weight for the all columns
        for i in range(3):
            self.frame0.layout().setRowStretch(i, 1)  # Set weight for all rows

        # Load the icon image using PIL
        icon_path = ResourceFinder.find_resource("ballac.png")
        icon_image = Image.open(icon_path)
        self.icon_image = qtg.QPixmap(icon_path)  # Use the icon path directly

        # Create button with icon
        icon_button = qtw.QPushButton()
        icon_button.setIcon(qtg.QIcon(self.icon_image))
        icon_button.setIconSize(self.icon_image.size())
        icon_button.setCursor(qtc.Qt.PointingHandCursor)
        if self.outdated_version:
            icon_button.setStyleSheet("background-color: red;")
            icon_button.setToolTip(f"Please update PickAssist\nCurrent Version: {THIS_VERSION}\nLatest Version: {self.latest_version}")
        else:
            icon_button.setFlat(True)  # Makes the button background transparent
        
        icon_button.clicked.connect(lambda: qtg.QDesktopServices.openUrl(qtc.QUrl("https://w.amazon.com/bin/view/Users/zavaugha/Projects/PickAssist")))
        self.frame0.layout().addWidget(icon_button, 0, 0, alignment=qtc.Qt.AlignLeft)

        # Add welcome label
        self.welcome_label = qtw.QLabel(
            f"PickAssist {self.version} // {self.site_code} \n|| Welcome {self.alias}! ||"
        )
        self.welcome_label.setFont(qtg.QFont("Helvetica", 16))
        self.frame0.layout().addWidget(self.welcome_label, 0, 1, alignment=qtc.Qt.AlignLeft)


        # Add link button to display source links
        self.link_button = qtw.QPushButton("🔗")
        self.link_button.setCursor(qtc.Qt.PointingHandCursor)
        self.link_button.setFont(qtg.QFont("Helvetica", 16))
        self.link_button.setToolTip("Click to open source links")

        self.frame0.layout().addWidget(self.link_button, 0, 2, alignment=qtc.Qt.AlignRight)


        # Add link button to display FC Eligibility
        self.cluster_button = qtw.QPushButton("👥")
        self.cluster_button.setCursor(qtc.Qt.PointingHandCursor)
        self.cluster_button.setFont(qtg.QFont("Helvetica", 16))
        self.cluster_button.setToolTip("Click to open FC Eligibility")
        self.cluster_button.clicked.connect(lambda: qtg.QDesktopServices.openUrl(qtc.QUrl(f"https://fc-eligibility-website-iad.aka.amazon.com/#/picker-eligibilities/{self.site_code}")))

        self.frame0.layout().addWidget(self.cluster_button, 0, 2, alignment=qtc.Qt.AlignLeft)

        # Add site selector
        site_selector = qtw.QComboBox()
        site_selector.setFont(qtg.QFont("Helvetica", 12))
        site_selector.setEditable(True)
        site_selector.lineEdit().setAlignment(qtc.Qt.AlignCenter)
        site_selector.lineEdit().setReadOnly(True)

        site_selector.addItems(SITES)
        site_selector.setCurrentText(self.site_code)
        site_selector.setEnabled(True)

        # Connect the signal
        site_selector.currentTextChanged.connect(self.on_site_changed)

        # Adjust size to content
        site_selector.setSizeAdjustPolicy(qtw.QComboBox.AdjustToContents)
        size = site_selector.sizeHint()
        site_selector.setMinimumWidth(size.width() + 30)  # Add padding for dropdown arrow


        # Add to layout
        self.frame0.layout().addWidget(site_selector, 0, 3, alignment=qtc.Qt.AlignCenter)


        # Add Go button
        self.go_button = qtw.QPushButton("Go!")
        self.go_button.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;  /* Green background color */
                color: white;  /* White text color */
                padding: 8px 16px;  /* Reduced padding */
                border: none;  /* Remove button border */
                border-radius: 4px;  /* Add rounded corners */
                font-size: 16px;  /* Increase font size */
                margin-top: 2px;  /* Reduced top margin */
                margin-bottom: 2px;  /* Reduced bottom margin */
            }
            
            QPushButton:hover {
                background-color: #45a049;  /* Slightly darker color on hover */
            }
        """)

        # Ensure single connection for go_button
        try:
            self.go_button.clicked.disconnect()
        except TypeError:
            pass  # No existing connections
        self.go_button.clicked.connect(self.handle_go_button_click)

        self.frame0.layout().addWidget(self.go_button, 1, 0, 1, 4)

        # Add last update label
        self.last_update_label = qtw.QLabel(f"Last Update: {self.last_update}")
        self.last_update_label.setFont(qtg.QFont("Helvetica", 12))
        self.frame0.layout().addWidget(self.last_update_label, 2, 0, 1, 2)

        # Add time remaining label
        self.time_remaining_label = qtw.QLabel(f"Shift Time Remaining: {self.formatted_hours_left_in_shift}")
        self.time_remaining_label.setFont(qtg.QFont("Helvetica", 12))
        self.frame0.layout().addWidget(self.time_remaining_label, 2, 2, 1, 2)

        # Update layout and adjust frame height
        self.frame0.updateGeometry()  # Update geometry to reflect changes

        # Hint that the user should define pick areas
        if self.pick_areas.height < 1:
            # Create a container frame
            warning_container = qtw.QFrame(self.tab_overview)
            warning_container.setFrameStyle(qtw.QFrame.Panel | qtw.QFrame.Raised)
            warning_container.setLayout(qtw.QVBoxLayout())

            # Create warning message
            warning_label = qtw.QLabel("No Pick Areas Configured")
            warning_label.setFont(qtg.QFont("Helvetica", 16, qtg.QFont.Bold))
            warning_label.setAlignment(qtc.Qt.AlignCenter)

            # Create instruction message
            instruction_label = qtw.QLabel("Please go to the Settings tab to configure Pick Areas")
            instruction_label.setFont(qtg.QFont("Helvetica", 12))
            instruction_label.setAlignment(qtc.Qt.AlignCenter)

            # Style the container
            warning_container.setStyleSheet("""
                QFrame {
                    background-color: #f0f0f0;
                    border-radius: 10px;
                    padding: 20px;
                }
                QLabel {
                    color: #404040;
                }
            """)

            # Add labels to container
            warning_container.layout().addWidget(warning_label)
            warning_container.layout().addWidget(instruction_label)
            warning_container.layout().addStretch()

            # Add container to overview tab, centered
            self.tab_overview.layout().addWidget(warning_container)


    def clear_all_tabs(self):
        """Clears all widgets from all tab views"""
        try:
            # Clear Overview Tab
            if self.tab_overview and self.tab_overview.layout():
                while self.tab_overview.layout().count():
                    item = self.tab_overview.layout().takeAt(0)
                    if item.widget():
                        item.widget().deleteLater()

            # Clear Details Tab
            if self.tab_details and self.tab_details.layout():
                while self.tab_details.layout().count():
                    item = self.tab_details.layout().takeAt(0)
                    if item.widget():
                        item.widget().deleteLater()

            # Clear Paths Tab and its nested tabview
            if self.tab_paths and self.tab_paths.layout():
                while self.tab_paths.layout().count():
                    item = self.tab_paths.layout().takeAt(0)
                    if item.widget():
                        item.widget().deleteLater()

            # Clear Plan Tab
            if self.tab_plan and self.tab_plan.layout():
                while self.tab_plan.layout().count():
                    item = self.tab_plan.layout().takeAt(0)
                    if item.widget():
                        item.widget().deleteLater()

            logger.info("All tabs cleared successfully")

        except Exception as e:
            logger.error(f"Error clearing tabs: {str(e)}")
            raise


    def on_site_changed(self, new_site):
        self.clear_all_tabs()
        logger.info(f'Changing site from {self.site_code} to {new_site}')
        self.go_button.setStyleSheet("""
            QPushButton {
                    background-color: #4CAF50;
                    color: white;  /* White text color */
                    padding: 8px 16px;  /* Reduced padding */
                    border: none;  /* Remove button border */
                    border-radius: 4px;  /* Add rounded corners */
                    font-size: 16px;  /* Increase font size */
                    margin-top: 2px;  /* Reduced top margin */
                    margin-bottom: 2px;  /* Reduced bottom margin */
            }
        """)
        self.site_code = new_site
        
        # Get fresh instances with new site data
        self.time_manager = TimeManager.get_instance(new=True)

        success = TimeManager.get_instance().setup_shift(
            site_code=new_site,
            start_hour=self.shift_start,
            end_hour=self.shift_end
        )
        if success:
            self.shift_info = TimeManager.get_instance().get_shift_info()
            self.site_builder = SiteBuilder.get_instance(new=True).get_site_info()
            DataProcessor.reset_instance()
            DataProcessor.get_instance()
            
            # Use current_time instead of now for last_update
            current_time = self.shift_info['current_time']
            self.last_update = current_time.strftime('%Y-%m-%d %H:%M:%S %Z')
            
            # Update other instance variables
            self.site_code = self.shift_info['site_code']
            self.timezone = self.shift_info['timezone']
            self.shift_start = self.shift_info['shift_start'].hour
            self.shift_end = self.shift_info['shift_end'].hour
            self.formatted_hours_left_in_shift = self.shift_info['formatted_time_remaining']
            
            # Update UI elements
            self.welcome_label.setText(f"PickAssist {self.version} // {new_site} \n|| Welcome {self.alias}! ||")
            self.last_update_label.setText(f"Last Update: {self.last_update}")
            self.time_remaining_label.setText(f"Shift Time Remaining: {self.formatted_hours_left_in_shift}")
            
            # Update site builder and pick areas
            self.site_builder = SiteBuilder.get_instance(new=True)
            self.site_info = self.site_builder.get_site_info()
            self.pick_areas = self.site_info['pick_areas']
            
            self.update_shift_progress_bar()
            logger.info(f"Site changed to {new_site} ({self.timezone})")
            
            # Update displayed pick areas
            self.tab_settings.update_pick_areas_display()
            self.run_it()


    def show_links(self):
        """
        Opens the URL stored in self.urls in a new browser window.
        """
        for description, url in self.urls.items():
            if url:  # Check if URL exists and is not empty
                try:
                    webbrowser.open(url, new=2)
                except Exception as e:
                    logger.error(f"Error opening {description} URL: {str(e)}")
            else:
                logger.warning(f"No {description} URL available to open.")


    # Initial Run and Refresh Logic
    def handle_go_button_click(self, first_run=False):
        """
        Handle manual refresh button click
        """
        logger.info("Go button clicked")
        self.refresh_state = self.tab_settings.refresh_state
        if self.pick_areas is None or self.pick_areas.height < 1:
            base_message = "No pick areas found.\nPlease add pick areas in Settings tab before attempting to run."
            logger.info('Attempted run with no pick areas')
            self.show_timed_warning(base_message, timeout=5000)
            self.tabview.setCurrentIndex(4)
            return
        
        if first_run == True:
            logger.info('First run')
            self.run_it()
        elif not self.tab_settings.refresh_state:
            # Manual refresh requested (refresh off)
            logger.info("Manual refresh requested (refresh off)")
            self.tab_settings.stop_auto_refresh()
            self.run_it()
        elif not self.is_auto_refreshing:
            # Starting new auto-refresh
            logger.info("Starting new auto-refresh")
            self.run_it()
            self.tab_settings.start_auto_refresh()
        else:
            # Manual refresh requested
            logger.info("Manual refresh requested")
            self.stop_auto_refresh()
            if self.refresh_state == True:
                self.run_it()
                self.tab_settings.start_auto_refresh(changed=False)
            else:
                self.run_it()

        self.time_manager.update_shift()
        

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

                self.display_staged_warnings()
                
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
                self.go_button.setText("Refresh")
                self.go_button.setStyleSheet("""
                    QPushButton {
                            background-color: #4CAF50;
                            color: white;  /* White text color */
                            padding: 8px 16px;  /* Reduced padding */
                            border: none;  /* Remove button border */
                            border-radius: 4px;  /* Add rounded corners */
                            font-size: 16px;  /* Increase font size */
                            margin-top: 2px;  /* Reduced top margin */
                            margin-bottom: 2px;  /* Reduced bottom margin */
                    }
                """)
                
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
                self.go_button.setText("Refresh")
                self.go_button.setStyleSheet("""
                    QPushButton {
                            background-color: #4CAF50;
                            color: white;  /* White text color */
                            padding: 8px 16px;  /* Reduced padding */
                            border: none;  /* Remove button border */
                            border-radius: 4px;  /* Add rounded corners */
                            font-size: 16px;  /* Increase font size */
                            margin-top: 2px;  /* Reduced top margin */
                            margin-bottom: 2px;  /* Reduced bottom margin */
                    }
                """)
                
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
            self.run_it()
            
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
            
        # Stop threading.Timer if exists
        if hasattr(self, 'thread_timer') and self.thread_timer is not None:
            self.thread_timer.cancel()
            self.thread_timer = None
            logger.info("Thread Timer Stopped")                

    def display_staged_warnings(self):
        if hasattr(self, 'warn_messages') and self.warn_messages:
            for message in self.warn_messages:
                self.show_timed_warning(message)
            # Clear the warnings after displaying them
            self.warn_messages = []
            logger.info('Cleared staged warnings')

    def show_timed_warning(self, base_message, timeout=5000):  # timeout in milliseconds
        msg = qtw.QMessageBox(self)
        msg.setIcon(qtw.QMessageBox.Warning)
        msg.setWindowTitle('Warning')
        msg.setText(base_message)
        msg.setStandardButtons(qtw.QMessageBox.Ok)
        
        # Create timer to auto-close
        timer = QTimer()
        timer.setSingleShot(True)
        timer.timeout.connect(msg.close)
        timer.start(timeout)  # 5000 ms = 5 seconds
        
        # Show the message non-modally
        msg.show()
        
        # Process events to ensure the message is displayed
        while msg.isVisible():
            self.app.processEvents()

    def show_timed_info(self, base_message, timeout=5000):  # timeout in milliseconds
        msg = qtw.QMessageBox(self)
        msg.setIcon(qtw.QMessageBox.Information)
        msg.setWindowTitle('Information')
        msg.setText(base_message)
        msg.setStandardButtons(qtw.QMessageBox.Ok)
        
        # Create timer to auto-close
        timer = QTimer()
        timer.setSingleShot(True)
        timer.timeout.connect(msg.close)
        timer.start(timeout)  # 5000 ms = 5 seconds
        
        msg.exec_()



    def update_shift_progress_bar(self):
        """Updates shift progress bar with current progress and styling"""
        try:
            # Get current shift info
            self.shift_info = TimeManager.get_instance().get_shift_info()
            progress_percent = self.shift_info['progress_percent']
            hours_remaining = float(self.shift_info['hours_remaining'].total_seconds() / 3600)
            shift_start = self.shift_info['shift_start']
            shift_end = self.shift_info['shift_end']
            
            # Update progress value
            self.shift_progress.setValue(int(progress_percent))

            
            # Set color based on time remaining
            style_template = "QProgressBar { text-align: center; %s }"
            if hours_remaining <= 1:
                color = "color: red; background-color: #ffebee;"
            elif hours_remaining <= 2:
                color = "color: orange; background-color: #fff3e0;"
            else:
                color = "color: green; background-color: #e8f5e9;"
                
            self.shift_progress.setStyleSheet(style_template % color)
            
            # Update tooltip with detailed information
            timeframe = (f"{shift_start.strftime('%Y/%m/%d %H:%M')} to "
                        f"{shift_end.strftime('%Y/%m/%d %H:%M')}")
            
            tooltip_text = (f"Shift Progress: {progress_percent:.1f}%\n"
                        f"Hours Remaining: {hours_remaining:.1f}\n"
                        f"Timeframe: {timeframe}")
            
            self.shift_progress.setToolTip(tooltip_text)
            
            # Force immediate update
            self.shift_progress.update()
            qtw.QApplication.processEvents()
            
        except Exception as e:
            logger.error(f"Error updating shift progress: {str(e)}")
            self.shift_progress.setValue(0)
            self.shift_progress.setToolTip("Error updating progress")


    def calculate_alignment(self, time_spent, time_passed, time_remaining, work_remaining, current_rate):
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

        # Calculate average headcount based on time spent and time passed
        # avg_hc represents the average number of associates per hour
        avg_hc = float((time_spent / time_passed))
        
        # Calculate the alignment based on remaining time and work
        # alignment represents the difference between the estimated time remaining and
        # the time required to complete the remaining work at the current rate
        alignment = float(((time_remaining) - (work_remaining / ((avg_hc * current_rate) or 999))))
        
        return alignment