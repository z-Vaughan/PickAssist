import os
import sys
import PySide6.QtCore as qtc
import PySide6.QtWidgets as qtw


sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.config.constants import THIS_VERSION
from src.utils.logger import CustomLogger
logger = CustomLogger.get_logger(__name__)
#logger.error(f"Some Error: {str(e)}")
#logger.info("Some Info")


class VersionHandler:
    def __init__(self):
        self.latest_version = None
        self.outdated_version = False
        self.network_file_path = r"\\ant\dept-na\SAV7\Public\PickAssist\latest_version.txt"
        # Cache the QApplication instance
        self.app = qtw.QApplication.instance()

    def check_version(self):
        """
        Check the current version against the latest version from the network file.
        Returns tuple of (is_outdated, latest_version)
        """
        try:
            # Read file in one operation
            with open(self.network_file_path, 'r') as file:
                self.latest_version = file.readline().strip()

            # Compare versions only once
            if self.compare_versions(THIS_VERSION, self.latest_version):
                self.outdated_version = True
                self.raise_version_flag(self.latest_version)
            elif self.compare_versions(self.latest_version, THIS_VERSION):
                self.update_version_file(self.network_file_path, THIS_VERSION)
            else:
                logger.info(f"Current version ({THIS_VERSION}) is up to date.")

            return self.outdated_version, self.latest_version

        except FileNotFoundError:
            logger.error(f"Version file not found at {self.network_file_path}")
            return False, None
        except Exception as e:
            logger.error(f"Error reading version file: {str(e)}")
            return False, None

    @staticmethod
    def compare_versions(ver1, ver2):
        """
        Compare two version strings.
        Return True if ver1 is less than ver2.
        """
        # Convert versions to tuples once
        try:
            v1_tuple = tuple(map(int, ver1.split('.')))
            v2_tuple = tuple(map(int, ver2.split('.')))
            return v1_tuple < v2_tuple
        except (AttributeError, ValueError):
            return False

    def raise_version_flag(self, latest_version):
        """Raise a flag indicating that the current version is outdated."""
        logger.warning(f"Current version is outdated. Latest version is {latest_version}.")
        
        download_link = "https://drive.corp.amazon.com/view/zavaugha@/Codes/Python/PickAssist/SDCPickAssist.zip?download=true"
        message = f"""
        <p>A new version ({latest_version}) is available. Please update your application.</p>
        <p><a href="{download_link}" style="color: blue;">Download from Drive</a></p>
        """

        # Create QApplication only if needed
        if not self.app:
            self.app = qtw.QApplication([])

        # Create and configure message box
        msg_box = qtw.QMessageBox()
        msg_box.setWindowTitle("Version Update")
        msg_box.setModal(True)
        msg_box.setWindowFlags(msg_box.windowFlags() | qtc.Qt.WindowStaysOnTopHint)
        msg_box.setTextFormat(qtc.Qt.RichText)
        msg_box.setText(message)
        msg_box.setIcon(qtw.QMessageBox.Warning)
        msg_box.exec()

    def update_version_file(self, file_path, new_version):
        """Update the version file with the new version."""
        try:
            with open(file_path, 'w') as file:
                file.write(new_version)
            logger.info(f"Updated version file to {new_version}.")
        except Exception as e:
            logger.error(f"Error updating version file: {str(e)}")




