
import os
import sys
import PySide6.QtCore as qtc
import PySide6.QtGui as qtg
import PySide6.QtWidgets as qtw
# Module Path Fix
sys.path.append(os.path.dirname(__file__))
from ui.app import SDCPickAssistApp
from utils.logger import CustomLogger

logger = CustomLogger.get_logger(__name__)
#logger.error(f"Some Error: {str(e)}")
# - automatically includes traceback and slack post
#logger.info("Some Info")

# package with:  pyinstaller --clean --noconfirm sdcpa.spec

def start_application():

    """
    Initializes and starts the application.
    Creates a new QApplication instance if one doesn't exist.
    """
    try:
        # Check if QApplication instance exists
        app = qtw.QApplication.instance()
        if app is None:
            app = qtw.QApplication(sys.argv)
        
        app.setStyle("Fusion")
        app.setPalette(qtg.QPalette(qtc.Qt.gray))
        
        # Create and show main window
        sdcpa = SDCPickAssistApp()
        sdcpa.app = app  # Store reference to app
        sdcpa.show()
        
        return app.exec()
        
    except Exception as e:
        logger.error(f"Error starting application: {str(e)}")
        raise


if __name__ == "__main__":
    try:
        start_application()
    except Exception as e:
        raise e




##    _____________________________________
##   / Please Retain Credit:              /
##  / Created by Zac Vaughan (zavaugha)  / 
## /__Last Update: 3/7/2025_____________/   
##  
##   /\_/\
##  ( o.o ) ~Howdy, y'all
##   > ^ <  

