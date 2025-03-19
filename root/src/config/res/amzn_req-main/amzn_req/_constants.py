import os
import pathlib


HOME_FOLDER: pathlib.Path = pathlib.Path.home()
MIDWAY_COOKIE_FILENAME: str = os.path.join(HOME_FOLDER, ".midway", "cookie")
