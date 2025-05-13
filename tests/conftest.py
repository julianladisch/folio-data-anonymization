import pathlib
import sys


root_directory = pathlib.Path(__file__).parent.parent
dir = root_directory

sys.path.append(str(dir))
