from datetime import datetime
import re
import os

# set the working directory to the current directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

f = open("version.txt", "r", encoding="utf-8")
version = f.read()
f.close()

# use regex to get the date from the version.txt file
version = re.search(r"(\d{4}\.\d{2}\.\d{2})\.(\d+)", version)
if version is None:
    print("Error: could not parse version.txt")
    exit(1)
version = version.groups()
version_date = version[0]
version_number = version[1]


# if the current date is the same as the date in the file,
# increment the version number
if datetime.now().strftime("%Y.%m.%d") == version_date:
    version_number = int(version_number) + 1
    version_number = str(version_number)
    version_date = datetime.now().strftime("%Y.%m.%d")
    version = version_date + "." + version_number

else:
    version_number = "0"
    version_date = datetime.now().strftime("%Y.%m.%d")
    version = version_date + "." + version_number


print(f"New version: {version}")

f = open("version.txt", "w", encoding="utf-8")
f.write(version)
f.close()
