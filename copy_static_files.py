from anipyke.lib import *
import copy
import logging
import os
import re
import shutil
import sys

logger.info(f"=== Copying static files ===")

for filePath in list_anipike_pages(lambda x: True, html_only=False):
    if filePath.endswith(".htm") or filePath.endswith(".html") or filePath.endswith(".cgi"):
        continue
    aniPath = filepath_to_anipike_path(filePath)
    if (aniPath is None) or aniPath.startswith("?") or aniPath.startswith("cgi-bin"):
        continue
    targetPath = f"static/{aniPath}"
    if os.path.exists(targetPath):
        continue
    logger.info(targetPath)
    create_dir_parent(targetPath)
    shutil.copy(filePath, targetPath)