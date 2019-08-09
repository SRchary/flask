import os
from datetime import datetime

from loaders.mongo_data import export_data
from scripts.oracle_data import import_data

import conf


def run():
    try:
        folder_path = os.path.join(conf.OUTPUT_DIR,
                                   datetime.now().strftime('%Y%m%d%H%M'))
        print("Output Folder Path ", folder_path)
        os.makedirs(folder_path)
        print("Output Folder Created ", folder_path)
        import_data(folder_path)
        export_data(folder_path)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    run()
