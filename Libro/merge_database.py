import sqlite3
from zipfile import ZipFile
from datetime import datetime
from pytz import timezone
import os
from django.core.files.storage import FileSystemStorage
from django.conf import settings
import shutil


def process_file(file):
    time = datetime.now(timezone('Chile/Continental'))
    timestamp_str = str(int(datetime.timestamp(time)))
    folder_name = timestamp_str+"_tmpdirectory"
    print(folder_name)
    os.makedirs(folder_name)
    print("folder made")
    fs = FileSystemStorage(location=folder_name)
    fs.save(file.name + 'source.zip', file)
    print("file saved")
    full_path_file = os.path.join(folder_name, file.name + 'source.zip')
    with ZipFile(full_path_file, 'r') as zip_ref:
        zip_ref.extractall(folder_name)
    print("zip extracted")
    files = os.listdir(folder_name)
    shutil.copy("claveprivada.pem", folder_name)
    db_name = None
    for f in files:
        if f.find("_encryptedD3.db") > 0:
            db_name = f
            break
    conn = sqlite3.connect(os.path.join(folder_name, db_name))
    res = conn.execute("SELECT * FROM Person")
    for r in res:
        print(r)

