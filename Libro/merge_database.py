import sqlite3
from zipfile import ZipFile
from datetime import datetime
from pytz import timezone
import os
from django.core.files.storage import FileSystemStorage
from django.conf import settings
import shutil
import json
from django.db import connection

from . import models


def get_pk(tables):
    pk_idx = None
    for i in range(0, len(tables)):
        if tables[i][5] == 1:
            if pk_idx is not None:
                print(tables[1])
            pk_idx = i
            pk_name = tables[i][1]
    return pk_idx, pk_name


def get_mappings_pk(conn: sqlite3.Connection):
    res = conn.execute("SELECT tbl_name, sql FROM sqlite_schema")
    mapping_ids = {}
    i_table = 0
    for r in res:
        i_table += 1
        print(f"{i_table}, {r[0]}")
        # omit some specific non model tables
        # and all the views (ended in "List")
        if r[0] in ["sqlite_sequence", "_CEDStoNDSMapping", "tmp"] or r[0].find("List") > 0:
            print("------")
            continue
        # omit every reference table
        if r[0].find("Ref") == 0:
            print("*****")
            continue
        data = conn.execute(f"SELECT * FROM {r[0]}")
        mapping_ids[r[0]] = {}
        tbl_info = conn.execute(f"pragma table_info({r[0]})")
        tbl_info = [x for x in tbl_info]
        pk_idx, pk_name = get_pk(tbl_info)
        with connection.cursor() as cursor:
            max_id = cursor.execute(f"SELECT max({pk_name}) FROM {r[0]}").fetchone()
        max_id = max_id[0] + 1 if max_id[0] is not None else 0
        for row in data:
            if r[0] == "Organization" and row[pk_idx] == 1:  # Ministerio de educación
                mapping_ids[r[0]][row[pk_idx]] = row[pk_idx]
                continue
            mapping_ids[r[0]][row[pk_idx]] = max_id
            if type(max_id) == int:
                max_id += 1
            else:
                raise Exception("not int pk")
    return mapping_ids


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
    # shutil.copy("claveprivada.pem", folder_name)
    db_name = None
    encrypt_key = None
    # find database and key files
    for f in files:
        if f.find("_encryptedD3.db") > 0:  # database file
            db_name = f
        if f.find("_key.encrypted") > 0:  # key file
            encrypt_key = f
    if db_name is None:
        raise Exception("No hay base de datos")
    openssl_cmd = f'openssl rsautl -oaep -decrypt -inkey "{os.path.join(settings.BASE_DIR, "claveprivada.pem")}" -in "{encrypt_key}" -out "{os.path.join(folder_name, timestamp_str)}_key.txt"'
    conn = sqlite3.connect(os.path.join(folder_name, db_name))
    mapping_ids = get_mappings_pk(conn)
    with open(os.path.join(folder_name, "mappings.json"), "w") as mapping_json_file:
        json.dump(mapping_ids, mapping_json_file, indent=4)
        mapping_json_file.flush()
    print(mapping_ids["Organization"])


