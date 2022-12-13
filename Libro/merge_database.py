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
import sqlparse
import re
from . import models
import traceback

def get_fks_from_sql(sql_create: str) -> list:
    sql = sqlparse.format(sql_create, reindent=True, keyword_case='upper')
    sql_lines = sql.split("\n")
    fks = []
    for line in sql_lines:
        s2 = line.strip().replace(",", "")
        if s2.find("FOREIGN KEY") == 0:
            m = re.match('FOREIGN KEY\("[a-zA-Z]*"\)', s2)
            if m is None:
                continue
            tbl_name = m.group().replace('FOREIGN KEY("', "").replace('")', "")
            ref = s2[len(m.group()):].replace(' REFERENCES "', "").replace('")', "").replace(")", "").split('"("')
            if ref[1].find(" ") > 0:
                ref[1] = ref[1][:ref[1].find(" ")]
            # print(tbl_name, ref)
            fks.append((tbl_name, (ref[0], ref[1])))
    return fks


def get_pk(tables):
    pk_idx = None
    pk_name = None
    for i in range(0, len(tables)):
        if tables[i][5] == 1:
            if pk_idx is not None:
                print(tables[1])
            pk_idx = i
            pk_name = tables[i][1]
    return pk_idx, pk_name


def table_to_process(conn: sqlite3.Connection) -> tuple:
    res = conn.execute("SELECT tbl_name, sql FROM sqlite_schema")
    i_table = 0
    for r in res:
        i_table += 1

        # omit some specific non model tables
        # and all the views (ended in "List")
        if r[0] in ["sqlite_sequence", "_CEDStoNDSMapping", "tmp", "_CEDSElements", "Role"] \
                or r[0].find("List") == len(r[0]) - len("List"):  # find "List" at end of the name
            # print("------")
            continue
        # omit every reference table
        if r[0].find("Ref") == 0:
            # print("*****")
            continue
        print(f"{i_table}, {r[0]}")
        yield r[0], r[1]


def get_mappings_pk(conn: sqlite3.Connection):
    mapping_ids = {}
    all_fks = {}
    for tbl_name, sql_create in table_to_process(conn):
        if tbl_name in mapping_ids and tbl_name in all_fks and sql_create is None:
            continue
        if not tbl_name in mapping_ids:
            mapping_ids[tbl_name] = {}
        if not tbl_name in all_fks:
            all_fks[tbl_name] = {}
        if sql_create is not None:
            fks = get_fks_from_sql(sql_create)
            for col, ref in fks:
                all_fks[tbl_name][col] = ref
        if tbl_name == "IncidentPerson":
            print(fks)
            print(sql_create)
        data = conn.execute(f"SELECT * FROM {tbl_name}")
        tbl_info = conn.execute(f"pragma table_info({tbl_name})")
        tbl_info = [x for x in tbl_info]
        pk_idx, pk_name = get_pk(tbl_info)
        mapping_ids[tbl_name][pk_name] = {}
        with connection.cursor() as cursor:
            max_id = cursor.execute(f"SELECT max({pk_name}) FROM {tbl_name}").fetchone()
        max_id = max_id[0] + 1 if max_id[0] is not None else 0
        for row in data:
            if tbl_name == "Organization" and row[pk_idx] == 1:  # Ministerio de educación
                # mapping_ids[tbl_name][row[pk_idx]] = row[pk_idx]
                continue
            mapping_ids[tbl_name][pk_name][row[pk_idx]] = max_id
            if type(max_id) == int:
                max_id += 1
            else:
                raise Exception("not int pk")
    return mapping_ids, all_fks
def insert_rows(table_and_values: list):
    with connection.cursor() as cursor:
        sql_cmd = f"INSERT INTO {table_and_values['table_name']} VALUES {table_and_values['values']}"
        cursor.execute(sql_cmd)
        #cursor.commit()

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
    mapping_ids, all_fks = get_mappings_pk(conn)

    errors_at_insert = []
    processed_tables = []
    for tbl, sql in table_to_process(conn):
        if tbl in processed_tables:
            continue
        processed_tables.append(tbl)
        data = conn.execute(f"SELECT * FROM {tbl}")
        tbl_info = conn.execute(f"pragma table_info({tbl})")
        colnames = [x[1] for x in tbl_info]
        sc = "("
        for c in colnames:
            sc += str(c)+", "
        sc = sc[:-2] + ")"
        # print(sc)
        for drow in data:
            s = "("
            icol = 0
            try:
                for col in drow:
                    col_to_insert = col
                    if tbl in mapping_ids and colnames[icol] in mapping_ids[tbl] and col in mapping_ids[tbl][colnames[icol]]:
                        # print("MAPPING ", tbl, colnames[icol], col, mapping_ids[tbl][colnames[icol]][col])
                        col_to_insert = mapping_ids[tbl][colnames[icol]][col]
                    if (col is not None) and (tbl in all_fks) and (colnames[icol] in all_fks[tbl])\
                        and all_fks[tbl][colnames[icol]][0] in mapping_ids\
                            and all_fks[tbl][colnames[icol]][1] in mapping_ids[all_fks[tbl][colnames[icol]][0]]\
                                and col in mapping_ids[all_fks[tbl][colnames[icol]][0]][all_fks[tbl][colnames[icol]][1]]:
                                    # print("FK ", tbl, colnames[icol], col, all_fks[tbl][colnames[icol]])
                                    col_to_insert = mapping_ids[all_fks[tbl][colnames[icol]][0]][all_fks[tbl][colnames[icol]][1]][col]
                    if col is None:
                        s += "NULL, "
                    else:
                        if type(s) == str:
                            s += '"'+str(col_to_insert) + '", '
                        else:
                            s += str(col_to_insert)+", "
                    icol += 1
                s = s[:-2]+")"
                x = {'table_name': tbl, 'values': s}
                #print(x, drow)
                try:
                    insert_rows(x)
                except Exception as e:
                    print(e)
                    print(x)
                    print(drow)
                    errors_at_insert.append(x)
            except Exception as e:
                #pass
                print("ERROR: ", tbl, e)
                print(traceback.format_exc())
                    # print(s)
    i_error = 0
    cantidad_errores = 0
    while len(errors_at_insert) > 0:
        cantidad_errores = len(errors_at_insert)
        print(f"error {i_error} len: {len(errors_at_insert)}")
        errors = []
        for x in errors_at_insert:
            try:
                insert_rows(x)
            except Exception as e:
                #print(e)
                errors.append(x)
        errors_at_insert = errors
        i_error += 1
        if cantidad_errores == len(errors_at_insert):
            for x in errors_at_insert:
                print(x)
            break
    with open(os.path.join(folder_name, "mappings.json"), "w") as mapping_json_file:
        json.dump(mapping_ids, mapping_json_file, indent=4)
        mapping_json_file.flush()
    with open(os.path.join(folder_name, "fks.json"), "w") as mapping_json_file:
        json.dump(all_fks, mapping_json_file, indent=4)
        mapping_json_file.flush()
    #print(mapping_ids["Organization"])


