#!/usr/bin/env python3

__author__ = "A. Daouzli"
__version__ = "0.1"

import os
import re
import sqlite3


class ManageSqliteDB(object):
    def __init__(self, db_file="tst.sqlite", auto=False):
        '''Initializes the DB object.
        :param db_file: the file that contains the sqlite DB
        :type db_file: str
        :param auto: if True will open the DB else the user will have to run
        himself the method `opendb()`.'''
        self.db_file = db_file
        self.db = None
        if auto == True:
            self.opendb()

    def __del__(self):
        self.closedb()

    def opendb(self, db_file=None):
        '''Open the DB from file and create a cursor.'''
        if db_file is not None:
            self.db_file = db_file
        result = True
        db = sqlite3.connect(self.db_file)
        if db is None:
            print("failed to open DB '{}'".format(self.db_file))
            result = False
        else:
            self.db = db
            self.cursor = db.cursor()
        return result

    def closedb(self):
        if self.db:
            self.db.commit()
            self.cursor.close()
            self.db.close()
            self.db = None
            self.cursor = None

    def create(self, table, fields):
        '''Creates a table with provided fields.
        :param fields: list of either name fields (will be TEXT) or tuples
        (name,type).
        :type fileds: list of str and/or tuples
        '''
        txt = ''
        for field in fields:
            if type(field) is tuple:
                txt += field[0] + " " + field[1]
            else:
                txt += field + " TEXT"
            txt += ', '
        txt = ", ".join(txt.split(", ")[:-1])
        req = "CREATE TABLE {} ({})".format(table, txt)
        self.request(req)

    def list_tables(self):
        self.request("SELECT name FROM sqlite_master WHERE type='table'")
        return [e[0] for e in self.cursor.fetchall()]

    def request(self, req, values=None):
        if values:
            self.cursor.execute(req, values)
        else:
            self.cursor.execute(req)
        self.db.commit()

    def set(self, table, **kwargs):
        '''Sets records in a table.

        :param table: the table
        :param **kwargs: set of key/value to set in the table
        '''
        val_pattern = '? ' * len(kwargs.values())
        val_pattern = ", ".join(val_pattern.split())
        values = tuple(kwargs.values())
        req = "INSERT INTO {}({}) VALUES ({})".format(table,
                    "'" + "','".join(kwargs.keys()) + "'",
                    val_pattern
                    )
        self.request(req)

    def get(self, table, fields=None):
        '''Gets from the DB some fields from all records with or without
        condition(s).
        :param fields: list of fields to retrieve. If not given all fields are
        retrieved.
        :type fields: list or None
        :returns: a list of the found items
        :rtype: list of tuples
        '''
        if fields is None:
            fields = '*'
        else:
            fields = ", ".join(fields)
        req = "SELECT {} FROM {}".format(
                fields,
                table
                )
        self.request(req)
        res = self.cursor.fetchall()
        return res

    def delete(self, table, **kwargs):
        condition = " AND ".join([k + "='" + v + "'" for k, v in kwargs.items()])
        req = "DELETE FROM {} WHERE {}".format(
            table,
            condition
            )
        self.request(req)

