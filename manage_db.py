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

    def list_fields(self, table):
        self.request("PRAGMA table_info('{}')".format(table))
        return [e[1] for e in self.cursor.fetchall()]

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


def show_menu():
    txt = '------------\n'
    txt += '0 - exit\n'
    txt += '1 - open DB\n'
    txt += '2 - close\n'
    txt += '3 - get\n'
    txt += '4 - set\n'
    txt += '5 - del\n'
    txt += '6 - list tables\n'
    txt += '7 - create table\n'
    txt += '8 - request\n'
    txt += '9 - list fields\n'
    print(txt)


if __name__ == '__main__':
    print ("ManageSqliteDB v" + __version__)
    db = ManageSqliteDB(auto=False)
    txt = '\nManageDB interactive\n'
    print(txt)
    while True:
        show_menu()
        c = int(input('Your choice: '))
        if c == 0:
            break

        elif c == 1:
            n = input('DB file name: ')
            db.opendb(n)

        elif c == 2:
            db.closedb()

        elif c == 3:
            table = input('Table where to get items: ')
            fields = input('Fields to get (coma separated, else "*" for all): ')
            if fields == "*":
                fields = None
            else:
                fields = [s.strip() for s in fields.split(",")]
            print(db.get(table, fields))

        elif c == 9:
            table = input('Table where to get fields: ')
            print(db.list_fields(table))

        elif c == 4:
            table = input('Table where to add item: ')
            couples = input('Couples of field=value to set (coma separated): ')
            couples = [s.strip() for s in couples.split(",")]
            data = {}
            for couple in couples:
                k, v = couple.split("=")
                data[k.strip()] = v.strip()
            db.set(table, **data)

        elif c == 5:
            table = input('Table where to delete an item: ')
            couples = input('Couples of field=value identifying the item (coma separated): ')
            couples = [s.strip() for s in couples.split(",")]
            data = {}
            for couple in couples:
                k, v = couple.split("=")
                data[k.strip()] = v.strip()
            db.delete(table, **data)

        elif c == 6:
            print(db.list_tables())

        elif c == 7:
            table = input('Table to create: ')
            fields = input('List (coma separated) of fields (of type TEXT) and/or couples field=type: ')
            fields = [s.strip() for s in fields.split(",")]
            l = []
            for field in fields:
                if "=" in field:
                    k, v = field.split("=")
                    l.append((k.strip(), v.strip()))
                else:
                    l.append(field.strip())
            db.create(table, l)

        elif c == 8:
            req = input('Request: ')
            print("Result:\n{}".format(db.request(req)))

