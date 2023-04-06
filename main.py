# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import atexit
import sys

import numpy as np;
import sqlite3;
import os


class Hat:
    def __init__(self, id, topping, supplier, quantity):
        self.id = id
        self.topping = topping
        self.supplier = supplier
        self.quantity = quantity


class Supplier:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class Order:
    def __init__(self, id, location, hat):
        self.id = id
        self.location = location
        self.hat = hat

    # Data Access Objects:
    # All of these are meant to be singletons


class _Hats:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, hat):
        self._conn.execute("""
               INSERT INTO hats (id, topping, supplier, quantity) VALUES (?, ?, ?, ?)
           """, [hat.id, hat.topping, hat.supplier, hat.quantity])

    def find(self, topping, supplier):
        c = self._conn.cursor()
        c.execute("""
            SELECT * FROM hats WHERE topping = ? AND supplier=?
        """, [topping, supplier])
        hat = Hat(*c.fetchone())
        return hat.id

    def find_min_supplier(self, topping):
        c = self._conn.cursor()
        c.execute("""
                   SELECT min(supplier), quantity, id FROM (
                   SELECT * FROM hats WHERE topping = ?)
               """, [topping])
        supplier_id = c.fetchone()
        return supplier_id

    def remove_one(self, supplier, topping, quantity):
        self._conn.execute("""
                   UPDATE hats SET quantity=(?) WHERE supplier=(?) AND topping=(?)
               """, [quantity, supplier, topping])
        self._conn.execute("""
                          DELETE FROM hats WHERE quantity=0 
                      """)


class _Suppliers:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, supplier):
        self._conn.execute("""
        INSERT INTO suppliers (id, name) VALUES (?, ?)
        """, [supplier.id, supplier.name])

    def find(self, id):
        c = self._conn.cursor()
        c.execute("""
                SELECT id,name FROM suppliers WHERE id = ?
                """, [id])
        return Supplier(*c.fetchone())


class _Orders:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, order):
        self._conn.execute("""INSERT INTO orders (id, location, hat) VALUES (?, ?, ?)
         """, [order.id, order.location, order.hat])

    def find_all(self):
        c = self._conn.cursor()
        all = c.execute("""
        SELECT id, location, hat FROM orders
         """).fetchall()
        return [Order(*row) for row in all]

    # The Repository


class _Repository:
    def __init__(self):
        if os.path.exists(sys.argv[4]):
            os.remove(sys.argv[4])
        self._conn = sqlite3.connect(sys.argv[4])
        self.hats = _Hats(self._conn)
        self.suppliers = _Suppliers(self._conn)
        self.orders = _Orders(self._conn)
        self.create_tables()
        configfile = open(sys.argv[1], "r")
        self.reading_config(configfile)
        orderfile = open(sys.argv[2], "r")
        outputfile = open(sys.argv[3], "w+")
        self.reading_orders(orderfile, outputfile)

    def _close(self):
        self._conn.commit()
        self._conn.close()

    def create_tables(self):
        self._conn.executescript("""
        CREATE TABLE hats (
                id      INT         PRIMARY KEY,
                topping      TEXT        NOT NULL,
                supplier        INT,
                quantity        INT        NOT NULL,
                
                FOREIGN KEY(topping)     REFERENCES suppliers(id)
            );

            CREATE TABLE suppliers (
                id      INT     PRIMARY KEY,
                name     TEXT    NOT NULL
            );

            CREATE TABLE orders (
                id      INT     NOT NULL,
                location  INT     NOT NULL,
                hat     INT     NOT NULL,

                FOREIGN KEY(hat)     REFERENCES hats(id)
            ); 
            """)

    # the repository singleton
    def reading_config(self, configfile):
        lines = configfile.read()
        count = 0
        line = ""
        while count < len(lines) and lines[count] != '\n':
            line = line + lines[count]
            count = count + 1
        hatsnumber = 0
        suppliersnumber = 0
        countline = 0
        check = 0
        while countline < len(line):
            number = ""
            while countline < len(line) and line[countline] != ',':
                number = number + line[countline]
                countline = countline + 1
            countline = countline + 1
            if check == 0:
                hatsnumber = int(number)
                check = check + 1
            else:
                suppliersnumber = int(number)
        count = count + 1
        while count < len(lines):
            line = ""
            while count < len(lines) and lines[count] != '\n':
                line = line + lines[count]
                count = count + 1
            if hatsnumber > 0:
                countline = 0
                countpart = 0
                id = 0
                topping = ""
                supplier = 0
                quantity = 0
                while countline < len(line):
                    part = ""
                    while countline < len(line) and line[countline] != ',':
                        part = part + line[countline]
                        countline = countline + 1
                    if countpart == 0:
                        id = int(part)
                    else:
                        if countpart == 1:
                            topping = part
                        else:
                            if countpart == 2:
                                supplier = int(part)
                            else:
                                quantity = int(part)
                    countpart = countpart + 1
                    countline = countline + 1
                self.hats.insert(Hat(id, topping, supplier, quantity))
                hatsnumber = hatsnumber - 1
            else:
                countline = 0
                countpart = 0
                id = 0
                name = ""
                while countline < len(line):
                    part = ""
                    while countline < len(line) and line[countline] != ',':
                        part = part + line[countline]
                        countline = countline + 1
                    if countpart == 0:
                        id = int(part)
                    else:
                        name = part
                    countpart = countpart + 1
                    countline = countline + 1
                self.suppliers.insert(Supplier(id, name))
            count = count + 1

    def reading_orders(self, orderfile, outputfile):
        lines = orderfile.read()
        count = 0
        order_id = 1
        while count < len(lines):
            line = ""
            while count < len(lines) and lines[count] != '\n':
                line = line + lines[count]
                count = count + 1
            location = ""
            topping = ""
            countpart = 0
            countline = 0
            while countline < len(line):
                part = ""
                while countline < len(line) and line[countline] != ',':
                    part = part + line[countline]
                    countline = countline + 1
                countline = countline + 1
                if countpart == 0:
                    location = part
                else:
                    topping = part
                countpart = countpart + 1
            min_supplier = self.hats.find_min_supplier(topping)

            if min_supplier[0] is not None and min_supplier[1] is not None and min_supplier[2] is not None:
                supplier = min_supplier[0]
                quantity = min_supplier[1] - 1
                hat_id = min_supplier[2]
                sup = self.suppliers.find(supplier)
                name = sup.name
                self.hats.remove_one(supplier, topping, quantity)
                self.orders.insert(Order(order_id, location, hat_id))
                ans = topping + "," + name + "," + location
                outputfile.write(ans+"\n")
            order_id = order_id + 1
            count = count + 1
        outputfile.close()


repo = _Repository()
atexit.register(repo._close)
