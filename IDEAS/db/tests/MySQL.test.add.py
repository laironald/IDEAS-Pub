#!/usr/bin/python

import unittest
import os
import sys
import IDEAS.lib.MySQLdb as MySQLdb
import IDEAS.config as IC
sys.path.append('..')
import MySQL

class TestMySQL(unittest.TestCase):

    def removeFile(self, file):
        #delete a file if it exists
        if os.path.isfile(file):
            os.system("rm {file}".format(file=file))

    def createFile(self, file, type=None, data="1,2,3"):
        if file.split(".")[-1] == "csv" or type == "csv":
            os.system("echo '{data}' >> {file}".\
                format(data=data, file=file))


    def setUp(self):
        self.m = MySQL.MySQL(IC.config["mysql"], tbl="test_test")

    def tearDown(self):
        self.m.delete(["test_test", "test_foo", "test_bar"])
        self.removeFile("test.csv")
        self.m.close()

if __name__ == '__main__':
    unittest.main()
