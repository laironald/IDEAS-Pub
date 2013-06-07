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
        self.m.delete(["test_test", "test_foo", "test_bar", "test_moo"])
        self.removeFile("test.csv")
        self.m.close()

    def test___init__(self):
        pass

    def test_chgTbl(self):
        self.m.chgTbl("test_test2")
        self.assertEqual("test_test2", self.m.tbl)

    def test_insert(self):
        self.createFile("test.csv", data='c,b,a')
        self.createFile("test.csv", data='X,X,X')
        self.m.insert({'a':'D'})
        self.m.insert([{'a':'z', 'c':'z'},[5,'D',15]], field=['c','b','a'])
        self.m.insert([[5,5],[6,6]], field=['a','b'])
        self.m.insert([['a','c'],['Z','Z']], header=True)
        self.m.insert([['a','c'],['Z','Z']], tbl="test_foo", header=True)
        self.m.insert([['a','c'],['Z','Z']], tbl="test_foo")
        self.m.insert([['a','c'],['Z','Z']], tbl="test_bar")
        self.m.insert([['X']], tbl="test_bar")
        self.m.insert([['a','c'],['Z','Z'],['Y','Y'],['X']], 
            header=True, tbl="test_moo")
        #self.m.insert([['Z','Z','Z']], tbl="test_bar", errlog="err")
        self.m.insert(self.m.csvInput("test.csv", iter=True), header=True)
        self.m.insert(self.m.csvInput("test.csv", iter=True))
        #test to ensure that our inserts are to our expectations

        #print self.m.fetch(header=True)
        #print self.m.fetch(header=True, tbl="test_foo")
        #print self.m.fetch(header=True, tbl="test_bar")

        self.assertEqual({'a': ['Z', 'a', 'Z'], 'c': ['Z', 'c', 'Z']},
            self.m.fetch(header=True, tbl="test_foo"))
        self.assertEqual({'v1': ['a', 'Z', 'X'], 'v2': ['c', 'Z', None]},
            self.m.fetch(header=True, tbl="test_bar"))
        self.assertEqual({'a': ['Z', 'Y', 'X'], 'c': ['Z', 'Y', None]},
            self.m.fetch(header=True, tbl="test_moo"))

    def test_fetch(self):
        self.m.c.execute("CREATE TABLE test_test (a INTEGER, b INTEGER, c INTEGER)")
        self.m.c.execute("INSERT INTO test_test VALUES (1,3,4)")
        self.m.c.execute("INSERT INTO test_test VALUES (2,3,4)")
        self.assertEqual(2, len(self.m.fetch()))
        self.assertFalse(self.m.fetch(table="foo"))
        self.assertTrue((2,3,4) in self.m.fetch(random=True))
        self.assertTrue(1, len(self.m.fetch(limit=1)))
        self.assertEqual(((1L,), (2L,)),
            self.m.fetch(field=["a"]))
        self.assertEqual({'a': [1L, 2L]}, self.m.fetch(field=["a"], header=True))

    def test_column(self):
        self.m.insert([[5,10,15]], field=['a','B','c'])
        self.assertEqual(['a','B','c'], self.m.columns())
        self.assertEqual(['a','b','c'], self.m.columns(lower=True))
        self.assertEqual([], self.m.columns(tbl="test_foo2"))
        self.assertTrue(self.m.columns(lookup='a'))
        self.assertFalse(self.m.columns(lookup='A'))
        self.assertTrue(self.m.columns(lower=True, lookup='a'))
        self.assertTrue(self.m.columns(lower=True, lookup='A'))
        self.assertTrue(
            self.m.columns(types=True),
            [('a', 'int'), ('B', 'int'), ('c', 'int')])
        self.assertTrue(
            self.m.columns(types=True, lower=True),
            [('a', 'int'), ('B', 'int'), ('c', 'int')])

    def test_add(self):
        self.m.insert([[5,10,15]], field=['a','B','c'])
        self.m.add("d")
        self.m.add("e")
        self.m.add(keys=["e", "f"])
        self.assertItemsEqual(["B","a","c","d","e","f"],
            self.m.columns())
        self.m.insert([['a','c'],['Z','Z']], tbl="test_foo", header=True)
        self.m.add({"G":"INTEGER", "H":"TEXT"}, table="test_foo")
        self.assertItemsEqual(["a","c","G","H"],
            self.m.columns(table="test_foo"))

    def test_drop(self):
        self.m.insert([[5,10,15,20,25,30]], field=['a','B','c', 'd', 'e', 'f'])
        self.m.drop("d")
        self.m.drop("e")
        self.m.drop(keys=["e", "f"])
        self.assertItemsEqual(["B","a","c"], self.m.columns())

    def test_indexes(self):
        self.m.c.execute("CREATE TABLE test_test (a INTEGER, b INTEGER, c INTEGER)")
        self.m.c.execute("CREATE INDEX idx ON test_test (a)")
        self.m.c.execute("CREATE INDEX idy ON test_test (a,b)")
        self.assertIn('idx', self.m.indexes())
        self.assertTrue(self.m.indexes(lookup="idx"))
        self.assertFalse(self.m.indexes(lookup="xdi"))
        self.assertEquals([0,0], self.m.indexes(seq="xdi"))
        self.assertEquals([1,1], self.m.indexes(seq="idx"))
        self.m.c.execute("CREATE INDEX idx1 ON test_test (b)")
        self.m.c.execute("CREATE INDEX idx2 ON test_test (c)")
        self.m.c.execute("CREATE INDEX idx5x3 ON test_test (a)")
        self.m.c.execute("CREATE INDEX idx10x ON test_test (a)")
        self.assertEquals([1,3], self.m.indexes(seq="idx"))

    def test__baseIndex(self):
        self.m.c.execute("CREATE TABLE test_test (a INTEGER, b INTEGER, c INTEGER)")
        self.m.c.execute("CREATE INDEX idx ON test_test (a)")
        self.m.c.execute("CREATE INDEX idy ON test_test (b,a)")
        self.m.c.execute("CREATE UNIQUE INDEX idz ON test_test (c,a)")
        self.assertItemsEqual(['(a)', '(a,b)', 'unique (a,c)'],
            self.m._baseIndex())
        self.assertEqual('(a)', self.m._baseIndex(idx="idx"))
        self.assertEqual('(bar,foo)',
            self.m._baseIndex(idx="create index x on foo (foo, bar)"))
        self.assertEqual('unique (foo)',
            self.m._baseIndex(idx="create unique index x on foo (foo)"))

    def test_index(self):
        self.m.c.execute("CREATE TABLE test_test (a INTEGER, b INTEGER, c INTEGER)")
        self.m.c.execute("CREATE TABLE test_foo (d INTEGER, e INTEGER, f INTEGER)")
        self.m.index(['a','c'])
        self.assertIn('(a,c)', self.m._baseIndex())
        self.m.index('a', unique=True)
        self.assertIn('unique (a)', self.m._baseIndex())
        self.m.index('f', tbl="test_foo")
        self.m.index('e', tbl="test_foo")
        self.assertIn('(f)', self.m._baseIndex(tbl="test_foo"))
        self.m.index(['e', 'f'], combo=True, tbl="test_foo")
        self.assertIn('(e)', self.m._baseIndex(tbl="test_foo"))
        self.assertIn('(e,f)', self.m._baseIndex(tbl="test_foo"))

if __name__ == '__main__':
    unittest.main()
