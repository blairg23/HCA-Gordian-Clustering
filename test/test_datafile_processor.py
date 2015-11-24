# -*- coding: utf-8 -*-
'''
Name: test_datafile_processor.py
Author: Blair Gemmer
Version: 20151123

Description: Unit Testing for datafile_processor module
'''

import unittest
import os
import json

from ..DatafileProcessor import DatafileProcessor

class TestDatafileProcessorMethods(unittest.TestCase):

	def setUp(self):
		self.datafile_processor = DatafileProcessor()

	def test_create_trie(self):
		test_trie = {'b': {'a': {'r': {'_end_': '_end_', 'z': {'_end_': '_end_'}}, 'z': {'_end_': '_end_'}}}, 'f': {'o': {'o': {'_end_': '_end_'}}}} # The result trie should look like this
		result_trie = self.datafile_processor.create_trie('foo', 'bar', 'baz', 'barz')
		self.assertTrue(result_trie == test_trie)

	def test_in_trie(self):
		test_trie = self.datafile_processor.create_trie('foo', 'bar', 'baz', 'barz')

		test_query_true_list = ['baz', 'barz']
		test_query_false_list = ['barzz', 'bart', 'ba']
		truth_data_list = []

		for query in test_query_true_list:
			result = self.datafile_processor.in_trie(test_trie, query)
			truth_data_list.append(result)

		for query in test_query_false_list:
			if not self.datafile_processor.in_trie(test_trie, query):
				result = True
			truth_data_list.append(result)
		self.assertTrue(all(truth_data_list))

if __name__ == '__main__':
	unittest.main()