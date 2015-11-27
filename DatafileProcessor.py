# -*- coding: utf-8 -*-
'''
Name: DatafileProcessor.py
Author: Blair Gemmer
Version: 20151125

Description: 

1. Able to open datafiles, which have been encoded in json or csv format.

2. Performs clustering techniques on the data, using the specified (Gordian or HCA) clustering method.

3. Outputs the minimal unique column combinations.
'''
import numpy as np
import pandas as pd
import os
import json
import csv
from Node import Node, load_dictionary, load_dataframe, in_trie

class DatafileProcessor():
	def __init__(self, filename=None, algorithm='Gordian', verbose=False):
		if filename != None:
			dataframe = self.read_file(filename=filename, verbose=verbose)
		else:
			print "[ERROR] Requires a filename."

		if algorithm.lower() == 'gordian':
			self.results = self.Gordian(dataframe=dataframe, verbose=verbose)
		elif algorithm.lower() == 'hca':
			self.results = self.HCA(dataframe=dataframe, verbose=verbose)
		else:
			print '[ERROR] Specify an algorithm (HCA or Gordian).'

	def read_file(self, filename=None, verbose=False):
		if verbose:		
			print '[Processing file: {filename}...]\n'.format(filename=filename)
		name, ext = os.path.splitext(filename)
		if ext == '.json':
			if verbose:
				print '[Processing as JSON...]\n'
			dataframe = self.read_json(filename=filename, verbose=verbose)
		elif ext == '.csv':
			if verbose:
				print '[Processing as CSV...]\n'
			dataframe = self.read_csv(filename=filename, verbose=verbose)
		else:
			print '[ERROR] Requires file encoded in the CSV or JSON file format.'		
		return dataframe

	def read_json(self, filename=None, dataset_header='dataset', verbose=False):
		'''
		Reads in a file in the JSON format and returns a Pandas dataframe.
		'''
		if verbose:
			print '[Reading JSON file...]\n'
		def byteify(input):
			'''
			Removes unicode encodings from the given input string.
			'''
			if isinstance(input, dict):
				return {byteify(key):byteify(value) for key,value in input.iteritems()}
			elif isinstance(input, list):
				return [byteify(element) for element in input]
			elif isinstance(input, unicode):
				return input.encode('utf-8')
			else:
				return input
		def json_to_csv(filename=None, dataframe=None):
			'''
			Converts the json dataframe to csv format
			'''
			if verbose:
				print '[Translating to CSV format...]\n'
			filename, ext = os.path.splitext(filename)
			filename += '.csv'
			keys = set(dataframe[0].keys())
			for row in dataframe:
				for key in row.keys():
					if key not in keys:
						keys.add(key)
			with open(filename, 'w+') as csv_file:
				writer = csv.DictWriter(csv_file, fieldnames=keys)
				writer.writeheader()

				for row in dataframe:
					row = byteify(row) # Removes unicode characters
					writer.writerow(row)
			if verbose:
				print '[Wrote file: {filename}...]\n'.format(filename=filename)
			return filename

		# Convert JSON to CSV:
		dataframe = pd.read_json(filename)[dataset_header]
		csv_file = json_to_csv(filename=filename, dataframe=dataframe)
		
		return self.read_csv(filename=csv_file, verbose=verbose)

	def read_csv(self, filename=None, verbose=False):
		'''
		Reads in a file in the CSV format.
		'''
		if verbose:
			print '[Reading CSV file...]\n'
		dataframe = pd.read_csv(filename)
		#data_dict = dataframe.to_dict()
		if verbose:
			print '[Printing Dataframe Info...]\n'
			print '[Keys: ]\n'
			print dataframe.keys(), '\n'
			print '[Data: ]\n'
			print dataframe.head(), '\n'			
		return dataframe

	def create_trie(self, dataframe=None, keys=None, verbose=False):	
		_end = '_end_'

		if verbose:
			print '[Creating a prefix tree...]\n'
			print '[Keys: ]', '\n'
			print ','.join(keys)
			# for key in keys:
			# 	print key
			print '\n'		

		trie = load_dataframe(dataframe=dataframe)
		
		return trie

	def Gordian(self, dataframe=None, verbose=False):
		if verbose:
			print '[Performing Gordian analysis on dataframe...]\n'
		trie = self.create_trie(dataframe=dataframe, keys=dataframe.keys(), verbose=verbose)
		for index, row in dataframe.iterrows():
		# 	row = [str(words).strip().lower() for words in row]
		# 	print row
			print '[Results: ]'
			print in_trie(trie=trie, words=row, verbose=False), '\n'
		return trie


	def HCA(self, dataframe=None, verbose=False):
		if verbose:
			print '[Performing HCA analysis on dataframe...]\n'
		pass


	def HCA_Gordian(self):
		pass

if __name__ == '__main__':
	test_data = os.path.join('data', 'data.json')
	test_techcrunch = os.path.join('data', 'techcrunch.csv')
	test_json = os.path.join('data', 'test_data.json')
	test_csv = os.path.join('data', 'test_data.csv')

	verbose = True
	dfp = DatafileProcessor(filename=test_json, algorithm='Gordian', verbose=verbose)

	if verbose:
		print '[Results: ]'
		print dfp.results
		print dfp.results.children
		# for child in dfp.results.children:
		# 	print dfp.results.children[child].children
	