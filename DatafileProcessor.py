# -*- coding: utf-8 -*-
'''
Name: DatafileProcessor.py
Author: Blair Gemmer
Version: 20151123

Description: 

1. Able to open datafiles, which have been encoded in json or csv format.

2. Performs clustering techniques on the data, using the Gordian and HCA clustering methods.

3. Outputs the maximal uniqueness
'''
import numpy as np
import pandas as pd
import os
import json
import csv

class DatafileProcessor():
	def __init__(self, filename=None, verbose=False):
		if filename != None:
			self.read_file(filename=filename, verbose=verbose)
		else:
			print "[ERROR] Requires a filename."

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
			# Find max number of keys and use that as our header:
			# max_keys = 0
			keys = set(dataframe[0].keys())
			for row in dataframe:
				for key in row.keys():
					if key not in keys:
						keys.add(key)
				# if len(row.keys()) > max_keys:
				# 	max_keys = len(row.keys())
				# 	max_candidate = row.keys()
				# 	print max_candidate
			# print dataframe[0].keys()
			with open(filename, 'w+') as csv_file:
				writer = csv.DictWriter(csv_file, fieldnames=keys)
				writer.writeheader()

				for row in dataframe:
					row = byteify(row)
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
			print dataframe.head()			
		return dataframe

	

	def HCA(self):
		pass

	def Gordian(self):
		pass

	def HCA_Gordian(self):
		pass

if __name__ == '__main__':
	test_data = os.path.join('data', 'data.json')
	test_techcrunch = os.path.join('data', 'techcrunch.csv')
	test_json = os.path.join('data', 'test_data.json')
	test_csv = os.path.join('data', 'test_data.csv')
	dfp = DatafileProcessor(filename=test_data, verbose=True)