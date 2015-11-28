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

	def create_trie(self, dataframe=None, verbose=False):	
		_end = '_end_'

		if verbose:
			print '[Creating a prefix tree...]\n'
			print '[Keys: ]', '\n'
			print ','.join(dataframe.keys())
			# for key in keys:
			# 	print key
			print '\n'		

		trie = load_dataframe(dataframe=dataframe, verbose=verbose)
		
		return trie

	def Gordian(self, dataframe=None, verbose=False):
		'''
		Performs the Gordian clustering technique to find minimum unique column combinations.
		'''
		if verbose:
			print '[Performing Gordian analysis on dataframe...]\n'
		def find_uniques(trie=None, max_non_uniques=None, min_uniques=None, columns={}):
			'''
			Traverse a given trie node for the minimum uniques.
			'''			
			for child in trie.children:				
				# If the child object has more than one child of its own,
				# then this means the path has diverged and it's a non-unique,
				# along with every superset:
				if len(trie.children[child].children.keys()) > 1:
					# If it's already in the minimal uniques, we need to remove it:
					if columns in min_uniques:
						min_uniques.remove(columns)
					# Add the child to the list of columns discovered so far:
					columns[trie.children[child].column] = child
				 	# And add the column combination list to the 
				 	# maximal non-unique list:
				 	max_non_uniques.append(columns)
				 	if verbose:
						print '-------------------------'
						print '[Maximal Non-Uniques: ]'
						print max_non_uniques, '\n'
						print '[Minimal Uniques: ]'
						print min_uniques, '\n'
						print '[Columns: ]'
						print columns, '\n'
						print '[Next Root: ]'
						print trie.children[child].word, '\n'
					# finally, reset the columns, since we've found the maximal non-unique list:
					columns = {}
					# and add the new root to the list of column combinations:
					columns[trie.children[child].column] = child
					return find_uniques(trie=trie.children[child], max_non_uniques=max_non_uniques, min_uniques=min_uniques, columns=columns)				
				# Otherwise, add child to the column combination list and the
				# columns to the minimal unique list:
				else:
					columns[trie.children[child].column] = child
					# If it's not a duplicate:
					if columns not in min_uniques:
						# add to our minial uniques:
						min_uniques.append(columns)
					else: # otherwise,
						# Remove it from our minimal uniques:
						min_uniques.remove(columns)
						# and add it to our maximal non-uniques:
						max_non_uniques.append(columns)		
					if verbose:
						print '-------------------------'
						print '[Maximal Non-Uniques: ]'
						print max_non_uniques, '\n'
						print '[Minimal Uniques: ]'
						print min_uniques, '\n'
						print '[Columns: ]'
						print columns, '\n'
						print '[Next Root: ]'
						print trie.children[child].word, '\n'
					return find_uniques(trie=trie.children[child], max_non_uniques=max_non_uniques, min_uniques=min_uniques, columns=columns)							
				columns = {} # Once we're done, restart the process by resetting the column combinations
				# and recursing:
				return find_uniques(trie=trie.children[child], max_non_uniques=max_non_uniques, min_uniques=min_uniques, columns=columns)				
			# When we're finally complete, return teh maximal non-uniques and minimal uniques:	
			return max_non_uniques, min_uniques

		trie = self.create_trie(dataframe=dataframe, verbose=verbose)
		# Just to ensure we created a path for each row:
		for index, row in dataframe.iterrows():
			if verbose:
				print '[Results: ]'
				print in_trie(trie=trie, words=row, verbose=False), '\n'

		# Maximum non-unique candidates:
		max_non_uniques = []
		# Minimum unique candidates:
		min_uniques = []

		# Recursively traverse trie to determine non-uniques:
		max_non_uniques, min_uniques = find_uniques(trie=trie, max_non_uniques=max_non_uniques, min_uniques=min_uniques)
		if verbose:
			print '[Maximal Non-Uniques: ]'
			print max_non_uniques, '\n'
			print '[Minimal Uniques: ]'
			print min_uniques, '\n'
		# print '[ROOT]'
		# print trie.word
		# print trie.column
		# print '[CHILDREN]'
		# for child in trie.children:			
		# 	print '[CHILD: ]', child#trie.children[child].word
		# # 	print trie.children[child].column
		#  	print '[GRANDCHILDREN]'
		# 	for grandchild in trie.children[child].children:
		# 		print '[GRANDCHILD: ]', grandchild
		# 		print '[GREATGRANDCHILDREN]'
		# 		for greatgrandchild in trie.children[child].children[grandchild].children:
		# 			print '[GREATGRANDCHILD: ]', greatgrandchild
		# 			print '[GREATGREATGRANDCHILDREN]'
		# 		for greatgreatgrandchild in	trie.children[child].children[grandchild].children[greatgrandchild].children:
		# 			print '[GREATGREATGRANDCHILD: ]', greatgreatgrandchild
		# 		print trie.children[child].children[grandchild].word
		# 		print trie.children[child].children[grandchild].column
		# print '\n'
		return trie


	def HCA(self, dataframe=None, verbose=False):
		'''
		Performs the HCA clustering technique to find minimum unique column combinations.
		'''
		if verbose:
			print '[Performing HCA analysis on dataframe...]\n'
		pass


	def HCA_Gordian(self):
		pass

if __name__ == '__main__':
	test_data = os.path.join('data', 'data.json')
	test_techcrunch = os.path.join('data', 'techcrunch.csv')
	test_json = os.path.join('data', 'json', 'test_data.json')
	test_csv = os.path.join('data', 'csv', 'test_data.csv')

	verbose = True
	dfp = DatafileProcessor(filename=test_json, algorithm='Gordian', verbose=verbose)

	if verbose:
		print '[Results: ]'
		print dfp.results
		print dfp.results.children
		# for child in dfp.results.children:
		# 	print dfp.results.children[child].children
	