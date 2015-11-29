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
		def find_trie_height(trie=None):
			if len(trie.children.keys()) == 0:
				return 0
			max_size = 0
			for child in trie.children.values():
				size = find_trie_height(child)
				if size > max_size:
					max_size = size
			return max_size + 1 # To account for start at 0

		def find_combinations(branch=None, k=None):
			'''
			Find all combinations of a particular branch, given a length k.
			'''
			if k==0: yield []
			elif k==len(branch): yield branch
			else:
				for i in xrange(len(branch)):
					for cc in find_combinations(branch[i+1:], k-1):
						yield [branch[i]] + cc
		
		def find_paths(trie, path={}):
			'''
			Finds all paths of a given trie.
			'''
			# If we're not at the root:
			if trie.column != None:
				path[trie.column] = trie.word # Add the new node to our list of nodes in this branch

			if trie.final == True:
				yield path
			else:
				for child in trie.children:
					for p in find_paths(trie=trie.children[child], path=path):
						yield p


		def find_uniques(trie=None, k=None, non_uniques=None, uniques=None, verbose=False):
			'''
			Traverse a given trie node for the minimum uniques.
			'''	
			all_paths = find_paths(trie=trie)

			if verbose:
				print '[All Paths in Trie: ]'
				for paths in all_paths:
					print paths	
			
			# Find all combinations of the set of keys:
			verbose=False
			frequency_counter = {}
			for path in all_paths: # For all the branches in the trie,
				if verbose:
					print '[Path: ]'
					print path, '\n'
					print '[K: {k}]\n'.format(k=k)
					print '[Combinations: ]'
				print '[K: {k}]\n'.format(k=k)
				combinations = find_combinations(branch=path.keys(), k=k) # Get all combinations of size k
				for key_combination in combinations:										
					if verbose:						
						print key_combination, '\n'						
						print '[All values exist in the path: ]', all([path[key] in path.values() for key in key_combination]) 					
					if all([path[key] in path.values() for key in key_combination]):	# If all values from the key exist in the combination
						combination_dict = {}
						for key in key_combination:							
							combination_dict[key] = path[key]
						if verbose:
							print '\n[Combination Dictionary: ]'
							print combination_dict, '\n'

						if str(combination_dict) in frequency_counter:		# and if we've already added this combination to the frequency counter,
							frequency_counter[str(combination_dict)] += 1 	# then increment it.
						else: 												# Otherwise, 
							frequency_counter[str(combination_dict)] = 1 	# instantiate it with a value of 1
					
					for key in key_combination:
						if verbose:
							print '[Column Key Combination: ]', path[key]							
					if verbose:
						print '\n[Frequency Counter: ]'
						print frequency_counter, '\n'

			# Check if any non-uniques emerged:
			for key, value in frequency_counter.iteritems():
				if key != '{}':
					candidate_key = eval(key).keys()
					verbose=True
					if verbose:
						print '[Candidate Key: ] {candidate_key}\n'.format(candidate_key=candidate_key)
					if value > 1: # If we found a non-unique
						if verbose:
							print '[Non-Unique Found: ]', key
						# print frequency_counter
						# print eval(key)
						if candidate_key not in non_uniques:  # If we didn't already add the candidate,							
							non_uniques.append(candidate_key) # add it to non-uniques.
							if verbose:
								print '[Added to Non-Uniques: ]', candidate_key
								print '[Non-Uniques: ] {non_uniques}\n'.format(non_uniques=non_uniques)
							if candidate_key in uniques:      # But if it's in the uniques,
								uniques.remove(candidate_key) # remove it from uniques.
								print '[Removed from Uniques: ]', candidate_key
								print '[Uniques: ] {uniques}\n'.format(uniques=uniques)
								
					else:		
						if candidate_key not in uniques and candidate_key not in non_uniques: # If the candidate isn't already in uniques or non-uniques, 							
							uniques.append(candidate_key)		# add it to uniques
							if verbose:
								print '[Unique Found: ]', key
								print '[Added to Uniques: ]', candidate_key
								print '[Uniques: ] {uniques}\n'.format(uniques=uniques)							
						# elif candidate_key in uniques:    		# however, if it is in uniques already,
						# 	uniques.remove(candidate_key) 		# then it's not unique, so remove it
						# 	if verbose:
						# 		print '[Removed from Uniques: ]', candidate_key
						# 		print '[Uniques: ] {uniques}\n'.format(uniques=uniques)							
						# 	non_uniques.append(candidate_key)	# and add to non-uniques
						# 	if verbose:
						# 		print '[Added to Non-Uniques: ]', candidate_key
						# 		print '[Non-Uniques: ] {non_uniques}\n'.format(non_uniques=non_uniques)							
						else:
							if verbose:
								print '[Already in Non-Uniques]'
						# contrarily, if it's already in non-uniques, no need to do anything.

						if k > 0: # If we haven't reached the end, decrement k and try again:
							non_uniques, uniques = find_uniques(trie=trie, k=k-1, non_uniques=non_uniques, uniques=uniques)
						else:
							return non_uniques, uniques


			

			# for child in trie.children:				
			# 	# If the child object has more than one child of its own,
			# 	# then this means the path has diverged and it's a non-unique,
			# 	# along with every superset:
			# 	if len(trie.children[child].children.keys()) > 1:
			# 		# If it's already in the minimal uniques, we need to remove it:
			# 		if columns in min_uniques:
			# 			min_uniques.remove(columns)
			# 		# Add the child to the list of columns discovered so far:
			# 		columns[trie.children[child].column] = child
			# 	 	# And add the column combination list to the 
			# 	 	# maximal non-unique list:
			# 	 	max_non_uniques.append(columns)
			# 	 	if verbose:
			# 			print '-------------------------'
			# 			print '[Maximal Non-Uniques: ]'
			# 			print max_non_uniques, '\n'
			# 			print '[Minimal Uniques: ]'
			# 			print min_uniques, '\n'
			# 			print '[Columns: ]'
			# 			print columns, '\n'
			# 			print '[Next Root: ]'
			# 			print trie.children[child].word, '\n'
			# 		# finally, reset the columns, since we've found the maximal non-unique list:
			# 		columns = {}
			# 		# and add the new root to the list of column combinations:
			# 		columns[trie.children[child].column] = child
			# 		return find_uniques(trie=trie.children[child], max_non_uniques=max_non_uniques, min_uniques=min_uniques, columns=columns)				
			# 	# Otherwise, add child to the column combination list and the
			# 	# columns to the minimal unique list:
			# 	else:
			# 		columns[trie.children[child].column] = child
			# 		# If it's not a duplicate:
			# 		if columns not in min_uniques:
			# 			# add to our minial uniques:
			# 			min_uniques.append(columns)
			# 		else: # otherwise,
			# 			# Remove it from our minimal uniques:
			# 			min_uniques.remove(columns)
			# 			# and add it to our maximal non-uniques:
			# 			max_non_uniques.append(columns)		
			# 		if verbose:
			# 			print '-------------------------'
			# 			print '[Maximal Non-Uniques: ]'
			# 			print max_non_uniques, '\n'
			# 			print '[Minimal Uniques: ]'
			# 			print min_uniques, '\n'
			# 			print '[Columns: ]'
			# 			print columns, '\n'
			# 			print '[Next Root: ]'
			# 			print trie.children[child].word, '\n'
			# 		return find_uniques(trie=trie.children[child], max_non_uniques=max_non_uniques, min_uniques=min_uniques, columns=columns)							
			# 	columns = {} # Once we're done, restart the process by resetting the column combinations
			# 	# and recursing:
			# 	return find_uniques(trie=trie.children[child], max_non_uniques=max_non_uniques, min_uniques=min_uniques, columns=columns)				
			# # When we're finally complete, return teh maximal non-uniques and minimal uniques:	
			return non_uniques, uniques

		trie = self.create_trie(dataframe=dataframe, verbose=verbose)
		trie_height = find_trie_height(trie=trie)
		print '[Trie Height: ]', trie_height
		# Just to ensure we created a path for each row:
		for index, row in dataframe.iterrows():
			if verbose:
				print '[Results: ]'
				print in_trie(trie=trie, words=row, verbose=False), '\n'

		
		non_uniques = []
		uniques = []

		# Recursively traverse trie to determine non-uniques:
		non_uniques, uniques = find_uniques(trie=trie, k=trie_height, non_uniques=non_uniques, uniques=uniques)

		verbose = True
		if verbose:
			print '[Non-Uniques: ]'
			print non_uniques, '\n'
			print '[Uniques: ]'
			print uniques, '\n'
		
		# Get the minimal unique column combinations:		
		mymin = min(map(len,uniques))
		min_uniques = [candidate for candidate in uniques if len(candidate)==mymin] # Minimum unique candidates

		# Get the maximal non-unique column combination:
		mymax = max(map(len,non_uniques))
		max_non_unique = [candidate for candidate in non_uniques if len(candidate)==mymax] # Maximum unique candidates
		

		if verbose:
			print '[Maximal Non-Unique: ]'
			print max_non_unique, '\n'
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

	verbose = False
	dfp = DatafileProcessor(filename=test_csv, algorithm='Gordian', verbose=verbose)

	if verbose:
		print '[Results: ]'
		print dfp.results
		print dfp.results.children
		# for child in dfp.results.children:
		# 	print dfp.results.children[child].children
	