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
			frequency_counter = {}
			for path in all_paths: # For all the branches in the trie,			
				if verbose:
					print '[Path: ]'
					print path, '\n'
					print '[K: {k}]\n'.format(k=k)
					print '[Combinations: ]'
				#print '[K: {k}]\n'.format(k=k)
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
					if verbose:
						print '[Candidate Key: ] {candidate_key}\n'.format(candidate_key=candidate_key)
					if value > 1: # If we found a non-unique
						if verbose:
							print '[Non-Unique Found: ]', key
						if candidate_key not in non_uniques:  # If we didn't already add the candidate,							
							non_uniques.append(candidate_key) # add it to non-uniques.
							if verbose:
								print '[Added to Non-Uniques: ]', candidate_key
								print '[Non-Uniques: ] {non_uniques}\n'.format(non_uniques=non_uniques)
							if candidate_key in uniques:      # But if it's in the uniques,
								uniques.remove(candidate_key) # remove it from uniques.
								if verbose:
									print '[Removed from Uniques: ]', candidate_key
									print '[Uniques: ] {uniques}\n'.format(uniques=uniques)								
					else:		
						if candidate_key not in uniques and candidate_key not in non_uniques: # If the candidate isn't already in uniques or non-uniques, 							
							uniques.append(candidate_key)		# add it to uniques
							if verbose:
								print '[Unique Found: ]', key
								print '[Added to Uniques: ]', candidate_key
								print '[Uniques: ] {uniques}\n'.format(uniques=uniques)							
						else: # contrarily, if it's already in non-uniques, no need to do anything.
							if verbose:
								print '[Already in Non-Uniques]'

						if k > 0: # If we haven't reached the end, decrement k and try again:
							non_uniques, uniques = find_uniques(trie=trie, k=k-1, non_uniques=non_uniques, uniques=uniques, verbose=verbose)
						else:
							return non_uniques, uniques
			return non_uniques, uniques

		trie = self.create_trie(dataframe=dataframe, verbose=verbose)
		trie_height = find_trie_height(trie=trie)
		if verbose:
			print '[Trie Height: ]', trie_height
		# Just to ensure we created a path for each row:
		for index, row in dataframe.iterrows():
			if verbose:
				print '[Results: ]'
				print in_trie(trie=trie, words=row, verbose=False), '\n'		

		non_uniques = []
		uniques = []

		# Recursively traverse trie to determine non-uniques:
		non_uniques, uniques = find_uniques(trie=trie, k=trie_height, non_uniques=non_uniques, uniques=uniques, verbose=False)

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

		return max_non_unique, min_uniques


	def HCA(self, dataframe=None, verbose=False):
		'''
		Performs the HCA clustering technique to find minimum unique column combinations.
		Following Algorithm 2: "HCA Algorithm" from "Advancing the Discovery of Unique Column Combinations" precisely.
		'''
		def candidate_generation(non_uniques=None):
			'''
			Performs efficient candidate generation for HCA analysis, given a list of non-uniques.
			Following Algorithm 1: "CandidateGen" from "Advancing the Discovery of Unique Column Combinations" precisely.
			'''
			def is_not_minimal(candidate=None):
				'''
				Not sure what the algorithm is expecting here.
				'''
				return True

			candidates = []
			non_unique_1 = []
			non_unique_2 = []
			k = len(non_uniques)
			for i in range(k):
				for j in range(i+1, k):
					non_unique_1.append(non_uniques[i])
					non_unique_2.append(non_uniques[j])
					if non_unique_1[0:k-2] == non_unique_2[0:k-2]:
						candidate = non_unique_1[0:k-2]
					if non_unique_1[k-1] < non_unique_2[k-1]:
						candidate[k-1] = non_unique_1[k-1]
						candidate[k] = non_unique_2[k-1]
					else:
						candidate[k-1] = non_unique_2[k-1]
						candidate[k] = non_unique_1[k-1]
					if is_not_minimal(candidate=candidate):
						continue
					candidates.append(candidate)
			return candidates

		def is_unique(dataframe=None, current_column=None):
			'''
			Returns False if the given column is in the list of columns in the given dataframe; True if not.			
			'''
			# First, remove the current column from the list of columns to prevent false positive:
			columns = [key for key in dataframe.keys()]			
			# print 'columns:', columns
			# print 'current_column:', current_column
			columns.remove(current_column)
			# print 'columns:', columns
			# Then iterate through the columns to determine uniqueness:
			for column in columns:
				if (dataframe[current_column] == dataframe[column]).all(): # If we found a match,
					return False # The column isn't unique.
			return True # Otherwise, it is.

		def get_histogram(dataframe=None, current_column=None):
			'''
			Creates a histogram of all the values in the given column of data.
			'''	
			histogram = {}		
			for row in dataframe[current_column]:
				if row in histogram.keys():
					histogram[row] += 1
				else:
					histogram[row] = 1
			return histogram

		def is_futile(candidate=None):
			'''
			Not sure what the algorithm is expecting here.
			'''
			return True

		def pruned_by_histogram(candidate=None):
			'''
			Again, the paper does not implicitly state what this is supposed to do.
			'''
			return True

		def retrieve_fds():
			'''
			Not sure what they want here.
			'''
			pass

		if verbose:
			print '[Performing HCA analysis on dataframe...]\n'
		columns = dataframe.keys() # Get m columns
		non_unique_columns = []
		uniques = []
		column_histograms = {}
		for current_column in columns:
			if is_unique(dataframe=dataframe, current_column=current_column):
				uniques.append(dataframe[current_column])
			else:
				non_unique_columns.append(dataframe[current_column])
				column_histograms[current_column] = get_histogram(dataframe=dataframe, current_column=current_column)
			current_non_uniques = non_unique_columns			
			for k in range(2, len(non_unique_columns)):
				k_candidates = candidate_generation(non_uniques=current_non_uniques)
				current_non_uniques = []
				for candidate in k_candidates:
					if is_futile(candidate=candidate):
						pass
					if pruned_by_histogram(candidate=candidate):
						current_non_uniques.append(candidate)
						pass
					if is_unique(dataframe=dataframe, current_column=candidate):
						uniques.append(candidate)
						for k_candidate in k_candidates:
							uniques.append(k_candidate)
					else:
						current_non_uniques.append(candidate)
						for k_candidate in k_candidates:
							current_non_uniques.append(k_candidate)
						column_histograms[candidate] = get_histogram(dataframe=dataframe, current_column=candidate)
				if k == 2:
					retrieve_fds()
		if verbose:
			print '[Uniques: ]'
			print uniques
			print '[Non_uniques: ]'
			print non_unique_columns

		return uniques

	def HCA_Gordian(self):
		pass

if __name__ == '__main__':
	test_data = os.path.join('data', 'data.json')
	test_techcrunch = os.path.join('data', 'techcrunch.csv')
	test_json = os.path.join('data', 'json', 'test_data.json')
	test_csv = os.path.join('data', 'csv', 'test_data.csv')

	verbose = False
	algorithm = 'gordian' # or Gordian
	dfp = DatafileProcessor(filename=test_json, algorithm=algorithm, verbose=verbose)

	if algorithm.lower() == 'gordian':
		max_non_unique, min_uniques = dfp.results

		verbose = True
		if verbose:
			print '[Maximal Non-Unique: ]'
			print max_non_unique, '\n'
			print '[Minimal Uniques: ]'
			print min_uniques, '\n'

	elif algorithm.lower() == 'hca':
		verbose=True
		if verbose:
			print '[Unique Columns: ]'
			for result in dfp.results:
				print result.name