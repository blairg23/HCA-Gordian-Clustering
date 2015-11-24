# -*- coding: utf-8 -*-
'''
Name: datafile_processor.py
Author: Blair Gemmer
Version: 20151123

Description: 

1. Able to open datafiles, which have been encoded in json or csv format.

2. Performs clustering techniques on the data, using the Gordian and HCA clustering methods.

3. Outputs the maximal uniqueness
'''
# import numpy
# import pandas

class DatafileProcessor():
	def __init__(self):
		pass

	def create_trie(self, *words):
		'''
		Creates a prefix tree (or trie) from the given list of words. 
		Borrowed from http://stackoverflow.com/questions/11015320/how-to-create-a-trie-in-python
		'''
		_end = '_end_'

		root = dict()
		for word in words:
			current_dict = root
			for letter in word:
				current_dict = current_dict.setdefault(letter, {})
			current_dict[_end] = _end
		return root

	def in_trie(self, trie, word):
		'''
		Traverses a prefix tree (or trie) and returns the boolean value if the given
		word is in the given trie.
		Borrowed from http://stackoverflow.com/questions/11015320/how-to-create-a-trie-in-python
		'''		
		_end = '_end_'

		current_dict = trie
		for letter in word:
			if letter in current_dict:
				current_dict = current_dict[letter]
			else:
				return False
		else:
			if _end in current_dict:
				return True
			else:
				return False

	def HCA(self):
		pass

	def Gordian(self):
		pass

	def HCA_Gordian(self):
		pass