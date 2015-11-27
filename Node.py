class Node(object):
	'''
	Implementation of a trie (prefix tree). 
	Adapted from:
	<http://stackoverflow.com/questions/55210/algorithm-to-generate-anagrams/1924561#1924561>
	'''
	def __init__(self, word='', final=False, depth=0, column=None):
		self.word = word
		self.final = final
		self.depth = depth
		self.children = {}
		self.column = column
	def add(self, words=None, keys=None):
		node = self
		column_idx = 0
		for index, word in enumerate(words):
			if word not in node.children:
				node.children[word] = Node(word=word, final=index==len(words)-1, depth=index+1, column=keys[column_idx])				
			node = node.children[word]
			column_idx += 1

def in_trie(trie=None, words=None, verbose=False):
	current_trie = trie
	for word in words:
		word = str(word).strip().lower()
		if verbose:
			print word, current_trie.children.keys()
			print word in current_trie.children.keys()
			print current_trie.children[word], '\n'
		if word in current_trie.children:
			current_trie = current_trie.children[word]
		else:
			if verbose:
				print word, current_trie.children.keys()
				print word in current_trie.children.keys()
				print current_trie.children[word], '\n'
			return False
	else:
		if len(current_trie.children.keys()) == 0:
			return True
		else:
			if verbose:
				print word, current_trie.children.keys()
				print word in current_trie.children.keys()
				print current_trie.children[word], '\n'
			return False


def load_dictionary(dictionary=None, keys=None):
	trie = Node()
	words = [words.strip().lower() for words in dictionary]
	trie.add(words, keys)
	return trie

def load_dataframe(dataframe=None, verbose=False):
	trie = Node()
	for index, row in dataframe.iterrows():
		row = [str(words).strip().lower() for words in row]
		if verbose:
			print 'row: ', row		
		trie.add(row, dataframe.keys())
	return trie

def run():
	print 'Loading word list.'
	words = load_dictionary(dictionary=['Max', 'Payne', '32', '1234'], keys=['first', 'last', 'age', 'phone'])
	print words.children    

if __name__ == '__main__':
	run()