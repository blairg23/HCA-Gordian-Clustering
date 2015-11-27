class Node(object):
	'''
	Implementation of a trie (prefix tree). 
	Adapted from:
	<http://stackoverflow.com/questions/55210/algorithm-to-generate-anagrams/1924561#1924561>
	'''
	def __init__(self, word='', final=False, depth=0):
		self.word = word
		self.final = final
		self.depth = depth
		self.children = {}
	def add(self, words):
		_end = '_end_'
		node = self
		for index, word in enumerate(words):
			if word not in node.children:
				node.children[word] = Node(word=word, final=index==len(words)-1, depth=index+1)
			node = node.children[word]
		node = _end

def in_trie(trie=None, words=None, verbose=False):
	_end = '_end'
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
				print 'hi'
				print word, current_trie.children.keys()
				print word in current_trie.children.keys()
				print current_trie.children[word], '\n'
			return False


def load_dictionary(dictionary=None):
	trie = Node()
	words = [words.strip().lower() for words in dictionary]
	trie.add(words)
	return trie

def load_dataframe(dataframe=None):
	trie = Node()
	for index, row in dataframe.iterrows():
		row = [str(words).strip().lower() for words in row]
		trie.add(row)
	return trie

def run():
	print 'Loading word list.'
	words = load_dictionary(dictionary=['Max', 'Payne', '32', '1234'])
	print words.children    

if __name__ == '__main__':
	run()