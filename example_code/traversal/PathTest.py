from itertools import chain, permutations
from unittest import TestCase

class PathTest(TestCase):
    def test_complete(self):
        # The maximal paths in a complete graph are the permutations
        # of the vertices.
        n = 8
        vertices = list(range(n))
        g = {v: vertices for v in vertices}
        found = sorted(chain.from_iterable(paths(g, i) for i in vertices))
        expected = map(list, permutations(vertices))
        for p, q in zip(found, expected):
            self.assertEqual(p, q)

    def test_binary(self):
        # Two alternatives at each of n places yields 2 ** n paths.
        n = 10
        g = {n * 2: [], n * 2 + 1: []}
        g.update({i: [i + 2, i + 3] for i in range(0, 2 * n, 2)})
        g.update({i: [i + 1, i + 2] for i in range(1, 2 * n, 2)})
        found = sorted(paths(g, 0))
        def expected():
            for i in range(2 ** n):
                p = []
                for j in reversed(range(0, 2 * n + 2, 2)):
                    p.append(j + i % 2)
                    i //= 2
                yield p[::-1]
        for p, q in zip(found, expected()):
            self.assertEqual(p, q)