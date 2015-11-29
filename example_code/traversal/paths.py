def paths(graph, v):
    """Generate the maximal cycle-free paths in graph starting at v.
    graph must be a mapping from vertices to collections of
    neighbouring vertices.

    >>> g = {1: [2, 3], 2: [3, 4], 3: [1], 4: []}
    >>> sorted(paths(g, 1))
    [[1, 2, 3], [1, 2, 4], [1, 3]]
    >>> sorted(paths(g, 3))
    [[3, 1, 2, 4]]

    """
    path = [v]                  # path traversed so far
    seen = {v}                  # set of vertices in path
    def search():
        dead_end = True
        for neighbour in graph[path[-1]]:
            if neighbour not in seen:
                dead_end = False
                seen.add(neighbour)
                path.append(neighbour)
                yield from search()
                path.pop()
                seen.remove(neighbour)
        if dead_end:
            yield list(path)
    yield from search()