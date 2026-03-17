class DAG:
    def __init__(self):
        self.nodes = set()
        self.edges = {}

    def add_node(self, n):
        self.nodes.add(n)
        if n not in self.edges:
            self.edges[n] = []

    def add_edge(self, n, dep):
        self.add_node(n)
        self.add_node(dep)
        if dep not in self.edges[n]:
            self.edges[n].append(dep)

    def get_dependencies(self, n):
        return self.edges.get(n, [])

    # Check for cycles in the graph
    def has_cycle(self):
        visited = set()
        def dfs(n):
            if n in visited:
                return True
            visited.add(n)
            for d in self.get_dependencies(n):
                if dfs(d):
                    return True
            return False
        for n in self.nodes:
            if dfs(n):
                return True
        return False

    # Sort nodes topologically
    def topological_sort(self):
        result = []
        visited = set()
        def visit(n):
            if n in visited:
                return
            visited.add(n)
            result.append(n)
            for d in self.get_dependencies(n):
                visit(d)
        for n in sorted(self.nodes):
            visit(n)
        return result
