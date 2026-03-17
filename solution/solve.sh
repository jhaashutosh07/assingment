#!/bin/bash
set -e
cd /app

python3 << 'PYFIX'
# Fix dag.py: cycle detection + topological sort
with open('dag.py', 'r') as f:
    c = f.read()

c = c.replace(
    '''    def has_cycle(self):
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
        return False''',
    '''    def has_cycle(self):
        visited = set()
        rec_stack = set()
        def dfs(n):
            visited.add(n)
            rec_stack.add(n)
            for d in self.get_dependencies(n):
                if d not in visited:
                    if dfs(d):
                        return True
                elif d in rec_stack:
                    return True
            rec_stack.discard(n)
            return False
        for n in self.nodes:
            if n not in visited:
                if dfs(n):
                    return True
        return False''')

c = c.replace(
    '''    def topological_sort(self):
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
        return result''',
    '''    def topological_sort(self):
        result = []
        visited = set()
        def visit(n):
            if n in visited:
                return
            visited.add(n)
            for d in self.get_dependencies(n):
                visit(d)
            result.append(n)
        for n in sorted(self.nodes):
            visit(n)
        return result''')

with open('dag.py', 'w') as f:
    f.write(c)
print("Fixed dag.py")

# Fix config.py: greedy regex
with open('config.py', 'r') as f:
    c = f.read()
c = c.replace("pattern = r'\\$\\{(.+)\\}'", "pattern = r'\\$\\{([^}]+)\\}'")
with open('config.py', 'w') as f:
    f.write(c)
print("Fixed config.py")

# Fix orchestrator.py: deepcopy + failure propagation
with open('orchestrator.py', 'r') as f:
    c = f.read()

c = c.replace('import os, json, sys', 'import os, json, sys, copy')

c = c.replace(
    '            input_data = self.data_store[task["input"]]\n            self.data_store[task["output"]] = transform_data(input_data, task["operations"])',
    '            input_data = copy.deepcopy(self.data_store[task["input"]])\n            self.data_store[task["output"]] = transform_data(input_data, task["operations"])')

c = c.replace(
    '                self.results[name] = {"status": "skipped", "reason": "dependency_failed"}\n                continue',
    '                self.results[name] = {"status": "skipped", "reason": "dependency_failed"}\n                failed_tasks.add(name)\n                continue')

with open('orchestrator.py', 'w') as f:
    f.write(c)
print("Fixed orchestrator.py")
PYFIX

echo "All fixes applied."
python3 orchestrator.py
