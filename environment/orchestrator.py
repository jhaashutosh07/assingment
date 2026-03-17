import os, json, sys
from dag import DAG
from config import ConfigLoader
from transforms import read_csv, clean_data, transform_data, merge_data
from validators import validate_schema

class PipelineOrchestrator:
    def __init__(self, config_path="pipeline_config.yaml"):
        self.config_loader = ConfigLoader(config_path)
        self.pipeline_config = self.config_loader.get_pipeline_config()
        self.dag = DAG()
        self.data_store = {}
        self.results = {}

    # Build the DAG from tasks
    def build_dag(self):
        for t in self.pipeline_config.get("tasks", []):
            self.dag.add_node(t["name"])
            for d in t.get("depends_on", []):
                self.dag.add_edge(t["name"], d)

    # Check for cycles
    def validate_dag(self):
        if self.dag.has_cycle():
            raise ValueError("Pipeline has circular dependencies!")

    # Execute the pipeline
    def execute(self):
        self.build_dag()
        self.validate_dag()
        order = self.dag.topological_sort()
        task_map = {t["name"]: t for t in self.pipeline_config["tasks"]}
        failed_tasks = set()
        for name in order:
            task = task_map[name]
            deps = task.get("depends_on", [])
            if any(d in failed_tasks for d in deps):
                self.results[name] = {"status": "skipped", "reason": "dependency_failed"}
                continue
            try:
                self._execute_task(task)
                self.results[name] = {"status": "success"}
            except Exception as e:
                self.results[name] = {"status": "failed", "error": str(e)}
                failed_tasks.add(name)
        self._generate_report()
        return self.results

    # Execute a single task
    def _execute_task(self, task):
        tt = task["type"]
        if tt == "read_csv":
            self.data_store[task["output"]] = read_csv(task["input"])
        elif tt == "clean_data":
            self.data_store[task["output"]] = clean_data(self.data_store[task["input"]], task["operations"])
        elif tt == "validate_schema":
            input_data = self.data_store[task["input"]]
            validate_schema(input_data, task["schema"])
            self.data_store[task["output"]] = input_data
        elif tt == "transform":
            input_data = self.data_store[task["input"]]
            self.data_store[task["output"]] = transform_data(input_data, task["operations"])
        elif tt == "merge":
            datasets = [self.data_store[n] for n in task["inputs"]]
            self.data_store[task["output"]] = merge_data(datasets)
        elif tt == "report":
            input_data = self.data_store[task["input"]]
            op = task["output_file"]
            os.makedirs(os.path.dirname(op), exist_ok=True)
            with open(op, "w") as f:
                json.dump(input_data, f, indent=2)

    # Generate execution report
    def _generate_report(self):
        r = {"pipeline": self.pipeline_config.get("name", ""), "results": self.results,
             "total": len(self.results),
             "ok": sum(1 for v in self.results.values() if v["status"] == "success"),
             "fail": sum(1 for v in self.results.values() if v["status"] == "failed"),
             "skip": sum(1 for v in self.results.values() if v["status"] == "skipped")}
        print(json.dumps(r, indent=2))
        return r

if __name__ == "__main__":
    o = PipelineOrchestrator()
    res = o.execute()
    if any(r["status"] == "failed" for r in res.values()):
        sys.exit(1)
