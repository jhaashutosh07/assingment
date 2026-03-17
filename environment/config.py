import re
import yaml

class ConfigLoader:
    def __init__(self, config_path):
        with open(config_path) as f:
            self.raw_config = yaml.safe_load(f)
        self.variables = self.raw_config.get("variables", {})

    # Interpolate variables in text
    def interpolate(self, text, variables=None):
        if variables is None:
            variables = self.variables
        if not isinstance(text, str):
            return text
        pattern = r'\$\{(.+)\}'
        def replace(m):
            v = m.group(1)
            if v in variables:
                return str(variables[v])
            return m.group(0)
        return re.sub(pattern, replace, text)

    def get_pipeline_config(self):
        return self._interpolate_recursive(self.raw_config.get("pipeline", {}))

    # Recursively interpolate in nested structures
    def _interpolate_recursive(self, obj):
        if isinstance(obj, str):
            return self.interpolate(obj)
        elif isinstance(obj, dict):
            return {k: self._interpolate_recursive(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._interpolate_recursive(i) for i in obj]
        return obj
