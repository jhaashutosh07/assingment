# Validate data against schema
def validate_schema(data, schema):
    for i, rec in enumerate(data["records"]):
        for col in schema.get("columns", []):
            cn, ct = col["name"], col["type"]
            if cn not in rec:
                raise ValueError(f"Record {i}: missing column '{cn}'")
            v = rec[cn]
            if ct == "integer":
                try: int(v)
                except (ValueError, TypeError): raise ValueError(f"Record {i}: '{cn}' not integer")
            elif ct == "float":
                try: float(v)
                except (ValueError, TypeError): raise ValueError(f"Record {i}: '{cn}' not float")
            elif ct == "string":
                if not isinstance(v, str): raise ValueError(f"Record {i}: '{cn}' not string")
