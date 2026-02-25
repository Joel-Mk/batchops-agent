import os
import yaml
import pandas as pd

def run_ingestion(provider):

    # Load provider config
    config_path = os.path.join("configs", f"{provider}.yaml")

    if not os.path.exists(config_path):
        raise ValueError(f"Provider config not found: {provider}")

    with open(config_path) as f:
        config = yaml.safe_load(f)

    data_path = config["data_path"]
    entities = config["entities"]

    data = {}

    for entity_name, entity_info in entities.items():
        file_path = os.path.join(data_path, entity_info["file"])

        if not os.path.exists(file_path):
            raise ValueError(f"Missing file: {file_path}")

        df = pd.read_csv(file_path)
        data[entity_name] = df

        print(f"Ingested {entity_name}: {len(df)} rows")

    return data