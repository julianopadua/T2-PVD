import os
import yaml

def load_config():
    """Carrega config.yaml do diretório raiz e resolve caminhos relativos."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "..", "config.yaml")

    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    paths = {
        "script_dir": script_dir,
        "data_raw": os.path.join(script_dir, config["paths"]["data_raw"]),
        "data_processed": os.path.join(script_dir, config["paths"]["data_processed"]),
        "images": os.path.join(script_dir, config["paths"]["images"]),
        "report": os.path.join(script_dir, config["paths"]["report"]),
        "addons": os.path.join(script_dir, config["paths"]["addons"]),
    }

    return paths, config
