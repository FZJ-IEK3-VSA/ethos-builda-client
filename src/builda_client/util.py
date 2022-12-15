import re
from pathlib import Path
from typing import Dict

import yaml
from shapely import wkt


def load_config() -> Dict:
    """Loads the config file.

    Returns:
        dict: The configuration.
    """
    project_dir = Path(__file__).resolve().parents[0]
    config_file_path = project_dir / "config.yml"
    with open(str(config_file_path), "r") as config_file:
        return yaml.safe_load(config_file)

    
def determine_nuts_query_param(nuts_lau_code: str) -> str:
    """Determines the correct query parameter based on the given NUTS or LAU code.

    Args:
        nuts_lau_code (str): The code for which to query.

    Raises:
        ValueError: If the code is invalid.

    Returns:
        str: The appropriate query parameter for the given code.
    """
    pattern = re.compile("^[A-Z]{2}[A-Z0-9]*$")
    if pattern.match(nuts_lau_code):
        # Probably NUTS code
        if len(nuts_lau_code) == 2:
            return "nuts0"
        if len(nuts_lau_code) == 3:
            return "nuts1"
        if len(nuts_lau_code) == 4:
            return "nuts2"
        if len(nuts_lau_code) == 5:
            return "nuts3"
        raise ValueError("NUTS region code too long.")

    # Maybe LAU code
    return "lau"


def ewkt_loads(x):
    try:
        wkt_str = x.split(";")[1]
        return wkt.loads(wkt_str)
    except Exception:
        return None