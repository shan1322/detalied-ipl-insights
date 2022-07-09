import json
import os


def load_json(file_name, directory):
    path = os.path.join(directory, file_name)
    try:
        f = open(path)
        data = json.load(f)
        return data
    except:
        return {}
