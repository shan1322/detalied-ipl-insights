import json
import os


def load_json(file_name, directory):
    path = os.path.join(directory, file_name)
    try:
        f = open(path)
        data = json.load(f)
        return data
    except Exception as e:
        print(e)

        return {}


def dump_file(file_name, directory, dict_):
    path = os.path.join(directory, file_name)
    with open(path, "w") as outfile:
        json.dump(dict_, outfile)
