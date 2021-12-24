import json

def check_config(data):
    if not 'connections' in data:
        raise Exception("`connections' field missing from config")
    elif not isinstance(data['connections'], list):
        raise Exception("`connections' field is not an array")

def load(path):
    with open(path, 'r') as fd:
        data = json.load(fd)
        check_config(data)
        return data
