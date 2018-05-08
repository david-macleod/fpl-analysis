import json 

def load_config(config_path):
    """ Load config json into a dict """
    with open(config_path, 'r') as config_file:
        config = json.load(config_file)
     
    return config