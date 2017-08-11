import json

with open("config/demroes_config.json", "r") as fp:
    configs = json.load(fp)

unitDict = {
        'battery_voltage':'V'
        }

for building_name, url in configs.items():
    
