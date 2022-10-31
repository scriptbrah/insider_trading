import json
import os 

try:
    with open('parameters.json', 'r') as file:
        data = json.load(file)
except:
    print("coud not load parameters.json")
    os.exit()


print(data["last_update"])
print(data["stocks"][1])