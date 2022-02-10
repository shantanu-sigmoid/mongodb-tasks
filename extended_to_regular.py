
import json
from bson import ObjectId

# Dict stores every path location as key which ends with key starts with "$" and its value as value
finder = {}
def printer(obj, loc):
    global finder
    for key, val in obj.items():
        if(type(val) == dict):
            printer(val, loc +"-"+ key)
        else:
            if(key.startswith("$")):
                finder[loc + "-" + key] = val

# obj must be a dictionary => json + single document { }
def convert(objType, val):
    if(objType == "$numberInt"):
        return f"int({val})"
    elif(objType == "$numberDouble"):
        return f"float({val})"
    elif(objType == "$oid"):
        return f"ObjectId('{val}')"
    else:
        return val

def exetended_json_to_regular(obj):
    global finder
    finder = {}
    printer(obj, "")
    for key in finder.keys():
        key_list = key.split("-")[1:]
        command = "obj"
        for gokey_list in key_list[:-1]:
            command += f"{[gokey_list]}"
        command += f"={convert(key_list[-1], finder[key])}"
        print(command)
        exec(command)
    return obj

with open("/Users/shantanu/Downloads/sample_mflix/movies.json") as f:
    for json_obj in f:                               # extracting every single json 
        if json_obj:
            my_dict = json.loads(json_obj)           # string to dictionary conversion of single json     
            obj = exetended_json_to_regular(my_dict) # obj now is converted to regular json