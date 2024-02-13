import pandas as pd

def check_na(value, type):
    if pd.isna(value):
        if type == "fraction":
            value = [None, None]
        else:
            value = None
    elif type =="interval" and value == "-":
        value = None
    else:
        if type == "int":
            value = int(value)
        elif type == "percentage":
            value = float(value.strip("%")) / 100.0
        elif type =="fraction":
            value = value.split("/")
        elif type == "float":
            value = float(value)
    return value