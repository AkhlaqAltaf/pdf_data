from datetime import date, datetime

def make_serializable(data):
    serializable_data = {}
    for k, v in data.items():
        if isinstance(v, (datetime, date)):
            serializable_data[k] = v.strftime("%Y-%m-%d")
        else:
            serializable_data[k] = v
    return serializable_data
