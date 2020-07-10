import argparse
import pymongo
import subprocess as sp
import json

mongo_db_key = 'mongodb://localhost:27017/bifrost_prod'

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--i", help="selects a .tsv file", required=True)
    
    return parser.parse_args()

def get_connection():
    connection = pymongo.MongoClient(mongo_db_key)
    return connection

def get_samples_name(run_name):
    connection = get_connection()

    DB = "migration"
    db = connection[DB]


    run = db.runs.find_one(
        {"name": run_name},
        {
            "_id": 0,
            "samples.name": 1
        }
    )
    if run is None:
        run_samples = []
    else:
        run_samples = run["samples"]
    sample_names = [str(s["name"]) for s in run_samples]


    return sample_names

def get_sample(names):
    connection = get_connection()
    
    DB = "bifrost_upgrade_test"
    db = connection[DB]

    samples = list(db.samples.find(
        {'name': {'$in': names}}
    ))

    if "_id" in samples:
        samples["_id"] = samples["_id"].astype(str)
    
    return samples

def extract_tsv(file):
    with open(file, "r") as tsv_file:
                values = []
                headers = None
                for line in tsv_file:
                    line = line.strip() #This line removes leading and trailing white spaces
                    if headers is None:
                        headers = line.split('\t') #if there is no header, just return the line as header (?)
                    else:
                        row = line.split('\t')
                        values.append(dict(zip(headers, row))) 

    file_content = values
    return file_content
                    
sample_names = get_samples_name("190417_NB551234_0125_N_WGS_217_AHH3GNAFXY")

samples = get_sample(sample_names)
#samples = pd.DataFrame(samples)
#samples = samples.to_dict("records")
print(samples[0])

with open('./migration_samples.json', 'w') as fout:
    json.dump(samples , fout)

