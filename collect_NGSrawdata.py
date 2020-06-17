import argparse
import pymongo
import subprocess as sp

mongo_db_key = 'mongodb://localhost:27017/bifrost_prod'

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--i", help="selects a .tsv file", required=True)
    
    return parser.parse_args()

def get_connection():
    connection = pymongo.MongoClient(mongo_db_key)
    return connection

def get_samples_id(run_name):
    connection = get_connection()

    DB = "bifrost_prod"
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
    
    DB = "bifrost_prod"
    db = connection[DB]

    sample = db.samples.find(
        {'name': {'$in': names}},
        {'reads': 1}
    )

    return sample

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
                    
#sample_names = get_samples_id("samples")
args = parse_args()
file = args.i

content = extract_tsv(file)
names = []
for i in range(len(content)):
    names.append(content[i].get('sample', {}))

print(names)
samples =list(get_sample(names))
print(samples)

for i in range(len(samples)):
    R1 = ''
    R2 = ''
    if 'reads' in samples[i]:
        if 'R1' in samples[i]['reads']:
                R1 = samples[i]['reads']['R1']
        if 'R2' in samples[i]['reads']:
                R2 = samples[i]['reads']['R2']
    
    sp.run(["cp", R1, "./files" ])
    sp.run(["cp", R2, "./files" ])
