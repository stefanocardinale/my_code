import pymongo
import pandas as pd
import numpy as np
from bson.objectid import ObjectId
import os

mongo_db_key = 'mongodb://stca:P4yAP6ue4YTy@s-sdi-calc2-p.ssi.ad:27017/bifrost_upgrade_test?authSource=admin'
os.chdir("/Users/stefanocardinale/Documents/")

template = pd.read_csv("resfinder_template.csv", sep=';')

def get_run_list():
    connection = pymongo.MongoClient(mongo_db_key)
    DB = "bifrost_prod"
    db = connection[DB]

    # Fastest.
    runs = list(db.runs.find({}, #{"type": "routine"}, #Leave in routine
                                {"samples._id": 1}))
    return runs

def get_amr(sample_ids):
    connection = pymongo.MongoClient(mongo_db_key)
    DB = "bifrost_prod"
    db = connection[DB]

    return list(db.sample_components.find({
        "sample._id": {"$in": list(map(lambda x: ObjectId(x), sample_ids))},
        "component.name": "min_read_check"
    }, {'sample.name': 1, 'summary': 1}))


query = get_run_list()

for w in range(len(query)):
    sample_ids = []
    for i in range(len(query[0]['samples'])):
        sample_ids.append(query[0]['samples'][i]['_id'])

    amr_query = get_amr(sample_ids)
    temp = []
    genes = []

    #print(query[0]['summary'])
    #print(len(query[0]['summary']['ariba_resfinder']))
    for n in range(len(amr_query[0]['summary']['ariba_resfinder'])):
        temp.append(amr_query[0]['summary']['ariba_resfinder'][n]['DATABASE'][13:])
        genes.append(amr_query[0]['summary']['ariba_resfinder'][n]['GENE'][:3])


    #print(genes)

    ind = pd.Index(list(template.iloc[0]))

    bb = dict(zip(temp, [temp.count(i) for i in temp]))
    bs = list(bb.keys())
    row = pd.DataFrame(0, index=range(1), columns=range(96))
    row.columns = template.columns
    row.iloc[0]['isolate'] = amr_query[0]['sample']['name']
    for i in range(len(bs)):
        row.iloc[0][row.columns.get_loc(bs[i])] = list(bb.values())[i]

    bg = dict(zip(genes, [genes.count(i) for i in genes]))
    bgs = list(bg.keys())
    for i in range(len(bg)):
        row.iloc[0][ind.get_loc(bgs[i])] = list(bg.values())[i]

    template = pd.concat([template, row], ignore_index= True)
#template.to_csv('test.csv', index=False)
