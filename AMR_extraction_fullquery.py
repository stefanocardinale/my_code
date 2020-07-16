import pymongo
import pandas as pd
import numpy as np
from bson.objectid import ObjectId
import os
#from progress.bar import Bar

mongo_db_key = 'mongodb://stca:P4yAP6ue4YTy@s-sdi-calc2-p.ssi.ad:27017/bifrost_upgrade_test?authSource=admin'
os.chdir("/Users/stefanocardinale/Documents/")
template = pd.read_csv("resfinder_template.csv", sep=',')


def get_run_list():
    connection = pymongo.MongoClient(mongo_db_key)
    DB = "bifrost_prod"
    db = connection[DB]

    # Fastest.
    runs = list(db.runs.find({}, {"samples.name": 1}))
    return runs

def get_amr(sample_name):
    connection = pymongo.MongoClient(mongo_db_key)
    DB = "bifrost_prod"
    db = connection[DB]

    return list(db.sample_components.find({
        "sample.name": {"$in": sample_name},
        "component.name": "ariba_resfinder"
    }, {'sample.name': 1, 'summary.ariba_resfinder': 1}))

def get_samples():
    connection = pymongo.MongoClient(mongo_db_key)
    
    DB = "bifrost_prod"
    db = connection[DB]

    samples = list(db.samples.find({'sample_sheet.provided_species': 'Salmonella'}, {"name": 1}))

    if "_id" in samples:
        samples["_id"] = samples["_id"].astype(str)
    
    return samples

#query = get_run_list()
samples = get_samples()

sample_names = []
for sample in samples:
    sample_names.append(sample['name'])

amr_datas = get_amr(['GMI117-002-DNA'])

test = []
for w in range(1):

    temp = []
    genes = []
    bs = []
    bgs = []

    row = pd.DataFrame(0, index=range(1), columns=range(95))
    row.columns = template.columns

    amr_data = amr_datas[w]
    
    row['isolate'] = amr_data['sample']['name']
    #print("The sample name is: {}".format(amr_data[x]['sample']['name']))
    matchinfo_short = ""

    if 'summary' in amr_data:
        my_set = []
        col_names = []
        #print("The length is: {}".format(len(amr_data[x]['summary']['ariba_resfinder'])))
        for n in range(len(amr_data['summary']['ariba_resfinder'])):
            if 'DATABASE' in amr_data['summary']['ariba_resfinder'][n]:
                temp.append(amr_data['summary']['ariba_resfinder'][n]['DATABASE'][13:])
            if 'GENE' in amr_data['summary']['ariba_resfinder'][n]:
                genes.append(amr_data['summary']['ariba_resfinder'][n]['GENE'][:3])
                coverage = amr_data['summary']['ariba_resfinder'][n]['%COVERAGE'].split(".")[0]
                #print(coverage)
                if int(coverage) > 90:
                    matchinfo_short = matchinfo_short + amr_data['summary']['ariba_resfinder'][n]['DATABASE'][13:] + "_" + amr_data['summary']['ariba_resfinder'][n]['GENE'] + "_" + amr_data['summary']['ariba_resfinder'][n]['ACCESSION'] + "_%cov:" + coverage + "_%id:" + amr_data['summary']['ariba_resfinder'][n]['%IDENTITY'][:3] + ";"
                
                database = amr_data['summary']['ariba_resfinder'][n]['DATABASE'][13:] + "_" + str(n)
                gene = amr_data['summary']['ariba_resfinder'][n]['GENE'] + "_" + str(n)
                col_names.append('database' + "_" + str(n))
                col_names.append('gene' + "_" + str(n))
                my_set.append(database)
                my_set.append(gene)
        
        #new_data = pd.DataFrame(my_set, index=range(1), columns=range(len(my_set)))
        #new_data.columns = col_names
        new_data = pd.DataFrame(data = [my_set], columns=col_names)
        #print(new_data)

        bb = dict(zip(temp, [temp.count(i) for i in temp]))
        bs = list(bb.keys())

        for i in range(len(bs)):
            row[bs[i]] = list(bb.values())[i]

        bg = dict(zip(genes, [genes.count(i) for i in genes]))
        bgs = list(bg.keys())
        for i in range(len(bg)):
            row[bgs[i]] = list(bg.values())[i]

    row['matchinfo'] = matchinfo_short
    row = pd.concat([row,new_data], axis=1)


    if not os.path.isfile('amr_genes.csv'):
        #template = pd.concat([template, row], ignore_index=True, sort=False)
        row.to_csv('amr_genes.csv', header=row.columns, index=False)
    else:
        row.to_csv('amr_genes.csv', mode= 'a', header=False, index=False)

