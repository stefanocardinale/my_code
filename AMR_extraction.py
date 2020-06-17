import pymongo
import pandas as pd
import numpy as np
from bson.objectid import ObjectId
import os
from progress.bar import Bar

mongo_db_key = 'mongodb://stca:P4yAP6ue4YTy@s-sdi-calc2-p.ssi.ad:27017/bifrost_upgrade_test?authSource=admin'
os.chdir("/Users/stefanocardinale/Documents/SSI/PROJECTS/AMR")
template = pd.read_csv("resfinder_template.csv", sep=';')


def get_run_list():
    connection = pymongo.MongoClient(mongo_db_key)
    DB = "bifrost_prod"
    db = connection[DB]

    # Fastest.
    runs = list(db.runs.find({}, #{"type": "routine"}, #Leave in routine
                             {"samples.name": 1}))
    return runs

def get_amr(sample_names):
    connection = pymongo.MongoClient(mongo_db_key)
    DB = "bifrost_prod"
    db = connection[DB]

    return list(db.sample_components.find({
        "sample.name": {"$in": sample_names},
        "component.name": "ariba_resfinder"
    }, {'sample.name': 1, 'summary.ariba_resfinder': 1}))


query = get_run_list()

sample_names = []
for w in range(len(query)):

    for i in range(len(query[w]['samples'])):
        sample_names.append(query[w]['samples'][i]['name'])

with Bar('Processing', max=len(sample_names), suffix='%(percent)d%%') as bar:


    for m in range(len(sample_names)):
        bar.next()

        amr_query = get_amr([sample_names[m]])
        #print("The sample name is: {}".format(sample_names[m]))
        #print("The length is: {}".format(len(amr_query)))

        if len(amr_query) > 1:
            x = 0
            temp = []
            genes = []
            bs = []
            bgs = []

            row = pd.DataFrame(0, index=range(1), columns=range(96))
            row.columns = template.columns

            #print(amr_query[x])
            row['isolate'] = amr_query[x]['sample']['name']
            #print("The sample name is: {}".format(amr_query[x]['sample']['name']))
            matchinfo_short = ""

            if 'summary' in amr_query[x]:

                #print("The length is: {}".format(len(amr_query[x]['summary']['ariba_resfinder'])))
                for n in range(len(amr_query[x]['summary']['ariba_resfinder'])):
                    if 'DATABASE' in amr_query[x]['summary']['ariba_resfinder'][n]:
                        temp.append(amr_query[x]['summary']['ariba_resfinder'][n]['DATABASE'][13:])
                    if 'GENE' in amr_query[x]['summary']['ariba_resfinder'][n]:
                        genes.append(amr_query[x]['summary']['ariba_resfinder'][n]['GENE'][:3])
                        matchinfo_short = matchinfo_short + amr_query[x]['summary']['ariba_resfinder'][n]['GENE'] + "_" + amr_query[x]['summary']['ariba_resfinder'][n]['ACCESSION'] + "_%cov:" + amr_query[x]['summary']['ariba_resfinder'][n]['%COVERAGE'][:3] + "_%id:" + amr_query[x]['summary']['ariba_resfinder'][n]['%IDENTITY'][:3] + ";"

                bb = dict(zip(temp, [temp.count(i) for i in temp]))
                bs = list(bb.keys())

                for i in range(len(bs)):
                    row[bs[i]] = list(bb.values())[i]

                bg = dict(zip(genes, [genes.count(i) for i in genes]))
                bgs = list(bg.keys())
                for i in range(len(bg)):
                    row[bgs[i]] = list(bg.values())[i]

            x = 1
            if 'summary' in amr_query[x]:

                for n in range(len(amr_query[x]['summary']['ariba_resfinder'])):
                    if 'GENE' in amr_query[x]['summary']['ariba_resfinder'][n]:
                        matchinfo_short = matchinfo_short + "redo_" + amr_query[x]['summary']['ariba_resfinder'][n]['GENE'] + "_" + amr_query[x]['summary']['ariba_resfinder'][n]['ACCESSION'] + "_%cov:" + amr_query[x]['summary']['ariba_resfinder'][n]['%COVERAGE'][:3] + "_%id:" + amr_query[x]['summary']['ariba_resfinder'][n]['%IDENTITY'][:3] + ";"


            row['matchinfo'] = matchinfo_short
            row['Redone'] = 'yes'
            #template = pd.concat([template, row], ignore_index=True, sort=False)

        else:
            x = 0
            temp = []
            genes = []
            bs = []
            bgs = []

            row = pd.DataFrame(0, index=range(1), columns=range(96))
            row.columns = template.columns

            #print(amr_query[x])
            row['isolate'] = amr_query[x]['sample']['name']
            #print("The sample name is: {}".format(amr_query[x]['sample']['name']))
            matchinfo_short = ""

            if 'summary' in amr_query[x]:

                #print("The length is: {}".format(len(amr_query[x]['summary']['ariba_resfinder'])))
                for n in range(len(amr_query[x]['summary']['ariba_resfinder'])):
                    if 'DATABASE' in amr_query[x]['summary']['ariba_resfinder'][n]:
                        temp.append(amr_query[x]['summary']['ariba_resfinder'][n]['DATABASE'][13:])
                    if 'GENE' in amr_query[x]['summary']['ariba_resfinder'][n]:
                        genes.append(amr_query[x]['summary']['ariba_resfinder'][n]['GENE'][:3])
                        matchinfo_short = matchinfo_short + amr_query[x]['summary']['ariba_resfinder'][n]['GENE'] + "_" + amr_query[x]['summary']['ariba_resfinder'][n]['ACCESSION'] + "_%cov:" + amr_query[x]['summary']['ariba_resfinder'][n]['%COVERAGE'][:3] + "_%id:" + amr_query[x]['summary']['ariba_resfinder'][n]['%IDENTITY'][:3] + ";"


                bb = dict(zip(temp, [temp.count(i) for i in temp]))
                bs = list(bb.keys())

                for i in range(len(bs)):
                    row[bs[i]] = list(bb.values())[i]

                bg = dict(zip(genes, [genes.count(i) for i in genes]))
                bgs = list(bg.keys())
                for i in range(len(bg)):
                    row[bgs[i]] = list(bg.values())[i]

            row['matchinfo'] = matchinfo_short
            row['Redone'] = 'no'
            #template = pd.concat([template, row], ignore_index=True, sort=False)

        if not os.path.isfile('amr_genes.csv'):
            template = pd.concat([template, row], ignore_index=True, sort=False)
            template.to_csv('amr_genes.csv', header='column_names', index=False)
        else:
            row.to_csv('amr_genes.csv', mode= 'a', header=False, index=False)

#template.to_csv('amr_genes.csv', index=False)
#samples.to_csv('samples.csv', index=False)
