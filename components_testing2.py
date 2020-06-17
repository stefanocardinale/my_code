import components.import_data as import_data
from components import html_components as hc
import pymongo
import components.mongo_interface as mongo_interface
import os
import re
from bson.objectid import ObjectId
KEY = "BIFROST_DB_KEY"
import pandas as pd
pd.set_option('display.max_columns', None)

mongo_db_key = 'mongodb://stca:P4yAP6ue4YTy@s-sdi-calc2-p.ssi.ad:27017/bifrost_upgrade_test?authSource=admin'

def get_samples_id(species_source, run_name):
    connection = pymongo.MongoClient(mongo_db_key)
    DB = "bifrost_prod"
    db = connection[DB]

    if species_source == "provided":
        spe_field = "properties.sample_info.summary.provided_species"
    else:
        spe_field = "properties.species_detection.summary.detected_species"

        run = db.runs.find_one(
            {"name": run_name},
            {
                "_id": 0,
                "samples._id": 1
            }
        )
        if run is None:
            run_samples = []
        else:
            run_samples = run["samples"]
        sample_ids = [s["_id"] for s in run_samples]


    return sample_ids

def filter_all(species=None, species_source=None, group=None,
               qc_list=None, run_names=None, sample_ids=None,
               sample_names=None,
               pagination=None,
               projection=None):
    if sample_ids is None:
        query_result = mongo_interface.filter(
            run_names=run_names, species=species,
            species_source=species_source, group=group,
            qc_list=qc_list,
            sample_names=sample_names,
            pagination=pagination,
            projection=projection)
    else:
        query_result = filter(
            samples=sample_ids, pagination=pagination,
            projection=projection)
    return pd.io.json.json_normalize(query_result)

def filter(run_names=None,
           species=None, species_source="species", group=None,
           qc_list=None, samples=None, pagination=None,
           sample_names=None,
           projection=None):
    if species_source == "provided":
        spe_field = "properties.sample_info.summary.provided_species"
    elif species_source == "detected":
        spe_field = "properties.species_detection.summary.detected_species"
    else:
        spe_field = "properties.species_detection.summary.species"
    connection = pymongo.MongoClient(mongo_db_key)
    DB = "bifrost_prod"
    db = connection[DB]

    query = []
    sample_set = set()
    if sample_names is not None and len(sample_names) != 0:
        sample_names_query = []
        for s_n in sample_names:
            if s_n.startswith("/") and s_n.endswith("/"):
                sample_names_query.append(re.compile(s_n[1:-1]))
            else:
                sample_names_query.append(s_n)
        query.append({"name": {"$in": sample_names_query}})
    if samples is not None and len(samples) != 0:
        sample_set = {ObjectId(id) for id in samples}
        query.append({"_id": {"$in": list(sample_set)}})
    if run_names is not None and len(run_names) != 0:
        runs = list(db.runs.find(
            {"name": {"$in": run_names}},
            {
                "_id": 0,
                "samples._id": 1
            }
        ))
        if runs is None:
            run_sample_set = set()
        else:
            run_sample_set = {s["_id"] for run in runs for s in run['samples']}

        if len(sample_set):
            inter = run_sample_set.intersect(sample_set)
            query.append({"_id": {"$in": list(inter)}})
        else:
            query.append({"_id": {"$in": list(run_sample_set)}})
    if species is not None and len(species) != 0:

        if "Not classified" in species:
            query.append({"$or":
                [
                    {spe_field: None},
                    {spe_field: {"$in": species}},
                    {spe_field: {"$exists": False}}
                ]
            })
        else:
            query.append({spe_field: {"$in": species}})
    if group is not None and len(group) != 0:
        if "Not defined" in group:
            query.append({"$or":
                [
                    {"properties.sample_info.summary.group": None},
                    {"properties.sample_info.summary.group": {"$in": group}},
                    {"properties.sample_info.summary.group": {"$exists": False}}
                ]
            })
        else:
            query.append(
                {"properties.sample_info.summary.group": {"$in": group}})

    if pagination is not None:
        p_limit = pagination['page_size']
        p_skip = pagination['page_size'] * pagination['current_page']
    else:
        p_limit = 1000
        p_skip = 0

    skip_limit_steps = [
        {"$skip": p_skip}, {"$limit": p_limit}
    ]

    qc_query = filter_qc(qc_list)

    if len(query) == 0:
        if qc_query is None:
            match_query = {}
        else:
            match_query = qc_query["$match"]
    else:
        if qc_query is None:
            match_query = {"$and": query}
        else:
            match_query = {"$and": query + qc_query["$match"]["$and"]}
    query_result = list(db.samples.find(
        match_query, projection).sort([('name', pymongo.ASCENDING)]).skip(p_skip).limit(p_limit))

    return query_result

def filter_qc(qc_list):
    if qc_list is None or len(qc_list) == 0:
        return None
    qc_query = []
    for elem in qc_list:
        if elem == "Not checked":
            qc_query.append({"$and": [
                {"properties.datafiles.summary.paired_reads": {"$exists": True}},
                {"properties.stamper.summary.stamp.value": {"$exists": False}}
            ]})
        elif elem == "core facility":
            qc_query.append({"$or": [
                {"properties.datafiles.summary.paired_reads": {"$exists": False}},
                {"properties.stamper.summary.stamp.value": "core facility"}
            ]
            })
        else:
            qc_query.append({"properties.stamper.summary.stamp.value": elem})
    return {"$match": {"$and": qc_query}}

def get_run(run_name):
    connection = pymongo.MongoClient(mongo_db_key)
    DB = "bifrost_prod"
    db = connection[DB]

    return db.runs.find_one({"name": run_name})

def get_mlst(sample_ids):
    connection = pymongo.MongoClient(mongo_db_key)
    DB = "bifrost_prod"
    db = connection[DB]

    return list(db.sample_components.find({
        "sample._id": {"$in": list(map(lambda x: ObjectId(x), sample_ids))},
        "component.name": "ariba_mlst"
    },{'results.ariba_mlst/mlst_report_tsv.values': 1}))

sample_ids = get_samples_id("not defined", '200226_NB551234_0217_N_WGS_314_AH2M3CAFX2')
samples = filter_all(sample_ids=sample_ids)
samples = pd.DataFrame(samples)

#print(sample_ids)

# for col in samples.columns:
#     print(col)

#run = get_run('200226_NB551234_0217_N_WGS_314_AH2M3CAFX2')

#run_name = {'label':'run_name','value': run['name']}
#test = pd.DataFrame(run_name)
#print(run)

COLUMNS = ['name', 'sample_sheet.run_name']

table = samples.loc[samples['properties.detected_species'] == 'Yersinia enterocolitica'][COLUMNS]
#print(table)
#print(table['_id'][0])
sample_ids_subset =  samples.loc[samples['properties.detected_species'] == 'Yersinia enterocolitica']['_id']
#print(sample_ids_subset)

components = get_mlst(sample_ids_subset)
print(components)
values = [1]*len(components)

for n in range(len(components)):
    values[n] = components[n]['results']['ariba_mlst/mlst_report_tsv']['values'][0]['ST']

table['ST'] = values

table.to_csv(r'/Users/stefanocardinale/Documents/SSI/PROJECTS/yersinia_200226.csv', index= False)