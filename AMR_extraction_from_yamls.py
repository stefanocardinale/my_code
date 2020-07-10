import os
import csv
import pandas as pd

from bifrostlib import datahandling

os.chdir("/Users/stefanocardinale/Documents/SSI/PROJECTS/AMR")
template = pd.read_csv("resfinder_template.csv", sep=';')
samples_file = "samples_to_extract.txt"

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
    
    return values

test_samples = extract_tsv(samples_file)


samples = []
for i in range(len(test_samples)):
    samples.append(test_samples[i]['sample'])


output = []
for sample in samples:
    temp = []
    genes = []
    bs = []
    bgs = []

    file = sample + "/" + sample + "__pointfinder.yaml"
    file2 = sample + "/" + sample + "__ariba_resfinder.yaml"
    data = datahandling.load_sample_component(file)
    data2 = datahandling.load_sample_component(file2)

    matchinfo_short = ""
    row = pd.DataFrame(0, index=range(1), columns=range(96))
    row.columns = template.columns

    row['isolate'] = sample

    if 'summary' in data2:
        for n in range(len(data2['summary']['ariba_resfinder'])):
            if 'DATABASE' in data2['summary']['ariba_resfinder'][n]:
                temp.append(data2['summary']['ariba_resfinder'][n]['DATABASE'][13:])
            if 'GENE' in data2['summary']['ariba_resfinder'][n]:
                genes.append(data2['summary']['ariba_resfinder'][n]['GENE'][:3])
                matchinfo_short = matchinfo_short + data2['summary']['ariba_resfinder'][n]['GENE'] + "_" + data2['summary']['ariba_resfinder'][n]['ACCESSION'] + "_%cov:" + data2['summary']['ariba_resfinder'][n]['%COVERAGE'][:3] + "_%id:" + data2['summary']['ariba_resfinder'][n]['%IDENTITY'][:3] + ";"

        bb = dict(zip(temp, [temp.count(i) for i in temp]))
        bs = list(bb.keys())
        for i in range(len(bs)):
            row[bs[i]] = list(bb.values())[i]

        bg = dict(zip(genes, [genes.count(i) for i in genes]))
        bgs = list(bg.keys())
        for i in range(len(bgs)):
            row[bgs[i]] = list(bg.values())[i]

        row['matchinfo'] = matchinfo_short

    if not os.path.isfile('amr_genes.csv'):
        template = pd.concat([template, row], ignore_index=True, sort=False)
        template.to_csv('amr_genes.csv', header='column_names', index=False)
    else:
        row.to_csv('amr_genes.csv', mode= 'a', header=False, index=False)

