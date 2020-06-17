import os
import csv
from bifrostlib import datahandling

#run_file = "bifrost/run.yaml" 
samples_file = "samples_to_test.txt"

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

#run = datahandling.load_run(run_file)
keys = {}
summary = {}
samples = []
for i in range(len(test_samples)):
    samples.append(test_samples[i]['sample'])

output = []
for sample in samples:
    file = sample + "/" + sample + "__kma_pointmutations.yaml"
    file2 = sample + "/" + sample + "__pointfinder.yaml"
    file3 = sample + "/" + sample + "__ariba_resfinder.yaml"
    data = datahandling.load_sample_component(file)
    data2 = datahandling.load_sample_component(file2)
    data3 = datahandling.load_sample_component(file3)

    temp = {}
    values2 = {}
    if 'summary' in data:
        values = data['summary']['Point_mutations']
        print(values)
        if len(values) > 1:
            values2['Point_mutation'] = values[0]
            for i in range(1, len(values)):
                values2['Point_mutation_{}'.format(i)] = values[i]
            temp.update({**values2})
        else:
            temp.update({'Point_mutation': values[0]})
    
    print(temp)

    values2 = {}
    if 'results' in data2:
        if 'contigs_blastn_results_tsv' in data2['results']:
            if data2['results']['contigs_blastn_results_tsv']['values'] != []:
                values = data2['results']['contigs_blastn_results_tsv'].get('values', [{}])
                
                if len(values) > 1:
                    temp.update({**summary, **values[0]})
                    #print("There is one sample with {} pointfinder hit".format(len(values)))
                    for i in range(1,len(values)):
                        for k,v in values[i].items():
                            values2[k + "_{}".format(i)] = v
                        temp.update({**summary, **values2})
                       
                else:
                    temp.update({**summary, **values[0]})
            else:
                temp.update({**summary, **{'Amino acid change': '', 'Nucleotide change': '', 'Mutation': '', 'PMID': '', 'Resistance': ''}})
        else:
            temp.update({**summary, **{'Amino acid change': '', 'Nucleotide change': '', 'Mutation': '', 'PMID': '', 'Resistance': ''}})
    else:
            temp.update({**summary, **{'Amino acid change': '', 'Nucleotide change': '', 'Mutation': '', 'PMID': '', 'Resistance': ''}})


    temp2 = {}
    summary3 = {}
    summary4 = {}
    if 'summary' in data3:
        if data3['summary']['ariba_resfinder'] != []:
            summary2 = data3.get('summary', {}).get('ariba_resfinder', {})
            if len(summary2) > 1:
                #print("There is one sample with {} ariba_resfinder hit".format(len(summary2)))
                summary3 = {k: summary2[0][k] for k in ('%COVERAGE', '%IDENTITY', 'DATABASE', 'GENE')}
                temp2.update(summary3)

                for i in range(1,len(summary2)):
                    summary3 = {k: summary2[i][k] for k in ('%COVERAGE', '%IDENTITY', 'DATABASE', 'GENE')}
                    for k, v in summary3.items():
                        summary4[k + "_{}".format(i)] = v
                    temp2.update(summary4)
                    
            else:
                summary3 = {k: summary2[0][k] for k in ('%COVERAGE', '%IDENTITY', 'DATABASE', 'GENE')}
                temp2.update(summary3)

            temp.update({**temp2})
        else:
            temp2 = {'%COVERAGE':'', '%IDENTITY':'', 'DATABASE':'', 'GENE':''}
            temp.update({**temp2})
    else:
        temp2 = {'%COVERAGE':'', '%IDENTITY':'', 'DATABASE':'', 'GENE':''}
        temp.update({**temp2})

    #print(temp)
    output.append({**temp})
    keys.update({**temp})
    
    #print(keys)
    #print(output)
    #summary.update({'sample': str(data.get('sample', {}).get('name', {}))})

keys.update({'sample': 'sample'})

for i in range(len(output)):
    output[i].update({'sample': samples[i]})


csvfilename = 'kma_pointmutations.tsv'
with open(csvfilename, 'w') as output_file:
    dict_writer = csv.DictWriter(output_file, keys, delimiter="\t")
    dict_writer.writeheader()
    dict_writer.writerows(output)  