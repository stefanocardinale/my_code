import pandas as pd
import os
import numpy as np

os.chdir("/Users/stefanocardinale/Documents/SSI/PROJECTS/AMR")

data = pd.read_csv('./AMRgene_results2.csv', sep=";", index_col='isolate', na_values=[''])
#print(data.head())
data['matchinfo'] = data.matchinfo.astype(str)

isolates = data.loc[~data.index.duplicated(keep='first')]
duplicates = data.loc[data.index.duplicated(keep='first')]
duplicates2 = duplicates.loc[~duplicates.index.duplicated(keep='last')]
duplicates2 = duplicates2.loc[duplicates2.index != 'Undetermined']

print(duplicates2.shape)

for i in range(len(duplicates2.index.values)):
    #print(duplicates2.index.values[i])
    #print(i)
    if duplicates2.loc[duplicates2.index.values[i], 'matchinfo'] != 'nan' and duplicates2.index.values[i] in isolates.index.values:
        #print(isolates.loc[duplicates.index.values[i], 'matchinfo'])
        #print(duplicates2.loc[duplicates2.index.values[i], 'matchinfo'])

        isolates.loc[duplicates2.index.values[i], 'matchinfo'] = isolates.loc[duplicates2.index.values[i], 'matchinfo'] + "redo_(" + duplicates2.loc[duplicates2.index.values[i], 'matchinfo'] + ")"
        #print(isolates.loc[duplicates2.index.values[i], 'matchinfo'])

duplicates2.to_csv("duplicates.csv", index=True)
#isolates.to_csv('clean_isolates.csv', index=True)

