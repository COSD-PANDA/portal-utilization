
# coding: utf-8

# In[1]:


import pandas as pd
import json
import numpy as np
from ua_parser import user_agent_parser
import re
from datetime import datetime, timedelta
fy = 22


print("Reading in Google Analytics events")

ga = pd.read_csv("/Users/andrellbower/Desktop/portal_events_datasd.csv",
low_memory=False,
dtype='str')

ga_dates = ga['date'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d') if not pd.isna(x) else np.nan)

ga_this_fy = ga.loc[(ga_dates >= f'20{fy-1}-07-01')&
(ga_dates < f'20{fy}-07-01')]


print("Processing json files to get dataset info")
dataset_urls = pd.read_json(f'files/fy{fy}/datasets.json')
with open(f"files/fy{fy}/data.json", "r") as read_file:
    datasets = json.load(read_file)

resource_files = pd.json_normalize(data=datasets['dataset'], 
                                record_path='distribution', 
                                meta=['title'],
                                meta_prefix='page_'
                               )

resource_files.columns = ['file_name',
'type',
'download_url',
'media_type',
'format',
'title']

dataset_pages_join = pd.merge(resource_files[['download_url',
                                              'format',
                                              'file_name',
                                              'title']],
                              dataset_urls[['title',
                                            'url']],
                              left_on="title",
                              right_on="title",
                              how="left")



dataset_pages_join = dataset_pages_join.rename({'title':'dataset_name'})

final_dataset_pages = dataset_pages_join.loc[dataset_pages_join['download_url'].str.startswith('https://seshat.datasd.org/')].copy()
page_paths = final_dataset_pages['url'].apply(lambda x: x.replace('/datasets',''))
event_labels = final_dataset_pages['download_url']

final_dataset_pages = final_dataset_pages.assign(event_label=event_labels,
page_path_level2=page_paths)


print("Merging dataset info with GA results")
ga_pagepath = pd.merge(ga_this_fy,final_dataset_pages[['event_label','page_path_level2']],on='event_label',how="left")

# Here's how you make the old file lookup
# Uncomment the following 5 lines
#missing_keys = ga_pagepath.loc[ga_pagepath['page_path_level2'].isna(),'event_label'].tolist()
#missing_keys_set = set(missing_keys)
#old_file_lookup = pd.DataFrame(missing_keys_set,columns=['event_label'])
#old_file_lookup['page_path_level2'] = ''
#old_file_lookup.to_csv(f'files/fy{fy}/old-file-lookup.csv',index=False)

## Use datasets.json to fill in the page path

# In[16]:


# In the case where a filename was changed or a file was removed
# The file to the page path
# To make old file lookup, comment the following two lines
print("Looking up changes to datasets to fix for consistency")
old_links = pd.read_csv(f'files/fy{fy}/old-file-lookup.csv')

missing_pp2 = ga_pagepath.loc[ga_pagepath['page_path_level2'].isna()]
populated_pp2 = ga_pagepath.loc[~ga_pagepath['page_path_level2'].isna()]

missing_merge = pd.merge(missing_pp2,
  old_links,
  on='event_label',
  how='left'
  )

ga_final_pagepath = pd.concat([
  populated_pp2,
  missing_merge],
  ignore_index=True,
  sort=False,
  )

ga_final_pagepath.loc[ga_final_pagepath['page_path_level2'].isna(),
'page_path_level2'] = ga_final_pagepath.loc[ga_final_pagepath['page_path_level2'].isna(),
'page_path_level2_y']

ga_final_pagepath = ga_final_pagepath.drop(columns=['page_path_level2_x','page_path_level2_y'])
print("Writing dataset downloads")

ga_final_pagepath.to_csv(f'files/fy{fy}/dataset_downloads.csv',index=False)
#ga_pagepath.to_csv(f'files/fy{fy}/dataset_downloads.csv',index=False)

# In[22]:

print("Writing page links")
final_dataset_pages.groupby('page_path_level2').size().reset_index(name='counts').to_csv(f'files/fy{fy}/dataset_page_links.csv',index=False)
