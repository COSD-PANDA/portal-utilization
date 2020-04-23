
# coding: utf-8

# In[1]:


import keen
import pandas as pd
import json
from pandas.io.json import json_normalize
import numpy as np
from ua_parser import user_agent_parser
import re
from datetime import datetime, timedelta
fy = 19

# In[2]:


keen.project_id = "58b083098db53dfda8a88bcf"
keen.read_key = "B857F220E3CC4D775E1BBDD56F428903B1A147AE129C80D9020ADC43CBC32FFF1880586A273E876D4566B03C5042BA706F239E75A958A70E5943581845EC115FFC80294614FF8876DAB9BE2B58B2325875330D879D63FDF69CB1514185911CE5"
keen.master_key = "C1D6737D0D885EAC071B7BBC73AE2DC34A65095D1BFE261C59195FABC3863388"


# In[3]:


# This function gets a readable user agent family from the user agent string
def parse_user_agent(ua_string):
    jupyter_re = re.compile(r'[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}')
    if 'Drupal' in ua_string:
        return 'Drupal'
    elif jupyter_re.match(ua_string):
        return 'Jupyter kernel'
    else:
        parsed_string = user_agent_parser.Parse(ua_string)
        return parsed_string["user_agent"]["family"]


# In[4]:


# This function splits the date into year and month for grouping later
def get_date_cols(row):
    date = row['timeframe.start'].split('T')[0]
    date_as_dt = pd.to_datetime(date,errors='coerce')
    date_month = date_as_dt.month
    date_year = date_as_dt.year
    return date_as_dt, date_month, date_year


# In[5]:


# This function filters out files we don't need
def filter_files(df):
    print(df.shape)
    file_suffix = df['log.key'].apply(lambda x: x.split(".")[-1])
    df = df.assign(file_type=file_suffix)
    filter1 = df.loc[(df.file_type == 'csv') |
                                       (df.file_type == 'zip') |
                                       (df.file_type == 'geojson') |
                                       (df.file_type == 'pbf') |
                                       (df.file_type == 'topojson')
                                      ]
    #filter2 = filter1.loc[(~filter1['log.key'].str.startswith('city_docs/'))]
    filter3 = filter1.loc[filter1['result']>0]
    df_filter = filter3.copy()
    print(df_filter.shape)
    return df_filter


# In[6]:

ua_remove = ['ia_archiver',
'BingPreview',
'Qwantify',
'Apple Mail',
'Facebook',
'Outlook',
'Facebook Messenger',
'Yahoo! Slurp'
]

# This function removes user agents that are bots
def remove_bots(df):
    print(df.shape)
    crawl_filter = df.loc[(~df['user_agent_family'].str.contains('crawl',flags=re.IGNORECASE, regex=True))]
    bot_filter = crawl_filter.loc[(~crawl_filter['user_agent_family'].str.contains('bot',flags=re.IGNORECASE, regex=True))]
    final = bot_filter.loc[(~bot_filter['user_agent_family'].str.contains('spider',flags=re.IGNORECASE, regex=True))]
    print(final.shape)
    
    return final


# In[7]:


# This function applies all of the above functions
def process_results(df):
    
    print("Filtering on file type")
    df_keep = filter_files(df)
    
    print("Parsing user agent")
    agents = df_keep['log.user_agent'].unique()
    df_agents = pd.DataFrame(agents,columns=['user_agent'])
    agent_parse = df_agents['user_agent'].apply(lambda x: parse_user_agent(x))
    df_agents = df_agents.assign(user_agent_family=agent_parse)
    df_w_agents = pd.merge(df_keep,df_agents,how="left",left_on='log.user_agent',right_on='user_agent')
    
    print("Removing bots")
    df_no_bots = remove_bots(df_w_agents)
    
    print("Breaking out date")
    dates = df_no_bots.apply(get_date_cols,axis=1)
    new_dates = dates.apply(pd.Series)
    df_final = df_no_bots.assign(date_full=new_dates[0],date_month=new_dates[1],date_year=new_dates[2])
    return df_final


# In[9]:


# Here, we are making the request for data to Keen

months_tf = [{"start":f"20{fy-1}-07-01T00:00:00.000Z","end":f"20{fy-1}-08-01T00:00:00.000Z"},
             {"start":f"20{fy-1}-08-01T00:00:00.000Z","end":f"20{fy-1}-09-01T00:00:00.000Z"},
             {"start":f"20{fy-1}-09-01T00:00:00.000Z","end":f"20{fy-1}-10-01T00:00:00.000Z"},
             {"start":f"20{fy-1}-10-01T00:00:00.000Z","end":f"20{fy-1}-11-01T00:00:00.000Z"},
             {"start":f"20{fy-1}-11-01T00:00:00.000Z","end":f"20{fy-1}-12-01T00:00:00.000Z"},
             {"start":f"20{fy-1}-12-01T00:00:00.000Z","end":f"20{fy}-01-01T00:00:00.000Z"},
             {"start":f"20{fy}-01-01T00:00:00.000Z","end":f"20{fy}-02-01T00:00:00.000Z"},
             {"start":f"20{fy}-02-01T00:00:00.000Z","end":f"20{fy}-03-01T00:00:00.000Z"},
             {"start":f"20{fy}-03-01T00:00:00.000Z","end":f"20{fy}-04-01T00:00:00.000Z"},
             {"start":f"20{fy}-04-01T00:00:00.000Z","end":f"20{fy}-05-01T00:00:00.000Z"},
             {"start":f"20{fy}-05-01T00:00:00.000Z","end":f"20{fy}-06-01T00:00:00.000Z"},
             {"start":f"20{fy}-06-01T00:00:00.000Z","end":f"20{fy}-07-01T00:00:00.000Z"},
            ]

dfs_year = []

print("looping through months")
for index, month in enumerate(months_tf):
    downloads = keen.count("s3_seshat.datasd.org_logs",
                                timeframe=month,
                                group_by=["log.key","log.user_agent"],
                                interval="daily",
                                filters=[{'property_name':'log.operation',
                                   'operator':'contains',
                                   'property_value':'GET.OBJECT'}])
    df = pd.io.json.json_normalize(downloads, 'value', [['timeframe','end'],['timeframe','start']])
    dfs_year.append(df)
    print(f"appended month {index}")


# In[10]:

print("Concatting months")
df_year_all = pd.concat(dfs_year,ignore_index=True)


df_year_process = process_results(df_year_all)


df_year_final = df_year_process.drop(columns=['log.user_agent','timeframe.end','timeframe.start','user_agent'])


# We sometimes need to manipulate the data at the page level, so we need to add page paths
# Page paths match Google Analytics

# In[14]:


# Using the json of published datasets from the last publication of seaboard
# Or from a time period that makes sense

print("Processing json files to get dataset info")
dataset_urls = pd.read_json('datasets.json')
with open("data.json", "r") as read_file:
    datasets = json.load(read_file)

resource_files = json_normalize(data=datasets['dataset'], 
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

final_dataset_pages = dataset_pages_join.loc[dataset_pages_join['download_url'].str.startswith('http://seshat.datasd.org/')].copy()

final_dataset_pages['log.key'] = final_dataset_pages['download_url'].apply(lambda x: x.replace('http://seshat.datasd.org/',''))

final_dataset_pages['page_path_2'] = final_dataset_pages['url'].apply(lambda x: x.replace('/datasets',''))

# In[15]:

print("Merging dataset info with Keen results")
keen_pagepath = pd.merge(df_year_final,final_dataset_pages[['log.key','page_path_2']],left_on='log.key',right_on='log.key',how="left")


# In[16]:


# In the case where a filename was changed, probably to better follow standards, we can manually match 
# The file to the page path
# print("Looking up changes to datasets to fix for consistency")
# page_path_old_links = pd.read_csv('old-file-lookup.csv')
# old_links = page_path_old_links['old_links'].to_list()
# old_links_page = page_path_old_links['page_path_2'].to_list()


# In[17]:


def get_page(row):
    if row['page_path_2'] == '':
        if row['log.key'] in old_links:
            pos = old_links.index(row['log.key'])
            page_value = old_links_page[pos]
            return page_value
        else:
            return ''
    else:
        return row['page_path_2']


# In[18]:


#keen_final_pagepath = keen_pagepath.apply(get_page,axis=1)


# In[19]:


#keen_pagepath.loc[:,'page_path_2'] = keen_final_pagepath


print("Writing dataset downloads")

keen_pagepath.to_csv(f'dataset_downloads_fy{fy}.csv',index=False)


# In[22]:

print("Writing page links")
final_dataset_pages.groupby('page_path_2').size().reset_index(name='counts').to_csv('dataset_page_links.csv',index=False)
