
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
import json
fy = 19



print("Reading GA datasets for portal analytics")
visits = pd.read_csv('http://seshat.datasd.org.s3.amazonaws.com/web_analytics/portal_pages_datasd.csv',parse_dates=['date'])



#df_year_process.groupby(['user_agent_family','log.key']).agg({'result':'sum'}).reset_index().to_csv('keen-output/dataset_ua_sum.csv',index=False)


# In[5]:


# Pull visits from the portal pages dataset
visits_pages = visits.loc[(visits['page_path_1'] == '/datasets/') 
& ((visits['date'] >= f'20{fy-1}-07-01')&(visits['date'] < f'20{fy}-07-01')),
['date','page_path_2','users','pageviews']].copy()

print(f"Filtering GA for fiscal year and datasets for a total of {visits_pages.shape[0]} records")


# In[6]:

print("Breaking out dates to month and year")
date_month_visits = visits_pages['date'].apply(lambda x: x.month)
date_year_visits = visits_pages['date'].apply(lambda x: x.year)
visits_pages = visits_pages.assign(date_month=date_month_visits,date_year=date_year_visits)
visits_pages['date'] = visits_pages['date'].astype(str)


# # Metrics to calculate
# 
# ## For the current fiscal year
# 
# * Total number of users
# * Total number of downloads, script, browser, and other
# * Gigabytes transferred (need a new Keen query)
# * Total Google Analytics events
# * Top downloads by page, script, browser, and other
# * Top visits by page

# In[7]:

# Get total number of users
total_users = visits_pages['users'].sum()
print(f"Total users is {total_users}")

# Get users per month
monthly_users = visits_pages.groupby(['date_month']).agg({'users':'sum'}).reset_index()


# In[8]:


# Get list of most visited pages
users_by_page = visits_pages.groupby(['page_path_2']).agg({'users': 'sum','pageviews':'sum'}).reset_index()


# In[9]:

print("Calculating monthly users by page and writing to csv")
visits_pages.groupby(['page_path_2','date_month']).agg({'users': 'sum','pageviews':'sum'}).reset_index().to_csv(f'portal-pages-months-fy{fy}.csv',index=False)


# In[11]:

print("Reading in Keen counts")
keen = pd.read_csv(f'dataset_downloads_fy{fy}.csv',parse_dates=['date_full'])
keen['date_full'] = keen['date_full'].astype(str)


# In[13]:

print("Joining GA to Keen")
# Join page visits to keen page groups
keen_visits_merge = pd.merge(keen,visits_pages[['page_path_2','pageviews','date']],how="left",left_on=['page_path_2','date_full'],right_on=['page_path_2','date'])


# In[14]:


keen_visits_merge = keen_visits_merge.drop(columns='date')
keen_visits_merge['pageviews'] = keen_visits_merge['pageviews'].fillna(0)


# To get any kind of download totals, we need to subtract pageviews from results from 
# Python Requests user agent. First, we will create two groups, a total downloads by page, 
# and a total downloads by month. For each of these, we'll subtract pageviews.

# In[15]:
print("Subsetting into objects with a dataset page and objects without one")
# Get total downloads by day by page by user agent
keen_page_groups = keen_visits_merge.loc[keen_visits_merge['page_path_2'] != ''].groupby(['page_path_2',
                                                                                          'user_agent_family',
                                                                                          'date_full',
                                                                                          'date_month'
                                                                                         ]).aggregate({'result':'sum',
                                                                                                 'pageviews':'max'
                                                                                                }).reset_index()


# In[16]:


keen_nopages = keen_visits_merge.loc[keen_visits_merge['page_path_2'].isnull()]

print("Writing drupal activity per month")
keen_nopages.loc[keen_nopages['log.key'].str.startswith('city_docs/')].groupby(['log.key','date_month']).aggregate({'result':'sum'}).reset_index().to_csv(f'keen-drupal-months-fy{fy}.csv',index=False)


# In[17]:

print("Subtracting dataset previews from downloads")
# Here, we are subtracting pageviews from the Python Requests number
# Because Python Requests includes loading the preview when a user visits the page, which is not a download
def adjust_result(row):
    if row['user_agent_family'] == 'Python Requests':
        if row['result'] - row['pageviews'] < 0:
            return 0
        else:
            return row['result'] - row['pageviews']
    else:
        return row['result']


# In[18]:


keen_dl_adjusted = keen_page_groups.apply(adjust_result,axis=1)


# In[19]:


keen_page_groups.loc[:,'result'] = keen_dl_adjusted


# In[20]:


keen_page_groups = keen_page_groups.drop(columns=['pageviews'])


# In[21]:

# The strategy for this is to capture the majority of legitimate pageviews
# And discard the rest
# Other exists in this dictionary just for reference
# Bots, crawlers and spiders are removed in previous script

ua_lookup = {
  'browser':
    [
      'Amazon Silk',
      'Android',
      'CFNetwork',
      'Chrome',
      'Chrome Mobile',
      'Chrome Mobile iOS',
      'Chromium',
      'Edge',
      'Firefox',
      'Firefox Mobile',
      'IE',
      'Mobile Safari',
      'Opera',
      'Safari',
      'Samsung Internet',
      'Vivaldi',
      'Yandex Browser',
      'HeadlessChrome',
      'Chrome Mobile WebView',
      'Mobile Safari UI/WKWebView'
    ],
  'script':[
    'Apache-HttpClient',
    'curl',
    'Drupal',
    'Jupyter kernel',
    'Python Requests',
    'Python-urllib',
    'Wget',
    'aws-sdk-java'
  ],
  'other':[
    'Go-http-client', # This is Go building static site
    'Tableau' # This is the web data connector
    'Cyberduck' # This is downloads from Cyberduck
  ]
}


# In[22]:


def assign_ua_type(ua_family):
    if ua_family in ua_lookup.get('browser'):
        return 'browser'
    elif ua_family in ua_lookup.get('script'):
        return 'script'
    else:
        return "other"
        


# In[23]:

print("Assigning a user agent type based on family")
ua_type = keen_page_groups['user_agent_family'].apply(assign_ua_type)


# In[24]:


keen_page_groups = keen_page_groups.assign(user_agent_type=ua_type)


# In[25]:


# Get downloads by type by month
monthly_downloads = keen_page_groups.groupby(['date_month','user_agent_type']).agg({'result':'sum'}).reset_index()


# In[26]:


# Total downloads, all sources
total_downloads = keen_page_groups.loc[(keen_page_groups['user_agent_type'] == 'browser') |
(keen_page_groups['user_agent_type'] == 'script'),
'result'].sum()

print(f"Total downloads is {total_downloads}")

# In[27]:


# Get downloads by type by page
page_downloads = keen_page_groups.groupby(['page_path_2','user_agent_type']).agg({'result':'sum'}).reset_index()


# In[29]:

print("Writing Keen activity per page per ua")
keen_page_groups.to_csv(f'keen-pages-ua-fy{fy}.csv',index=False)


# In[30]:


# Utilization table
keen_browser = page_downloads.loc[page_downloads['user_agent_type'] == 'browser'].copy()


# In[31]:


keen_browser_users = pd.merge(keen_browser,users_by_page,how="left",left_on="page_path_2",right_on='page_path_2')


# In[32]:

print("Reading in dataset page links")
pages_links = pd.read_csv('dataset_page_links.csv')


# In[33]:

print("Merging Keen events from browsers per page with number of links per page")
keen_users_links = pd.merge(keen_browser_users,pages_links,how='left',left_on="page_path_2",right_on='page_path_2')


# In[34]:

print("Calculating utilization")
inverted_counts = keen_users_links.apply(lambda x: 1/x['counts'] , axis=1)


# In[35]:


keen_users_links = keen_users_links.assign(counts_inverted=inverted_counts)
total_inverted_counts = keen_users_links['counts_inverted'].sum()
weighted_dl = keen_users_links.apply(lambda x: (x['counts_inverted']/total_inverted_counts)*(x['result']/x['users']) , axis=1)


# In[36]:


keen_users_links = keen_users_links.assign(dl_user_weight=weighted_dl)


# In[37]:


keen_dl_users_page = keen_users_links.drop(columns=['pageviews','counts','counts_inverted'])
total_weighted_dl = keen_dl_users_page['dl_user_weight'].sum()
print(f"Overall utilization is {total_weighted_dl}")


# In[38]:

print("Writing utilization per page")
keen_dl_users_page.to_csv(f'portal-utilization-fy{fy}.csv',index=False)


# Utilization is the number of downloads per user, weighted according to the number of links on the page the user visited
