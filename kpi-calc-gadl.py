
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
import json
from datetime import datetime as dt
fy = 22



# In[6]:

dl_pages = pd.read_csv(f'files/fy{fy}/dataset_downloads.csv',parse_dates=['date'])
dl_pages = dl_pages.rename(columns={'users':'users_event'})


visits_pages = pd.read_csv("/Users/andrellbower/Desktop/portal_pages_datasd.csv",
low_memory=False,
dtype='str')

visits_pages_dates = visits_pages['date'].apply(lambda x: dt.strptime(x,'%Y-%m-%d') if not pd.isna(x) else np.nan)
visits_pages_users_int = visits_pages['users'].apply(lambda x: int(x))

visits_pages = visits_pages.assign(date=visits_pages_dates,users=visits_pages_users_int)

visits_pages_this_fy = visits_pages.loc[(visits_pages_dates >= f'20{fy-1}-07-01')&
(visits_pages_dates < f'20{fy}-07-01')]

print("Breaking out dates to month and year")
dl_month = dl_pages['date'].apply(lambda x: x.month)
dl_year = dl_pages['date'].apply(lambda x: x.year)
dl_pages = dl_pages.assign(date_month=dl_month,date_year=dl_year)

visits_month = visits_pages_this_fy['date'].apply(lambda x: x.month)
visits_year = visits_pages_this_fy['date'].apply(lambda x: x.year)
visits_pages_this_fy = visits_pages_this_fy.assign(date_month=dl_month,date_year=dl_year)





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
dataset_pg_list = dl_pages['page_path_level2'].unique()
dataset_pg_users = visits_pages_this_fy.loc[visits_pages_this_fy['page_path_level2'].isin(dataset_pg_list)]
total_users = dataset_pg_users['users'].sum()
print(f"Total users is {total_users}")

total_downloads = dl_pages['total_events'].sum()
print(f"Total downloads is {total_downloads}")

# Get users per month
monthly_users = visits_pages_this_fy.groupby(['date_month']).agg({'users':'sum'}).reset_index()


# In[8]:


# Get list of most visited pages
users_by_page = visits_pages_this_fy.groupby(['page_path_level2']).agg({'users': 'sum'}).reset_index()
dls_by_page = dl_pages.groupby(['page_path_level2']).agg({'total_events': 'sum'}).reset_index()
dl_visits_by_page = pd.merge(dls_by_page,users_by_page,how='outer',on='page_path_level2')
print(dl_visits_by_page.columns)
# In[9]:

print("Calculating monthly users by page and writing to csv")
visits_pages_this_fy.groupby(['page_path_level2','date_month']).agg({'users': 'sum'}).reset_index().to_csv(f'files/fy{fy}/portal-pages-months.csv',index=False)


# In[32]:

print("Reading in dataset page links")
pages_links = pd.read_csv(f'files/fy{fy}/dataset_page_links.csv')


# In[33]:

print("Merging GA users from browsers per page with number of links per page")
ga_users_links = pd.merge(dl_visits_by_page,pages_links,how='left',on='page_path_level2')


# In[34]:

print("Calculating utilization")
inverted_counts = ga_users_links.apply(lambda x: 1/x['counts'] , axis=1)


# In[35]:


ga_users_links = ga_users_links.assign(counts_inverted=inverted_counts)
total_inverted_counts = ga_users_links['counts_inverted'].sum()
weighted_dl = ga_users_links.apply(lambda x: (x['counts_inverted']/total_inverted_counts)*(x['total_events']/x['users']) , axis=1)


# In[36]:


ga_users_links = ga_users_links.assign(dl_user_weight=weighted_dl)


# In[37]:


#keen_dl_users_page = keen_users_links.drop(columns=['pageviews','counts','counts_inverted'])
ga_dl_users_page = ga_users_links
total_weighted_dl = ga_dl_users_page['dl_user_weight'].sum()
print(f"Overall utilization is {total_weighted_dl}")


# In[38]:

print("Writing utilization per page")
ga_dl_users_page.to_csv(f'files/fy{fy}/portal-utilization.csv',index=False)


# Utilization is the number of downloads per user, weighted according to the number of links on the page the user visited
