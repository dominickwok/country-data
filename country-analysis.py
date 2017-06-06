
# coding: utf-8

# In[2]:

import requests
import pprint
import pandas as pd
import wbdata
import matplotlib.pyplot as plt


# In[3]:

countries = ["AR","BR","CO","CL","MX","PE"]


# In[4]:

indicators = {
    'NY.GNP.PCAP.CD':'gni_per_capita',
    'NY.GDP.MKTP.CD':'gdp',
    'SP.POP.TOTL':'pop',
    'SP.URB.TOTL':'pop_urban',
    'SP.POP.0014.TO.ZS':'pop_0-14',
    'SP.POP.1564.TO.ZS':'pop_15-64',
    'SP.POP.65UP.TO.ZS':'pop_65+',
    'SL.TLF.TOTL.IN':'labor_force',
    'SH.DYN.MORT':'mortality_rate',
    'IT.NET.USER.P2': 'internet_user_per_100_people',
    'IT.NET.BBND': 'fixed_broadband_subs',
    'IT.CEL.SETS': 'cellular_mobile_subs'
}


# In[5]:

df = wbdata.get_dataframe(
            indicators,
            country=countries,
            convert_date=True,
            keep_levels=True)


# In[6]:

dfu = df.reset_index()


# In[7]:

print(dfu.head())


# In[8]:

dfu.to_csv('wb_data.csv',index=False)
pd.io.gbq.to_gbq(dfu,
                 destination_table='dkwok.wb_data',
                 project_id='gro-analytics',
                 if_exists='replace')

