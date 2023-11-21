#!/usr/bin/env python
# coding: utf-8

# In[7]:


import pandas as pd


# In[8]:


df = pd.read_parquet('yellow_tripdata_2023-01.parquet')


# In[9]:


print('Converting file...')


# In[ ]:


df.to_csv('yellow_tripdata_2023-01.csv')


# In[ ]:


print('Converting file finished!')


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




