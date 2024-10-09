#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# #### Using OpenAlex

# In[2]:


orcid_clusters = pd.read_parquet("<path-to-dataframe-of-grouped-openalex-data-by-orcid>")


# In[3]:


orcid_clusters.shape


# In[4]:


orcid_clusters.sample(20)


# In[5]:


for col in ['works','names','aff_strings','institutions','concepts']:
    orcid_clusters[f'{col}_len'] = orcid_clusters[col].apply(len)


# In[6]:


orcid_clusters.sort_values('institutions_len', ascending=False).head(15)


# In[7]:


orcid_clusters[['orcid','names','works_len','names_len','aff_strings_len','institutions_len','concepts_len']]\
.to_parquet("orcid_oa_to_explore.parquet")


# In[ ]:


orcid_clusters[orcid_clusters['orcid']=='']


# #### Using ORCID public data file

# In[3]:


orcid_pdf_clusters = pd.read_parquet("<path-to-dataframe-of-grouped-orcid-public-data-file-by-orcid>")


# In[4]:


orcid_pdf_clusters.shape


# In[5]:


orcid_pdf_clusters.sample(20)


# In[6]:


for col in ['works','names','aff_strings','institutions','concepts']:
    orcid_pdf_clusters[f'{col}_len'] = orcid_pdf_clusters[col].apply(len)


# In[11]:


orcid_pdf_clusters.sort_values('works_len', ascending=False).head(40)


# In[12]:


orcid_pdf_clusters[['orcid','names','works_len','names_len','aff_strings_len','institutions_len','concepts_len']]\
.to_parquet("orcid_pdf_to_explore.parquet")


# In[ ]:


orcid_pdf_clusters[orcid_pdf_clusters['orcid']=='']


# In[ ]:





# #### Looking at both together

# In[2]:


orcid_pdf = pd.read_parquet("orcid_pdf_to_explore.parquet")
orcid_pdf['df_type'] = 'pdf'
orcid_oa = pd.read_parquet("orcid_oa_to_explore.parquet")
orcid_oa['df_type'] = 'oa'


# In[22]:


orcid_full = pd.concat([orcid_pdf, orcid_oa], axis=0) \
.sort_values('df_type').sort_values('orcid')


# In[23]:


orcid_full.shape


# In[5]:


orcid_full.drop_duplicates(subset=['orcid']).shape


# In[25]:


orcid_full.iloc[2483473:2483493, :]


# In[ ]:





# #### Testing one orcid file

# In[2]:


test_df = pd.read_parquet("<path-to-dataframe-of-all-grouped-data-by-orcid>")


# In[3]:


test_df.sample(15)


# In[ ]:




