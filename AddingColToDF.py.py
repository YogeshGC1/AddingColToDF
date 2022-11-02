#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()


# In[4]:


from pyspark.sql import SparkSession

spark=SparkSession.builder                  .master("local")                  .appName("AddColumntoDF")                  .getOrCreate()


# In[5]:


data = [('James','Smith','M',3000), ('Anna','Rose','F',4100),
  ('Robert','Williams','M',6200)]

columns = ["firstname","lastname","gender","salary"]

df = spark.createDataFrame(data=data, schema = columns)
df.show()


# In[12]:


#Add New Column with Constant Value

#lit() function by importing from pyspark.sql.functions import lit

from pyspark.sql.functions import lit
df.withColumn("constant value", lit(0.3))   .show()


# In[16]:


#Add Column Based on Another Column of DataFrame

df2=df.withColumn("YearlySalary", df.salary*12)      .show()


# In[19]:


#Add column by concatinating existing columns
from pyspark.sql.functions import concat_ws

df.withColumn("name", concat_ws(",", "firstname", "lastname"))  .show()


# In[24]:


#Add Column to DataFrame using select()
   
   


# In[29]:


#Add Column Value Based on Condition


from pyspark.sql.functions import when

df.withColumn("grade",    when((df.salary < 4000), lit("A"))      .when((df.salary >= 4000) & (df.salary <= 5000), lit("B"))      .otherwise(lit("C"))   ).show()


# In[33]:


#5. Add Column When not Exists on DataFrame

# first get all thecolumn names present in DF
df.columns

# now add a column conditionally if not exist

if 'dummy' not in df.columns:
   df.withColumn("dummy",lit(None))
df.show()


# In[ ]:


#Add Multiple Columns using Map

# if you wanted to add a known set of columns you can easily do it by chaining withColumn() or using select()

#sometimes you may need to add multiple columns after applying some transformations, In that case, you can use either map() or foldLeft()

(/Let's, assume, DF, has, just, 3, columns, c1,c2,c3)
df2 = df.rdd.map(row=>{
//apply transformation on these columns and derive multiple columns
//and store these column vlaues into c5,c6,c7,c8,c9,10
(c1,c2,c5,c6,c7,c8,c9,c10)
})

