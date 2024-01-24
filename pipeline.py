# importing pandas library
import pandas as pd
import pdpipe as pdp
# Create empty dataframe
df = pd.DataFrame()
 
# Creating a simple dataframe
df['name'] = ['Reema', 'Shyam', 'Jai', 
              'Nimisha', 'Rohit', 'Riya']
df['gender'] = ['Female', 'Male', 'Male', 
                'Female', 'Male', 'Female']
df['age'] = [31, 32, 19, 23, 28, 33]

print(df)

#transformation functions
def mean_age_by_group(dataframe, col):
   
    # groups the data by a column and 
    # returns the mean age per group
    return dataframe.groupby(col).mean()

# function to convert to uppercase
def uppercase_column_name(dataframe):
   
    # Converts all the column names into uppercase
    dataframe.columns = dataframe.columns.str.upper()
     
    # And returns them
    return dataframe  


# Create a pipeline that applies both the functions created above
pipeline = df.pipe(mean_age_by_group, col='gender').pipe(uppercase_column_name)

# calling pipeline
pipeline
