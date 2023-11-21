import pandas as pd
print('reading file...')
df = pd.read_csv('yellow_tripdata_2023-01.csv')
print('reading file...done!')
# If you know the name of the column skip this
first_column = df.columns[0]
# Delete first
print('dropping column from df...')
df = df.drop([first_column], axis=1)
print('dropping column from df...done!')
print('saving file...')
df.to_csv('yellow_tripdata_2023-01.csv', index=False)
print('saving file...done!')
