import pandas as pd

# Sample DataFrame
data = {'Name': ['Alice', 'Bob', 'Charlie'],
        'Age': [25, 30, 22],
        'City': ['New York', 'San Francisco', 'Los Angeles']}

df = pd.DataFrame(data)

# Create a dictionary with 'Name' column as key and 'Age' and 'City' as values
name_dict = df.set_index('Name')[['Age', 'City']].to_dict(orient='index')

# Display the resulting dictionary
print(name_dict)
