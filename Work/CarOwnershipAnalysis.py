import pandas as pd

# Create the data with repeated names for multiple cars
data = {
    'name': [
        'John', 'John',  # John owns two cars
        'Emma',  # Emma owns one car
        'Michael', 'Michael', 'Michael',  # Michael owns three cars
        'Sarah',  # Sarah owns one car
        'David', 'David',  # David owns two cars
        'Lisa',  # Lisa owns one car
        'James',  # James owns one car
        'Anna',  # Anna owns one car
        'Peter', 'Peter',  # Peter owns two cars
        'Sophie'  # Sophie owns one car
    ],
    'car': [
        'Toyota', 'BMW',  # John's cars
        'Honda',  # Emma's car
        'Toyota', 'Tesla', 'Audi',  # Michael's cars
        'BMW',  # Sarah's car
        'Honda', 'Mercedes',  # David's cars
        'Mercedes',  # Lisa's car
        'BMW',  # James's car
        'Audi',  # Anna's car
        'Tesla', 'Toyota',  # Peter's cars
        'Mercedes'  # Sophie's car
    ]
}

# Create the DataFrame
df = pd.DataFrame(data)

# Sort the DataFrame by name
df = df.sort_values(by='name')

# Reset the index after sorting
df = df.reset_index(drop=True)

# Display the DataFrame
print("\nDataFrame (sorted by name):")
print(df)

# Show which cars appear multiple times
print("\nCars and their counts:")
print(df['car'].value_counts())

# Show how many cars each person has
print("\nNumber of cars per person:")
print(df['name'].value_counts())