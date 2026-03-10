import pandas as pd

# Create the data with repeated names for multiple cars and matching pets
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
    ],
    'pet': [
        'Dog', 'Dog',  # John has a dog
        'Cat',  # Emma has a cat
        'Fish', 'Fish', 'Fish',  # Michael has a fish
        'Bird',  # Sarah has a bird
        'Cat', 'Cat',  # David has a cat
        'Hamster',  # Lisa has a hamster
        'Dog',  # James has a dog
        'Cat',  # Anna has a cat
        'Bird', 'Bird',  # Peter has a bird
        'Rabbit'  # Sophie has a rabbit
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

# Show car counts
print("\nCars and their counts:")
print(df['car'].value_counts())

# Show pet counts
print("\nPets and their counts:")
print(df['pet'].value_counts())

# Show how many cars each person has
print("\nNumber of cars per person:")
print(df['name'].value_counts())