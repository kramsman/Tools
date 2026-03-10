import pandas as pd

# Create lists for names, cars, and pets
names = ['John', 'Emma', 'Michael', 'Sarah', 'David', 'Lisa', 'James', 'Anna', 'Peter', 'Sophie']
cars = ['Toyota', 'Honda', 'Toyota', 'BMW', 'Honda', 'Mercedes', 'BMW', 'Audi', 'Tesla', 'Mercedes']
pets = ['Dog', 'Cat', 'Dog', 'Fish', 'Cat', 'Bird', 'Hamster', 'Dog', 'Fish', 'Cat']

# Create the DataFrame
df = pd.DataFrame({
    'name': names,
    'car': cars,
    'pet': pets
})

# Display the DataFrame
print("\nDataFrame:")
print(df)

# Show which cars are duplicated
print("\nCars that appear multiple times:")
print(df['car'].value_counts())

# Show which pets are duplicated
print("\nPets that appear multiple times:")
print(df['pet'].value_counts())