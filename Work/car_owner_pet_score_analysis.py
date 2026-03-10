from typing import Dict, List, Union
import pandas as pd
from pandas import DataFrame


def reverse_string(text: str) -> str:
    """
    Function that accepts a string and returns it reversed, maintaining proper case
    if the input was in proper case
    
    Args:
        text (str): The input string to reverse
        
    Returns:
        str: The reversed string, maintaining proper case if input was proper case
    """
    # Check if the original string is in proper form (first letter capital, rest lowercase)
    is_proper: bool = text == text.capitalize() and text[1:].islower()

    # Reverse the string
    reversed_text: str = text[::-1]

    # If original was in proper form, convert reversed string to proper form
    if is_proper:
        reversed_text = reversed_text.lower().capitalize()

    return reversed_text


def add_reversed_column(df: DataFrame, field_name: str) -> DataFrame:
    """
    Function that adds a new column with reversed values from the specified field
    
    Args:
        df (pandas.DataFrame): The DataFrame to modify
        field_name (str): The name of the field to reverse
        
    Returns:
        pandas.DataFrame: Modified DataFrame with the new reversed column
    """
    if field_name not in df.columns:
        print(f"Error: Field '{field_name}' does not exist in the DataFrame")
        return df

    # Create new column name
    new_column_name: str = f"{field_name}_reversed"

    # Add new column with reversed values
    df[new_column_name] = df[field_name].apply(reverse_string)

    return df


# Create the data with repeated names for multiple cars and matching pets
data: Dict[str, List[Union[str, float]]] = {
    'name': [
        'John', 'John',  # John owns two cars
        'Emma',  # Emma owns one car
        'Michael', 'Michael', 'Michael',  # Michael owns three cars
        'Sarah',  # Sarah owns one car
        'David', 'David',  # David owns two cars
        'Lisa',  # Lisa owns one car
        'James',  # James owns one car
        'anna',  # anna owns one car
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
    ],
    'score': [
        8.5, 7.9,  # John's scores
        9.1,  # Emma's score
        7.8, 8.2, 8.7,  # Michael's scores
        9.3,  # Sarah's score
        8.1, 8.4,  # David's scores
        8.9,  # Lisa's score
        7.7,  # James's score
        8.6,  # anna's score
        9.2, 8.8,  # Peter's scores
        9.0  # Sophie's score
    ]
}

# Create the DataFrame
df: DataFrame = pd.DataFrame(data)

# Sort the DataFrame by name
df = df.sort_values(by='name')

# Reset the index after sorting
df = df.reset_index(drop=True)

# Add reversed name column
df = add_reversed_column(df, 'name')

# Print the DataFrame
print(df)
