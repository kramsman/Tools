import pandas as pd

def reverse_string(text):
    """
    Function that accepts a string and returns it reversed, maintaining proper case
    if the input was in proper case
    
    Args:
        text (str): The input string to reverse
        
    Returns:
        str: The reversed string, maintaining proper case if input was proper case
    """
    # Check if the original string is in proper form (first letter capital, rest lowercase)
    is_proper = text == text.capitalize() and text[1:].islower()
    
    # Reverse the string
    reversed_text = text[::-1]
    
    # If original was in proper form, convert reversed string to proper form
    if is_proper:
        reversed_text = reversed_text.lower().capitalize()
    
    return reversed_text

def add_reversed_column(df, field_name):
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
    new_column_name = f"{field_name}_reversed"