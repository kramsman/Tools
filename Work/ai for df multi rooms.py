import pandas as pd


def filter_and_sort_multiple_rooms(df, name_col="name", room_col="room"):
    """
    Return all rows for names that appear in more than one unique room,
    sorted by name.

    Parameters:
    - df: pandas DataFrame
    - name_col: column name for the person's name
    - room_col: column name for the room
    """
    # Count unique rooms per name
    room_counts = df.groupby(name_col)[room_col].nunique()

    # Get names with more than one room
    names_multiple_rooms = room_counts[room_counts > 1].index.tolist()

    # Filter original DataFrame for those names
    filtered_df = df[df[name_col].isin(names_multiple_rooms)].sort_values(by=name_col).reset_index(drop=True)

    return filtered_df


# Example DataFrame with a 'pet' column
df = pd.DataFrame({
    "name": ["Alice", "Bob", "Alice", "Charlie", "Bob", "Diana"],
    "room": ["Room A", "Room B", "Room C", "Room A", "Room C", "Room B"],
    "pet": ["Dog", "Cat", "Parrot", "Hamster", "Fish", "Rabbit"]
})

# Call the function specifying column names (optional if default names match)
result_df = filter_and_sort_multiple_rooms(df, name_col="name", room_col="room")

print("Filtered DataFrame (names in multiple rooms):")
print(result_df)
