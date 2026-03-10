def get_multi_room_data(people):
    # Count how many rooms each name has
    name_rooms = {}
    for name, room in people:
        name_rooms.setdefault(name, set()).add(room)

    # Find names with more than one unique room
    multi_room_names = {name for name, rooms in name_rooms.items() if len(rooms) > 1}

    # Filter the original data for those names
    filtered = [entry for entry in people if entry[0] in multi_room_names]

    # Sort the result by name, then room
    filtered.sort(key=lambda x: (x[0], x[1]))

    return filtered


# Example data
people = [
    ["Alice", "Room 1"],
    ["Bob", "Room 2"],
    ["Charlie", "Room 1"],
    ["Diana", "Room 3"],
    ["Alice", "Room 2"],
    ["Charlie", "Room 3"],
    ["Eve", "Room 2"],
    ["Charlie", "Room 2"]   # 👈 Added Charlie to Room 2
]


# Get the filtered, sorted data
result = get_multi_room_data(people)

# Print nicely outside the function
print("Names in Multiple Rooms")
print("-----------------------")
for name, room in result:
    print(f"{name}: {room}")
