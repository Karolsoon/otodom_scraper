


def parse_floor(floor: str|int|None) -> int|None:
    if isinstance(floor, int):
        return floor
    floor_mapping = {
        'ground_floor': 1,
        'one_floor': 1,
        'two_floors': 2,
        'three_floors': 3,
        'more': 4,
        '1': 1,
        '2': 2,
        '3': 3,
        '4': 4,
        '5': 5,
        '6': 6,
        '7': 7,
        '8': 8,
        '9': 9,
        '10': 10
    }
    return floor_mapping.get(floor, None)


def parse_street(street: dict[str, str|None]) -> str|None:
    """
    Parse the street from the JSON data.
    """
    if isinstance(street, dict):
        s = street['name']
        if street.get('number'):
            s += ' ' + street['number']
        return s
    return street

def parse_floor(floor: str|None) -> int|None:
    """
    Parse the floor from the JSON data.
    """
    if isinstance(floor, int):
        return floor
    floor_mapping = {
        'parter': 0,
        '1': 1,
        '2': 2,
        '3': 3,
        '4': 4,
        '5': 5,
        '6': 6,
        '7': 7,
        '8': 8,
        '9': 9,
        '10': 10,
        '> 10': 11
    }
    return floor_mapping.get(floor, None)