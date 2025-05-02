from typing import Any


def filter_dict(item: dict[str, Any], attributes: list[str]) -> dict[str, Any]:
    """
    Returns a dictionary filtered down to the keys from attributes.
    """
    return {k: v for k, v in item.items() if k in attributes}


def filter_list_of_dict(
        data: list[dict[str, Any]],
        attributes: list[str]
) -> list[dict[str, Any]]:
    """
    Returns a list of dictionaries filtered down to the keys from attributes.
    """
    return [
        {k: v for k, v in item.items() if k in attributes}
        for item in data
    ]


def extract_from_list_of_dict(
        data: list[dict[str, str]],
        keys: list[str],
) -> dict[str, dict[str, str]]:
    """
    Extract value by key from a list of dict into a flat list.
    Returns list of values.
    """
    return [
        item[key]
        for item in data
        for key in keys
    ]


def rename_keys(
        item: dict[str, str],
        keys_mapping: dict[str, str]
) -> dict[str, str]:
    """
    Rename the keys of a dictionary according to the specified mapping.
    """
    return {keys_mapping.get(k, k): v for k, v in item.items()}


def extract_characteristics(
        data: list[dict[str, Any]],
        keys: list[str]
) -> dict[str, str]:
    """
    Extract value by key from a list of dict into a flat list.
    Returns list of values.
    """
    return {
        item['label']: {
            key: item[key]
            for key in keys
        }
        for item in data
    }


def extract_features(data: list[dict[str, Any]],
        *args,
        **kwargs
) -> dict[str, list[str]]:
    """
    Extract value by key from a list of dict into a flat list.
    Returns list of values.
    """
    return {
        item['label']: item['values']
        for item in data
    }
