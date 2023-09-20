import json
import re
from json import JSONDecodeError
import logging

LOGGER = logging.getLogger(__name__)


def generator_wrapper(root_iterator):
    json_obj = json.load(root_iterator)

    # to_return["content"] = json_obj

    for key in json_obj.keys():
        if key is None:
            key = "_smart_extra"
        to_return = {}

        formatted_key = key
        # remove non-word, non-whitespace characters
        formatted_key = re.sub(r"[^\w\s]", "", formatted_key)
        # replace whitespace with underscores
        formatted_key = re.sub(r"\s+", "_", formatted_key)
        to_return["KEY"] = key
        to_return["VALUE"] = json_obj[key]
        yield to_return


def get_row_iterator(table_spec, reader):
    try:
        return generator_wrapper(iter(reader))
    except JSONDecodeError as jde:
        if jde.msg.startswith("Extra data"):
            reader.seek(0)
            json_objects = []
            for jobj in reader:
                json_objects.append(json.loads(jobj))
            return generator_wrapper(json_objects)
        else:
            raise jde
