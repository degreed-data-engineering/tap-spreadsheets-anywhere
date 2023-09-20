import json
import re
from json import JSONDecodeError
import logging

LOGGER = logging.getLogger(__name__)


def generator_wrapper(root_iterator):
    LOGGER.info("########## root_iterator" + str(root_iterator))
    for obj in root_iterator:
        LOGGER.info("########## obj:" + str(obj))
        json_obj = json.loads(obj)
        to_return = {}
        LOGGER.info("########## json_obj type:" + str(type(json_obj)))
        LOGGER.info("########## json_obj sample:" + str(json_obj))
        for key, value in json_obj.items():
            if key is None:
                key = "_smart_extra"

            formatted_key = key
            # remove non-word, non-whitespace characters
            formatted_key = re.sub(r"[^\w\s]", "", formatted_key)
            # replace whitespace with underscores
            formatted_key = re.sub(r"\s+", "_", formatted_key)
            to_return[formatted_key.lower()] = value
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


# iterator = tap_spreadsheets_anywhere.json_of_jsons_handler.get_row_iterator(table_spec, reader)
