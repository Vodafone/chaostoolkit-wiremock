# -*- coding: utf-8 -*-
from typing import Any, List, Mapping

from chaoslib.types import Configuration
from logzero import logger

from .driver import Wiremock
from .utils import check_configuration, get_wm_params

__all__ = [
    "add_mappings",
    "populate_from_dir",
    "update_mappings_status_code",
    "update_all_mappings_status_code_and_body",
    "update_mappings_fault",
    "delete_mappings",
    "delete_all_mappings",
    "global_fixed_delay",
    "global_random_delay",
    "fixed_delay",
    "random_delay",
    "chunked_dribble_delay",
    "down",
    "up",
    "reset",
    "reset_mappings",
]


def _filter_mappings(f: Any, driver: Wiremock):
    if not filter:
        raise ValueError("Filter parameter cannot be empty")

    filter_keys = f.keys()
    if "request" in filter_keys and "metadata" in filter_keys:
        raise ValueError("Both 'request' and 'metadata' filters defined. Only one allowed at a time")

    if "request" in filter_keys:
        return driver.mappings_by_request(f["request"])
    elif "metadata" in filter_keys:
        return driver.mappings_by_metadata(f["metadata"])
    else:
        return driver.mappings_by_request(f)


def add_mappings(mappings: List[Any], configuration: Configuration = None) -> List[Any]:
    """adds more mappings to wiremock
    returns the list of ids of the mappings added
    """
    if not check_configuration(configuration):
        return []

    params = get_wm_params(configuration)
    w = Wiremock(url=params["url"], timeout=params["timeout"])
    return w.populate(mappings)


def populate_from_dir(dir: str = ".", configuration: Configuration = None) -> List[Any]:
    """adds all mappings found in the passed folder
    returns the list of ids of the mappings added
    """
    if not check_configuration(configuration):
        return []

    params = get_wm_params(configuration)
    w = Wiremock(url=params["url"], timeout=params["timeout"])
    return w.populate_from_dir(dir)


def delete_mappings(filter: List[Any], configuration: Configuration = None) -> List[Any]:
    """deletes a list of mappings
    returns the list of ids of the mappings deleted
    """
    if not check_configuration(configuration):
        return []

    params = get_wm_params(configuration)
    w = Wiremock(url=params["url"], timeout=params["timeout"])

    ids = []
    for f in filter:
        mapping = w.mapping_by_request(f)
        if mapping is None:
            logger.error("Mapping {} {} not found".format(mapping["request"]["method"], mapping["request"]["url"]))
            continue
        ids.append(w.delete_mapping(mapping["id"]))
    return ids


def update_mappings_status_code(status_code: str, filter: List[Any], configuration: Configuration = None) -> List[Any]:
    """deletes a list of mappings
    returns the list of ids of the mappings deleted
    """
    if not check_configuration(configuration):
        return []

    if not status_code:
        return []

    params = get_wm_params(configuration)
    w = Wiremock(url=params["url"], timeout=params["timeout"])

    mappings_to_update: List[Any] = None
    for f in filter:
        mapping = w.mapping_by_request(f)
        if mapping is None:
            logger.error("Mapping {} {} not found".format(mapping["request"]["method"], mapping["request"]["url"]))
            continue
        mappings_to_update.append(mapping)
    if len(mappings_to_update) > 0:
        return w.update_status_code(mappings_to_update, status_code)
    else:
        return []


def update_all_mappings_status_code_and_body(
    status_code: str, body: str = None, body_file_name: str = None, configuration: Configuration = None
) -> List[Any]:
    """changes all Wiremock mappings responses to the set status_code and body.
    :param status_code: the new http status code
    :param body: (optional) the response body as a string
    :param body_file_name: (optional) the response body as a file
    returns the list of ids of the mappings changed
    """
    if not check_configuration(configuration):
        return []

    if not status_code:
        return []

    params = get_wm_params(configuration)
    w = Wiremock(url=params["url"], timeout=params["timeout"])

    mappings_to_update: List[Any] = w.mappings()
    if len(mappings_to_update) > 0:
        return w.update_status_code_and_body(
            mappings_to_update, status_code=status_code, body=body, body_file_name=body_file_name
        )
    else:
        return []


def update_mappings_fault(filter: List[Any], fault: str, configuration: Configuration = None) -> List[Any]:
    """
    Updates the fault configuration of mappings
    """
    params = get_wm_params(configuration)
    w = Wiremock(url=params["url"], timeout=params["timeout"])

    mappings_to_update: List[Any] = []
    for f in filter:
        mappings = _filter_mappings(f, w)
        if mappings:
            mappings_to_update.extend(mappings)
        else:
            logger.error("No mappings found")

    if len(mappings_to_update) > 0:
        return w.update_fault(mappings_to_update, fault)
    else:
        return []


def delete_all_mappings(configuration: Configuration = None) -> bool:
    """deletes all mappings
    returns true if delete was successful and false if not
    """
    if not check_configuration(configuration):
        return False

    params = get_wm_params(configuration)
    w = Wiremock(url=params["url"], timeout=params["timeout"])
    return w.delete_all_mapping()


def down(filter: List[Any], configuration: Configuration = None) -> List[Any]:
    """set a list of services down
    more correctly it adds a chunked dribble delay to the mapping
    as defined in the configuration section (or action attributes)
    Returns the list of delayed mappings
    """
    params = get_wm_params(configuration)
    w = Wiremock(url=params["url"], timeout=params["timeout"])

    conf = configuration.get("wiremock", {})
    if "defaults" not in conf:
        logger.error("Down defaults not specified in config")
        return []

    defaults = conf.get("defaults", {})
    if "down" not in defaults:
        logger.error("Down defaults not specified in config")
        return []

    delayed = []
    for f in filter:
        delayed.append(w.chunked_dribble_delay(f, defaults["down"]))
    return delayed


def global_fixed_delay(fixedDelay: int = 0, configuration: Configuration = None) -> int:
    """add a fixed delay to all mappings"""
    params = get_wm_params(configuration)
    w = Wiremock(url=params["url"], timeout=params["timeout"])
    return w.global_fixed_delay(fixedDelay)


def global_random_delay(delayDistribution: Mapping[str, Any], configuration: Configuration = None) -> int:
    """adds a random delay to all mappings"""
    params = get_wm_params(configuration)
    w = Wiremock(url=params["url"], timeout=params["timeout"])
    return w.global_random_delay(delayDistribution)


def fixed_delay(filter: List[Any], fixedDelayMilliseconds: int, configuration: Configuration = None) -> List[Any]:
    """adds a fixed delay to a list of mappings"""
    params = get_wm_params(configuration)
    w = Wiremock(url=params["url"], timeout=params["timeout"])

    mappings_to_update: List[Any] = []
    for f in filter:
        mappings = _filter_mappings(f, w)
        if mappings:
            mappings_to_update.extend(mappings)
        else:
            logger.error("No mappings found")

    if len(mappings_to_update) > 0:
        return w.fixed_delay(mappings_to_update, fixedDelayMilliseconds)

    return []


def random_delay(
    filter: List[Any], delayDistribution: Mapping[str, Any], configuration: Configuration = None
) -> List[Any]:
    """adds a random delay to a list of mapppings"""
    params = get_wm_params(configuration)
    w = Wiremock(url=params["url"], timeout=params["timeout"])

    updated = []
    for f in filter:
        updated.append(w.random_delay(f, delayDistribution))
    return updated


def chunked_dribble_delay(
    filter: List[Any], chunkedDribbleDelay: Mapping[str, Any], configuration: Configuration = None
) -> List[Any]:
    """adds a chunked dribble delay to a list of mappings"""
    params = get_wm_params(configuration)
    w = Wiremock(url=params["url"], timeout=params["timeout"])

    updated = []
    for f in filter:
        updated.append(w.chunked_dribble_delay(f, chunkedDribbleDelay))
    return updated


def up(filter: List[Any], configuration: Configuration = None) -> List[Any]:
    """deletes all delays connected with a list of mappings"""
    params = get_wm_params(configuration)
    w = Wiremock(url=params["url"], timeout=params["timeout"])

    return w.up(filter)


def reset(configuration: Configuration = None) -> int:
    """resets the wiremock server: deletes all mappings!"""
    params = get_wm_params(configuration)
    w = Wiremock(url=params["url"], timeout=params["timeout"])

    return w.reset()


def reset_mappings(configuration: Configuration = None) -> int:
    """resets the wiremock server: deletes all mappings!"""
    params = get_wm_params(configuration)
    w = Wiremock(url=params["url"], timeout=params["timeout"])

    return w.reset_mappings()
