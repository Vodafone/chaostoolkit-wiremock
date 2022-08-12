# -*- coding: utf-8 -*-
"""

This module implements a few functions to handle mappings on a wiremock server.
It is meant not to be complete and to be the most transparent possible,
in contrast with the official wiremock driver. For example there is no
validation of the payloads.

"""

import glob
import json
import os
from typing import Any, Dict, List, Mapping, Optional

from logzero import logger
import requests

from .utils import can_connect_to

AVAILABLE_FAULTS = [
    "EMPTY_RESPONSE",
    "MALFORMED_RESPONSE_CHUNK",
    "RANDOM_DATA_THEN_CLOSE",
    "CONNECTION_RESET_BY_PEER",
]


class ConnectionError(Exception):
    pass


class Wiremock:
    def __init__(
        self,
        host: str = None,
        port: str = None,
        url: str = None,
        timeout: int = 1,
    ):

        if host and port:
            url = f"http://{host}:{port}"
        self.base_url = f"{url}/__admin"
        self.mappings_url = f"{self.base_url}/mappings"
        self.settings_url = f"{self.base_url}/settings"
        self.reset_url = f"{self.base_url}/reset"
        self.reset_mappings_url = f"{self.mappings_url}/reset"
        self.timeout = timeout
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        if (host and port) and can_connect_to(host, port) is False:
            raise ConnectionError("Wiremock server not found")

    def mappings(self) -> List[Any]:
        """
        retrieves all mappings
        returns the array of mappings found
        """
        r = requests.get(
            self.mappings_url, headers=self.headers, timeout=self.timeout
        )
        if r.status_code != 200:
            logger.error(
                "[mappings]:Error retrieving mappings: {}".format(r.text())
            )
            return []
        else:
            res = r.json()
            return res["mappings"]

    def mapping_by_id(self, id=int) -> Dict[str, Any]:
        r = requests.get(
            "{}/{}".format(self.mappings_url, id),
            headers=self.headers,
            timeout=self.timeout,
        )
        if r.status_code != 200:
            logger.error(
                "[mapping_by_id]:Error retrieving mapping: {}".format(r.text())
            )
            return -1
        else:
            return r.json()

    def filter_mapping(self, _filter: Mapping, strict: bool = True) -> Mapping:
        matching_mappings = self.filter_mappings(_filter, strict, limit=1)
        return matching_mappings[1] if len(matching_mappings) > 0 else None

    def filter_mappings(
        self, _filter: Mapping, strict: bool = True, limit: int = 0
    ) -> List[Mapping]:
        mappings = self.mappings()

        matching_mappings = []
        count = 0
        for mapping in mappings:
            if strict:
                node = mapping.get("request")
                matches = self.strict_filter(node, _filter)
            else:
                matches = self.recursive_filter(mapping, _filter)

            if matches:
                matching_mappings.append(mapping)
                count += 1

            if limit > 0 and count >= limit:
                break

        return matching_mappings

    def strict_filter(self, node: Mapping, _filter: Mapping) -> bool:
        intersec = node.keys() & _filter.keys()
        if len(intersec) != len(_filter.keys()):
            return False
        for key in _filter.keys():
            f = _filter[key]
            comp = node.get(key)
            if f != comp:
                return False
        return True

    def recursive_filter(
        self, node: Mapping, _filter: Mapping, depth: int = 0
    ) -> bool:
        intersec = node.keys() & _filter.keys()
        if len(intersec) != len(_filter.keys()):
            return False
        for key in _filter.keys():
            f = _filter[key]
            comp = node.get(key)
            if isinstance(f, Mapping):
                if not self.recursive_filter(comp, f, depth=depth + 1):
                    return False
            elif isinstance(f, List):
                if comp not in f:
                    return False
            elif f != comp:
                return False

        return True

    def mapping_by_request_exact_match(
        self, request: Mapping[str, Any] = None
    ) -> Dict[str, Any]:
        mappings = self.mappings()
        for mapping in mappings:
            if mapping["request"] == request:
                return mapping
        return None

    def populate(self, mappings: Mapping[str, Any]) -> List[Any]:
        """Populate: adds all passed mappings
        Returns the list of ids of mappings created
        """
        if isinstance(mappings, list) is False:
            logger.error("[populate]:ERROR: mappings should be a list")
            return None

        ids = []
        for mapping in mappings:
            id = self.add_mapping(mapping)
            if id is not None:
                ids.append(id)
            else:
                logger.error("[populate]:ERROR adding a mapping")
                return None

        return ids

    def populate_from_dir(self, dir: str) -> List[Any]:
        """reads all json files in a directory and adds all mappings
        Returns the list of ids of mappings created
        or None in case of errors
        """
        if not os.path.exists(dir):
            logger.error(
                "[populate_from_dir]: directory {} does not exists".format(dir)
            )
            return None

        ids = []
        for filename in glob.glob(os.path.join(dir, "*.json")):
            logger.info("Importing {}".format(filename))
            with open(filename) as f:
                mapping = json.load(f)
                id = self.add_mapping(mapping)
                if id is not None:
                    ids.append(id)
        return ids

    def update_fault(
        self, mappings: Mapping[str, Any], fault: str
    ) -> Optional[List[Any]]:
        """
        Updates fault status of stub mappings
        """
        if isinstance(mappings, list) is False:
            logger.error("[update_fault] mappings parameter should be a list")
            return None

        if fault not in AVAILABLE_FAULTS:
            logger.error(f"[update_fault] fault {fault} not available.")
            return None

        ids = []
        for mapping in mappings:
            id = mapping["id"]
            mapping["response"]["fault"] = fault

            if self.update_mapping(id, mapping):
                ids.append(id)
            else:
                logger.error(
                    "[populate]:ERROR updating a mapping with new fault"
                )
                return None
        return ids

    def update_status_code_and_body(
        self,
        mappings: Mapping[str, Any],
        status_code: str,
        body: str = None,
        body_file_name: str = None,
    ) -> List[Any]:
        """Populate: adds all passed mappings
        Returns the list of ids of mappings created
        """
        if isinstance(mappings, list) is False:
            logger.error("[populate]:ERROR: mappings should be a list")
            return None

        try:
            status_code_number = int(status_code)
            if status_code_number < 100 or status_code_number > 599:
                logger.error("ERROR: incorrect http status code")
                return None
        except Exception:
            logger.error("ERROR: incorrect http status code")
            return None

        ids = []
        for mapping in mappings:
            id = mapping["id"]
            mapping["response"]["status"] = status_code
            if body_file_name:
                mapping["response"]["bodyFileName"] = body_file_name
                mapping["response"]["body"] = None
            elif body:
                mapping["response"]["bodyFileName"] = None
                mapping["response"]["body"] = body

            if self.update_mapping(id, mapping):
                ids.append(id)
            else:
                logger.error(
                    "[populate]:ERROR updating a mapping with new status code"
                )
                return None
        return ids

    def update_mapping(
        self, id: str = "", mapping: Mapping[str, Any] = None
    ) -> Dict[str, Any]:
        """updates the mapping pointed by id with new mapping"""
        r = requests.put(
            "{}/{}".format(self.mappings_url, id),
            headers=self.headers,
            data=json.dumps(mapping),
            timeout=self.timeout,
        )
        if r.status_code != 200:
            logger.error("Error updating a mapping: " + r.text)
            return None
        else:
            return r.json()

    def add_mapping(self, mapping: Mapping[str, Any]) -> int:
        """add_mapping: add a mapping passed as attribute"""
        r = requests.post(
            self.mappings_url,
            headers=self.headers,
            data=json.dumps(mapping),
            timeout=self.timeout,
        )
        if r.status_code != 201:
            logger.error("Error creating a mapping: " + r.text)
            return None
        else:
            res = r.json()
            return res["id"]

    def delete_mapping(self, id: str):
        r = requests.delete(f"{self.mappings_url}/{id}", timeout=self.timeout)
        if r.status_code != 200:
            logger.error("Error deleting mapping %s: %s", id, r.text)
            return -1

        return id

    def delete_all_mappings(self):
        r = requests.delete(
            "{}".format(self.mappings_url), timeout=self.timeout
        )
        if r.status_code != 200:
            logger.error("Error deleting all mapping")
            return False
        else:
            return True

    def fixed_delay(
        self, mappings: Mapping[str, Any], fixedDelayMilliseconds: int = 0
    ) -> Dict:
        """
        updates the mappings adding a fixed delay
        returns the a list of updated mappings or none in case of errors
        """
        updated_ids = []
        for m in mappings:
            m["response"]["fixedDelayMilliseconds"] = fixedDelayMilliseconds
            m["response"]["delayDistribution"] = None
            result = self.update_mapping(m["id"], m)
            if result:
                updated_ids.append(m["id"])

        return updated_ids

    def global_fixed_delay(self, fixedDelay: int) -> int:
        r = requests.post(
            self.settings_url,
            headers=self.headers,
            data=json.dumps({"fixedDelay": fixedDelay}),
            timeout=self.timeout,
        )
        if r.status_code != 200:
            logger.error(
                "[global_fixed_delay]: Error setting delay: {}".format(r.text)
            )
            return -1
        else:
            return 1

    def random_delay(
        self, filter: Mapping[str, Any], delayDistribution: Mapping[str, Any]
    ) -> Dict[str, Any]:
        """
        Updates the mapping adding a random delay
        returns the updated mapping or none in case of errors
        """
        if not isinstance(delayDistribution, dict):
            logger.error("[random_delay]: parameter has to be a dictionary")

        mapping_found = self.mapping_by_request_exact_match(filter)

        if not mapping_found:
            logger.error("[random_delay]: Error retrieving mapping")
            return None

        mapping_found["response"]["delayDistribution"] = delayDistribution
        return self.update_mapping(mapping_found["id"], mapping_found)

    def global_random_delay(self, delayDistribution: Mapping[str, Any]) -> int:
        if not isinstance(delayDistribution, dict):
            logger.error(
                "[global_random_delay]: parameter has to be a dictionary"
            )
        r = requests.post(
            self.settings_url,
            headers=self.headers,
            data=json.dumps({"delayDistribution": delayDistribution}),
            timeout=self.timeout,
        )
        if r.status_code != 200:
            logger.error(
                "[global_random_delay]: Error setting delay: {}".format(r.text)
            )
            return -1
        else:
            return 1

    def chunked_dribble_delay(
        self, _filter: List[Any], chunkedDribbleDelay: Mapping[str, Any] = None
    ):
        """
        Adds a delay to the passed mapping
        returns the updated mapping or non in case of errors
        """
        if not isinstance(chunkedDribbleDelay, dict):
            logger.error(
                "[chunked_dribble_delay]: parameter has to be a dictionary"
            )
        if "numberOfChunks" not in chunkedDribbleDelay:
            logger.error(
                "[chunked_dribble_delay]: attribute numberOfChunks not "
                "found in parameter"
            )
            return None
        if "totalDuration" not in chunkedDribbleDelay:
            logger.error(
                "[chunked_dribble_delay]: attribute totalDuration not found "
                "in parameter"
            )
            return None

        mapping_found = self.mapping_by_request_exact_match(_filter)

        if not mapping_found:
            logger.error("[chunked_dribble_delay]: Error retrieving mapping")
            return None

        mapping_found["response"]["chunkedDribbleDelay"] = chunkedDribbleDelay
        return self.update_mapping(mapping_found["id"], mapping_found)

    def up(self, filter: List[Any] = None) -> List[Any]:
        """resets a list of mappings deleting all delays attached to them"""
        ids = []
        for f in filter:
            mapping_found = self.mapping_by_request_exact_match(f)
            if not mapping_found:
                next
            else:
                logger.debug(
                    "[up]: found mapping: {}".format(mapping_found["id"])
                )
                for key in [
                    "fixedDelayMilliseconds",
                    "delayDistribution",
                    "chunkedDribbleDelay",
                ]:
                    if key in mapping_found["response"]:
                        del mapping_found["response"][key]
                self.update_mapping(mapping_found["id"], mapping_found)
                ids.append(mapping_found["id"])
        return ids

    def reset(self) -> int:
        r = requests.post(
            self.reset_url, headers=self.headers, timeout=self.timeout
        )
        if r.status_code != 200:
            logger.error("[reset]:Error resetting wiremock server " + r.text)
            return -1
        else:
            return 1

    def reset_mappings(self) -> int:
        r = requests.post(
            self.reset_mappings_url, headers=self.headers, timeout=self.timeout
        )
        if r.status_code != 200:
            logger.error("[reset]:Error resetting wiremock mappings " + r.text)
            return -1
        else:
            return 1
