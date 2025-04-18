from typing import Tuple
from app.actions.rmwhub import GearSet, RmwSets

import logging

logger = logging.getLogger(__name__)


class RmwHubComparator:
    """
    This class is used to compare RMWhub gearsets with Earthranger gearsets.
    It provides methods to compare the data and determine if there is a match.
    """

    @staticmethod
    def compare_sets(self, rmw_sets: RmwSets, er_subjects: dict) -> Tuple[set, set]:
        """
        Compare all gearsets in ER with all gearsets in rmwHub.
        Return gearsets in ER that need to be inserted to rmwHub.
        """

        all_subjects = set()
        rmw_updates = set()
        rmw_inserts = set()
        for er_subject in er_subjects:
            if er_subject.get("name").startswith("rmwhub"):
                continue

            if not (
                er_subject.get("additional")
                and er_subject.get("additional").get("devices")
            ):
                logger.info(
                    f"ER subject with name {er_subject.get('name')} has no additional info or devices."
                )
                continue

            all_subjects.add(er_subject.get("name"))
            for rmw_gearset in rmw_sets.sets:
                if self._compare(er_subject, rmw_gearset):
                    rmw_updates.add(er_subject)

        rmw_inserts = all_subjects - rmw_updates

        return rmw_inserts, rmw_updates

    @staticmethod
    def _compare(rmw_gearset: GearSet, er_subject: dict) -> bool:
        """
        Compare the rmwHub gearset with the ER Subject.
        Return true if the ER subject is part of the rmwHub gearset.
        """

        rmw_device_list = []
        for rmw_device in rmw_gearset.traps:
            rmw_device_list.append(rmw_device.id)

        subject_devices = er_subject.get("additional").get("devices")
        er_device_list = []
        for er_device in subject_devices:
            er_device_list.append(er_device.get("device_id"))

        if rmw_device_list == er_device_list:
            logger.info(
                f"Rmw Gearset with Set ID {rmw_gearset.id} matches with ER subject with name: {er_subject.get('name')}"
            )
            return True
        else:
            return False
