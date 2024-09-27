# Utilities for working with townships when exporting QC reports
import csv
import dataclasses
import datetime
import pathlib


@dataclasses.dataclass
class Township:
    """Class that represents a township for the purposes of QC reporting.

    The schema for this class must match the town_active_schedule.csv data file
    stored in this directory, since the script parses that file and turns each
    row into an instance of this class."""

    township_code: str
    township_name: str
    tri_code: str
    tri_name: str
    # Dates that the town becomes "active" (i.e. eligible for QC reporting)
    # or inactive
    active_start_date: datetime.date | None
    active_end_date: datetime.date | None

    def is_reassessed_during(self, year: int) -> bool:
        """Helper function to determine if a town in a given year is undergoing
        triennial reassessment during a given year"""
        # 2024 is the City reassessment year (tri code 1), so
        # ((2024 - 2024) % 3) + 1 == 1, and so on for the other two tris
        return str(((year - 2024) % 3) + 1) == self.tri_code

    def is_active_on(self, date: datetime.date) -> bool:
        """Helper function to determine if a town is 'active' (i.e. eligible
        for QC reporting) on a given date"""
        if self.active_start_date and self.active_end_date:
            return self.active_start_date <= date <= self.active_end_date
        return False


# Get a list of townships with their code and schedule, either from a schedule
# file if one exists or from a static definition
TOWNSHIP_SCHEDULE_PATH = pathlib.Path("scripts/utils/town_active_schedule.csv")
if TOWNSHIP_SCHEDULE_PATH.is_file():
    with open(TOWNSHIP_SCHEDULE_PATH.resolve()) as town_schedule_fobj:
        date_format = "%m/%d/%Y"
        TOWNSHIPS: list[Township] = [
            Township(
                township_code=town["township_code"],
                township_name=town["township_name"],
                tri_code=town["tri_code"],
                tri_name=town["tri_name"],
                active_start_date=(
                    datetime.datetime.strptime(
                        town["active_start_date"], date_format
                    ).date()
                    if town.get("active_start_date")
                    else None
                ),
                active_end_date=(
                    datetime.datetime.strptime(
                        town["active_end_date"], date_format
                    ).date()
                    if town.get("active_end_date")
                    else None
                ),
            )
            for town in csv.DictReader(town_schedule_fobj)
        ]
else:
    # Instantiate the list of townships with no active/inactive date data,
    # so that the script still works without access to the town_active_schedule
    TOWNSHIPS: list[Township] = [
        Township(
            township_code="10",
            township_name="Barrington",
            tri_code="2",
            tri_name="North",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="11",
            township_name="Berwyn",
            tri_code="3",
            tri_name="South",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="12",
            township_name="Bloom",
            tri_code="3",
            tri_name="South",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="13",
            township_name="Bremen",
            tri_code="3",
            tri_name="South",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="14",
            township_name="Calumet",
            tri_code="3",
            tri_name="South",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="15",
            township_name="Cicero",
            tri_code="3",
            tri_name="South",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="16",
            township_name="Elk Grove",
            tri_code="2",
            tri_name="North",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="17",
            township_name="Evanston",
            tri_code="2",
            tri_name="North",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="18",
            township_name="Hanover",
            tri_code="2",
            tri_name="North",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="19",
            township_name="Lemont",
            tri_code="3",
            tri_name="South",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="20",
            township_name="Leyden",
            tri_code="2",
            tri_name="North",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="21",
            township_name="Lyons",
            tri_code="3",
            tri_name="South",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="22",
            township_name="Maine",
            tri_code="2",
            tri_name="North",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="23",
            township_name="New Trier",
            tri_code="2",
            tri_name="North",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="24",
            township_name="Niles",
            tri_code="2",
            tri_name="North",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="25",
            township_name="Northfield",
            tri_code="2",
            tri_name="North",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="26",
            township_name="Norwood Park",
            tri_code="2",
            tri_name="North",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="27",
            township_name="Oak Park",
            tri_code="3",
            tri_name="South",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="28",
            township_name="Orland",
            tri_code="3",
            tri_name="South",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="29",
            township_name="Palatine",
            tri_code="2",
            tri_name="North",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="30",
            township_name="Palos",
            tri_code="3",
            tri_name="South",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="31",
            township_name="Proviso",
            tri_code="3",
            tri_name="South",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="32",
            township_name="Rich",
            tri_code="3",
            tri_name="South",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="33",
            township_name="River Forest",
            tri_code="3",
            tri_name="South",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="34",
            township_name="Riverside",
            tri_code="3",
            tri_name="South",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="35",
            township_name="Schaumburg",
            tri_code="2",
            tri_name="North",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="36",
            township_name="Stickney",
            tri_code="3",
            tri_name="South",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="37",
            township_name="Thornton",
            tri_code="3",
            tri_name="South",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="38",
            township_name="Wheeling",
            tri_code="2",
            tri_name="North",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="39",
            township_name="Worth",
            tri_code="3",
            tri_name="South",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="70",
            township_name="Hyde Park",
            tri_code="1",
            tri_name="City",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="71",
            township_name="Jefferson",
            tri_code="1",
            tri_name="City",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="72",
            township_name="Lake",
            tri_code="1",
            tri_name="City",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="73",
            township_name="Lake View",
            tri_code="1",
            tri_name="City",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="74",
            township_name="North Chicago",
            tri_code="1",
            tri_name="City",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="75",
            township_name="Rogers Park",
            tri_code="1",
            tri_name="City",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="76",
            township_name="South Chicago",
            tri_code="1",
            tri_name="City",
            active_start_date=None,
            active_end_date=None,
        ),
        Township(
            township_code="77",
            township_name="West Chicago",
            tri_code="1",
            tri_name="City",
            active_start_date=None,
            active_end_date=None,
        ),
    ]

# Map township codes to the corresponding Township data object
TOWNSHIPS_BY_CODE: dict[str, Township] = {
    town.township_code: town for town in TOWNSHIPS
}
