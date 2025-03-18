# Utilities for working with townships when exporting QC reports
import dataclasses


@dataclasses.dataclass
class Township:
    """Class that represents a township for the purposes of QC reporting"""

    township_code: str
    township_name: str
    tri_code: str
    tri_name: str

    def is_reassessed_during(self, year: int) -> bool:
        """Helper function to determine if a town in a given year is undergoing
        triennial reassessment"""
        # 2024 is the City reassessment year (tri code 1), so
        # ((2024 - 2024) % 3) + 1 == 1, and so on for the other two tris
        return str(((year - 2024) % 3) + 1) == self.tri_code


# Instantiate a canonical list of townships
township_attrs = [
    # township_code, township_name, tri_code, tri_name
    ("10", "Barrington", "2", "North"),
    ("11", "Berwyn", "3", "South"),
    ("12", "Bloom", "3", "South"),
    ("13", "Bremen", "3", "South"),
    ("14", "Calumet", "3", "South"),
    ("15", "Cicero", "3", "South"),
    ("16", "Elk Grove", "2", "North"),
    ("17", "Evanston", "2", "North"),
    ("18", "Hanover", "2", "North"),
    ("19", "Lemont", "3", "South"),
    ("20", "Leyden", "2", "North"),
    ("21", "Lyons", "3", "South"),
    ("22", "Maine", "2", "North"),
    ("23", "New Trier", "2", "North"),
    ("24", "Niles", "2", "North"),
    ("25", "Northfield", "2", "North"),
    ("26", "Norwood Park", "2", "North"),
    ("27", "Oak Park", "3", "South"),
    ("28", "Orland", "3", "South"),
    ("29", "Palatine", "2", "North"),
    ("30", "Palos", "3", "South"),
    ("31", "Proviso", "3", "South"),
    ("32", "Rich", "3", "South"),
    ("33", "River Forest", "3", "South"),
    ("34", "Riverside", "3", "South"),
    ("35", "Schaumburg", "2", "North"),
    ("36", "Stickney", "3", "South"),
    ("37", "Thornton", "3", "South"),
    ("38", "Wheeling", "2", "North"),
    ("39", "Worth", "3", "South"),
    ("70", "Hyde Park", "1", "City"),
    ("71", "Jefferson", "1", "City"),
    ("72", "Lake", "1", "City"),
    ("73", "Lake View", "1", "City"),
    ("74", "North Chicago", "1", "City"),
    ("75", "Rogers Park", "1", "City"),
    ("76", "South Chicago", "1", "City"),
    ("77", "West Chicago", "1", "City"),
]
TOWNSHIPS = [
    Township(
        township_code=code,
        township_name=name,
        tri_code=tri_code,
        tri_name=tri_name,
    )
    for code, name, tri_code, tri_name in township_attrs
]

# Map township codes to the corresponding Township data object so that we can
# lookup Township objects by code
TOWNSHIPS_BY_CODE: dict[str, Township] = {
    town.township_code: town for town in TOWNSHIPS
}
