from dataclasses import dataclass


@dataclass
class SittingDayAttendee:
    url: str
    assembly: str
    sitting_date: str
    person_name: str
    is_leave: bool
    retrieved_date: str
