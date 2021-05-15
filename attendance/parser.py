import re
import string
from datetime import datetime
from typing import Optional, Dict, List, Any


from attendance.sitting_day_attendee import SittingDayAttendee


class Parser:

    item_content_xpath = '//div[@id="documentContentPanel"]//text()'
    item_metadata_dt_xpath = '//div[@class="metadata"]//dt[@class="mdLabel"]'
    item_metadata_dd_xpath = '//div[@class="metadata"]//dd[@class="mdValue"]'
    allowed_chars = string.digits + string.ascii_letters + string.punctuation

    def page_content(self, url, tree):
        retrieved_date = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
        result = {"content": None, "url": None, "metadata": {}}

        replacements = {
            "\xa0": " ",
            "/*": " ",
            "*/": " ",
            "<style>": "",
            "\n": " ",
            "\r": " ",
        }
        spaces_re = re.compile(" +")

        # get the content
        content_lines = []
        for line_raw in tree.xpath(self.item_content_xpath):
            line = str(line_raw).strip() or ""
            for find, replace in replacements.items():
                line = line.replace(find, replace)

            line = spaces_re.sub(" ", line).strip()
            if line:
                content_lines.append(line)

        if not content_lines:
            return []

        content_text = "\n".join(content_lines)
        result["content"] = content_text

        # get the attendance
        attendance = self.parse_attendance(content_text) or {}

        # get the metadata
        metadata_raw = tree.xpath(
            self.item_metadata_dt_xpath + "|" + self.item_metadata_dd_xpath
        )
        metadata_stop = int(len(metadata_raw) / 2)

        for index in range(0, metadata_stop):
            key_index = index * 2
            value_index = key_index + 1

            key = self.normalise_string(metadata_raw[key_index].text_content())
            if not key:
                continue

            value = self.normalise_string(metadata_raw[value_index].text_content())

            metadata_key = "{:02d}_{}".format(index, key).lower()

            if metadata_key.endswith("_date"):
                result["metadata"][metadata_key] = datetime.strptime(
                    value, "%d-%m-%Y"
                ).strftime("%Y-%m-%d")
            else:
                result["metadata"][metadata_key] = value

        metadata = result["metadata"]

        # datetimes TEXT: YYYY-MM-DD HH:MM:SS.SSS
        # booleans INTEGER: 0 (false), 1 (true)
        # others are TEXT

        # title = metadata.get("00_title")
        # database_name = metadata.get("01_database")
        sitting_date = metadata.get("02_date")
        # source = metadata.get("03_source")
        # number = metadata.get("04_number") or metadata.get("05_number")
        # parl_number = metadata.get("04_parl no.")
        # page = metadata.get("05_page") or metadata.get("06_page")
        # status = metadata.get("06_status") or metadata.get("07_status")
        # federation_chamber_main_committee = metadata.get(
        #     "07_federation chamber / main committee"
        # )
        # system_id = metadata.get("08_system id")

        outcome = []
        for person_name, is_leave in attendance.get("people", []):
            assembly = attendance.get("house")
            if assembly and sitting_date and person_name:
                outcome.append(
                    SittingDayAttendee(
                        url=url,
                        assembly=attendance.get("house"),
                        sitting_date=sitting_date,
                        person_name=person_name,
                        is_leave=is_leave,
                        retrieved_date=retrieved_date,
                    )
                )
            else:
                print(
                    f"skipping '{assembly}', '{sitting_date}', '{person_name}' from '{url}'."
                )
        return outcome

    def normalise_string(self, value: str):
        if not value:
            return ""

        value = value.replace("’", "'")
        remove_newlines = value.replace("\n", " ").replace("\r", " ").strip()
        result = "".join(
            c if c in self.allowed_chars else " " for c in remove_newlines
        ).strip()
        return result

    def parse_house(self, content: str) -> Optional[str]:
        """Determine which house this content is about."""

        lower = content.lower()
        is_senate = "senate" in lower or "senators" in lower
        is_reps = "house of representatives" in lower

        if is_reps and not is_senate:
            house = "House of Representatives"
        elif not is_reps and is_senate:
            house = "Senate"
        else:
            print("Could not determine house.")
            house = None

        return house

    def parse_clerk(self, lines: List[str]):
        # clerk name
        clerk = None
        for index, line in enumerate(lines):
            if "Clerk of the House of Representatives" in line:
                clerk = lines[index - 1].strip()
                break
            elif "Clerk of the Senate" in line:
                clerk = lines[index - 1].strip()
                break
            elif line == "Clerk":
                clerk = lines[index - 1].strip()
                break
            elif line == "Acting Clerk":
                clerk = lines[index - 1].strip()
                break

        if not clerk:
            print("Could not determine clerk.")
            # raise ValueError('Could not determine clerk')
            return None

        return clerk

    def parse_attendance(self, content: str) -> Optional[Dict[str, Any]]:
        people = []

        lines = content.split("\n")

        house = self.parse_house(content)
        if not house:
            return None

        clerk = self.parse_clerk(lines)

        # names and leave status
        all_attended = False

        # parse attendance
        for line in lines:
            if "All Members attended (at some time during the sitting) except " in line:
                names_raw = line.replace(
                    "All Members attended (at some time during the sitting) except ", ""
                )
                names_raw = names_raw.replace(" and ", ", ")
                names_raw = names_raw.replace("* ", "*, ")
                names_raw = names_raw.replace(" *", ", *")

                names_norm = names_raw.strip(".")
                names = [i.strip() for i in names_norm.split(",") if i.strip()]

                for name in names:
                    name = name.strip()
                    on_leave = name.startswith("*") or name.endswith("*")
                    name = name.strip("*")
                    name_norm = self.normalise_string(name)
                    if any([i in name_norm for i in ["(", ")", "*"]]):
                        raise ValueError(f"Invalid parse for raw string '{line}'.")
                    people.append((name_norm, on_leave))

            elif "All Members attended (at some time during the sitting)." in line:
                all_attended = True

            elif "Present, all senators except " in line:
                names_raw = line.replace("Present, all senators except Senators ", "")
                names_raw = names_raw.replace(
                    "Present, all senators except Senator ", ""
                )
                names_raw = names_raw.replace(" and ", ", ")
                names_raw = names_raw.replace("(*on leave).", "")
                names_raw = names_raw.replace("(* on leave).", "")
                names_raw = names_raw.replace("(on leave).", "")
                names_raw = names_raw.replace("* ", "*, ")
                names_raw = names_raw.replace(" *", ", *")

                names_norm = names_raw.strip(".")
                names = [i.strip() for i in names_norm.split(",") if i.strip()]

                for name in names:
                    name = name.strip()
                    on_leave = (
                        name.startswith("*")
                        or name.endswith("*")
                        or "(on leave)" in line
                    )
                    name = name.strip("*")
                    name_norm = self.normalise_string(name)
                    if any([i in name_norm for i in ["(", ")", "*"]]):
                        raise ValueError(f"Invalid parse for raw string '{line}'.")
                    people.append((name_norm, on_leave))

            elif "Present, all senators." in line:
                all_attended = True

        if not people and not all_attended:
            print("Could not extract names.")
        elif people and all_attended:
            raise ValueError("Error extracting names.")
        elif not people and all_attended:
            people = [("EVERYONE PRESENT", False)]
        elif people and not all_attended:
            pass
        else:
            raise ValueError()

        # result
        result = {
            "house": house,
            "clerk": clerk,
            "people": people,
        }

        return result
