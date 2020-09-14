from tap_acuite.fetch import handle_paginated, handle_projects


def transform_person(row):
    person_id = row["Id"]
    for p in row["AssignedProjects"]:
        p["PersonId"] = person_id
    return row


SYNC_FUNCTIONS = {
    "companies": handle_paginated("companies"),
    "locations": handle_paginated("locations"),
    "people": handle_paginated("people", func=transform_person),
    "projects": handle_projects,
}

SUB_STREAMS = {
    "projects": [
        "audits",
        "rfis",
        "hsevents",
        # > from hsevents
        "categories",
        "subcategories",
        # < from hsevents
    ],
}

