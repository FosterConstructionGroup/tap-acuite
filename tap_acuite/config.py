from tap_acuite.fetch import handle_paginated, handle_projects, handle_people


def transform_person(row):
    person_id = row["Id"]
    for p in row["AssignedProjects"]:
        p["PersonId"] = person_id
    return row


SYNC_FUNCTIONS = {
    "companies": handle_paginated("companies"),
    "locations": handle_paginated("locations"),
    "people": handle_people,
    "projects": handle_projects,
}

SUB_STREAMS = {
    "projects": [
        "audits",
        # > from audits
        "audit_sections",
        "audit_questions",
        "audit_question_comments",
        # < from audits
        "hsevents",
        # > from hsevents
        "categories",
        "subcategories",
        # < from hsevents
    ],
    "people": ["people_projects"],
}
