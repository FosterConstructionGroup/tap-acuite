from tap_acuite.fetch import handle_paginated, handle_projects


SYNC_FUNCTIONS = {
    "companies": handle_paginated("companies"),
    "locations": handle_paginated("locations"),
    "people": handle_paginated("people"),
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

