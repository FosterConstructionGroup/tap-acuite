from tap_acuite.fetch import get_paginated, get_projects


SYNC_FUNCTIONS = {
    "companies": get_paginated("companies"),
    "locations": get_paginated("locations"),
    "people": get_paginated("people"),
    "projects": get_projects,
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

