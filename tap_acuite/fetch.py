import json
import asyncio
import singer
from singer.bookmarks import get_bookmark
from singer import metadata
from tap_acuite.utility import (
    get_generic,
    get_all,
    format_date,
)

logger = singer.get_logger()


def handle_paginated(resource, url="", func=None):
    extraction_time = singer.utils.now()

    if url == "":
        url = resource

    async def get(session, schema, state, mdata):       
        bookmark = get_bookmark(state, resource, "since")
        qs = {} if bookmark is None else {"lastModifiedSince": bookmark}
        if (resource == "locations"):
            qs["countryId"] = 27
            for row in await get_all(session, resource, url, qs):
            # optional transform function
                if func != None:
                    row = func(row)
                write_record(row, resource, schema, mdata, extraction_time)
            qs["countryId"] = 44
        if (resource == "companies"):
            qs["includeDeleted"] = True
        for row in await get_all(session, resource, url, qs):
            # optional transform function
            if func != None:
                row = func(row)

            write_record(row, resource, schema, mdata, extraction_time)
        return [(resource, extraction_time)]

    return get


async def handle_projects(session, schemas, state, mdata):
    extraction_time = singer.utils.now()
    resource = "projects"
    bookmark = get_bookmark(state, resource, "since")
    times = [("projects", extraction_time)]

    rows = await get_all(session, "projects", "projects", {"includeArchived": "true"})

    # can't filter this in query string as we need all projects to pass to sub-streams. Has to be client-side filtering
    # only using string sorting rather than date comparison, but ISO date format means that this works perfectly
    filtered_projects = [
        r for r in rows if (bookmark is None or r["DateLastModified"] >= bookmark)
    ]
    write_many(
        filtered_projects, "projects", schemas["projects"], mdata, extraction_time
    )

    subqueries = []
    for project in rows:
        if schemas.get("audits"):
            subqueries.append(
                handle_audits(
                    session,
                    project["Id"],
                    schemas,
                    state,
                    mdata,
                )
            )
            times.append(("audits", extraction_time))
            if schemas.get("audit_sections"):
                times.append(("audit_sections", extraction_time))
            if schemas.get("audit_questions"):
                times.append(("audit_questions", extraction_time))
        if schemas.get("hsevents"):
            subqueries.append(
                handle_hsevents(session, project["Id"], schemas, state, mdata)
            )
            times.append(("hsevents", extraction_time))
    await asyncio.gather(*subqueries)

    return times


async def handle_people(session, schemas, state, mdata):
    extraction_time = singer.utils.now()
    resource = "people"
    url = resource
    sync_people_projects = "people_projects" in schemas
    bookmark = get_bookmark(state, resource, "since")
    qs = {"includeDeleted": "true"}
    if bookmark is not None:
        qs["lastModifiedSince"] = bookmark

    times = [(resource, extraction_time)]
    if sync_people_projects:
        times.append(("people_projects", extraction_time))

    for row in await get_all(session, resource, url, qs):
        write_record(row, resource, schemas[resource], mdata, extraction_time)

        if sync_people_projects:
            for p in row["AssignedProjects"]:
                record = {
                    "Id": str(row["Id"]) + "|" + str(p["Id"]),
                    "person_id": row["Id"],
                    "project_id": p["Id"],
                }
                if (
                    record["Id"] is None
                    or record["person_id"] is None
                    or record["project_id"] is None
                ):
                    continue
                write_record(
                    record,
                    "people_projects",
                    schemas["people_projects"],
                    mdata,
                    extraction_time,
                )

    return times


async def handle_hsevents(session, project_id, schemas, state, mdata):
    url = f"projects/{project_id}/hse/events"
    resource = "hsevents"
    extraction_time = singer.utils.now()

    sync_categories = schemas.get("categories")
    sync_subcategories = schemas.get("subcategories")
    categories_ids = set() if sync_categories else None
    subcategories_ids = set() if sync_subcategories else None

    bookmark = get_bookmark(state, resource, "since")
    qs = {} if bookmark is None else {"lastModifiedSince": bookmark}
    res = await get_generic(session, "hsevents", url, qs)
    # immediately discard everything except the ID to minimise memory footprint (could be holding this array for a while)
    row_ids = [row["Id"] for row in res["Data"]]

    columns_to_trim = [
        "Description",
        "PreventativeAction",
        "ActionTaken",
        "WeatherConditions",
    ]

    # do all processing at the row level, including writing records one at a time
    # this should minimise memory usage
    async def get_detail(id):
        r = await get_generic(session, "hsevents", f"{url}/{id}")
        row = r["Data"]
        # Project ID isn't returned in the record, so add it
        row["ProjectId"] = project_id

        # keep only first 500 characters of these columns as they aren't needed for reporting, take up space in Redshift, and Redshift tops out at 1k characters
        # need to trim before JSON-encoding as trimming a JSON-encoded string will leave a string that's not valid JSON
        for col in columns_to_trim:
            if row.get(col) and len(row[col]) > 500:
                row[col] = row[col][:500] + "..."

        # See https://www.notion.so/fosters/pipelinewise-target-redshift-strips-newlines-f937185a6aec439dbbdae0e9703f834b
        columns_with_special_characters = ["Description", "ActionTaken"]
        for col in columns_with_special_characters:
            if row.get(col):
                row[col] = json.dumps(row[col])

        write_record(row, "hsevents", schemas["hsevents"], mdata, extraction_time)

        if sync_categories:
            c = row["SubCategory"]["ParentCategory"]
            if c["Id"] not in categories_ids:
                categories_ids.add(c["Id"])
                write_record(
                    c, "categories", schemas["categories"], mdata, extraction_time
                )
        if sync_subcategories:
            s = row["SubCategory"]
            if s["Id"] not in subcategories_ids:
                subcategories_ids.add(s["Id"])
                write_record(
                    s, "subcategories", schemas["subcategories"], mdata, extraction_time
                )

    await asyncio.gather(*[get_detail(id) for id in row_ids])


# once closed, can't be edited (unless Acuite unlocks it), so safe to stop syncing
async def handle_audits(session, project_id, schemas, state, mdata):
    url = f"projects/{project_id}/audits"
    resource = "audits"
    extraction_time = singer.utils.now()

    sync_sections = "audit_sections" in schemas
    sync_questions = "audit_questions" in schemas
    sync_comments = "audit_question_comments" in schemas

    bookmark = get_bookmark(state, resource, "since")
    qs = {} if bookmark is None else {"lastModifiedSince": bookmark}
    r = await get_generic(session, resource, url, qs)

    for row in r["Data"]:
        detail = await get_generic(session, resource, f"{url}/{row['Id']}")
        detail = detail["Data"]
        detail["ProjectId"] = project_id

        if detail.get("AuditedCompany"):
            detail["AuditedCompanyId"] = detail["AuditedCompany"]["Id"]

        # write audit
        write_record(detail, resource, schemas[resource], mdata, extraction_time)

        if sync_sections:
            r = "audit_sections"
            for section in detail["Sections"]:
                section["audit_id"] = detail["Id"]
                write_record(section, r, schemas[r], mdata, extraction_time)

        if sync_questions:
            r = "audit_questions"
            for section in detail["Sections"]:
                for q in section["Questions"]:
                    q["audit_id"] = detail["Id"]
                    q["section_id"] = section["Id"]
                    if q.get("Answer"):
                        # Redshift has max length 1k characters
                        # see https://www.notion.so/fosters/pipelinewise-target-redshift-strips-newlines-f937185a6aec439dbbdae0e9703f834b
                        q["Answer"] = json.dumps(q["Answer"][:750])
                    write_record(q, r, schemas[r], mdata, extraction_time)

                    if sync_comments and q.get("Comments"):
                        for (i, c) in enumerate(q["Comments"]):
                            c["Id"] = str(q["Id"]) + "_" + str(i)
                            c["QuestionId"] = q["Id"]
                            # could have special characters
                            c["CommentText"] = json.dumps(c["CommentText"])
                            write_record(
                                c,
                                "audit_question_comments",
                                schemas["audit_question_comments"],
                                mdata,
                                extraction_time,
                            )


async def handle_detailed(session, resource, url, schemas, state, mdata):
    extraction_time = singer.utils.now()
    bookmark = get_bookmark(state, resource, "since")
    qs = {} if bookmark is None else {"lastModifiedSince": bookmark}
    r = await get_generic(session, resource, url, qs)

    for row in r["Data"]:
        detail = await get_generic(session, resource, f"{url}/{row['Id']}")
        write_record(
            detail["Data"], resource, schemas[resource], mdata, extraction_time
        )

    return [(resource, extraction_time)]


# More convenient to use but has to all be held in memory, so use write_record instead for resources with many rows
def write_many(rows, resource, schema, mdata, dt):
    for row in rows:
        write_record(row, resource, schema, mdata, dt)


def write_record(row, resource, schema, mdata, dt):
    with singer.Transformer() as transformer:
        rec = transformer.transform(row, schema, metadata=metadata.to_map(mdata))
    singer.write_record(resource, rec, time_extracted=dt)


def write_bookmark(state, resource, dt):
    singer.write_bookmark(state, resource, "since", format_date(dt))
    return state
