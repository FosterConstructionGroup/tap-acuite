import os
import json
import asyncio
import aiohttp
import singer
from singer import metadata
from singer.bookmarks import get_bookmark

from tap_acuite.utility import get_abs_path, initialise_semaphore
from tap_acuite.config import SYNC_FUNCTIONS, SUB_STREAMS
from tap_acuite.fetch import write_bookmark

logger = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["api_key"]


def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path("schemas")):
        path = get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas


def populate_metadata(schema_name, schema):
    mdata = metadata.new()
    mdata = metadata.write(mdata, (), "table-key-properties", ["Id"])

    for field_name in schema["properties"].keys():
        mdata = metadata.write(
            mdata,
            ("properties", field_name),
            "inclusion",
            "automatic" if field_name == "Id" else "available",
        )

    return mdata


def get_catalog():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        # get metadata for each field
        mdata = populate_metadata(schema_name, schema)

        # create and add catalog entry
        catalog_entry = {
            "stream": schema_name,
            "tap_stream_id": schema_name,
            "schema": schema,
            "metadata": metadata.to_list(mdata),
            "key_properties": ["Id"],
        }
        streams.append(catalog_entry)

    return {"streams": streams}


def do_discover():
    catalog = get_catalog()
    # dump catalog
    print(json.dumps(catalog, indent=2))


def get_selected_streams(catalog):
    """
    Gets selected streams.  Checks schema's 'selected'
    first -- and then checks metadata, looking for an empty
    breadcrumb and mdata with a 'selected' entry
    """
    selected_streams = []
    for stream in catalog["streams"]:
        stream_metadata = stream["metadata"]
        if stream["schema"].get("selected", False):
            selected_streams.append(stream["tap_stream_id"])
        else:
            for entry in stream_metadata:
                # stream metadata will have empty breadcrumb
                if not entry["breadcrumb"] and entry["metadata"].get("selected", None):
                    selected_streams.append(stream["tap_stream_id"])

    return selected_streams


def get_stream_from_catalog(stream_id, catalog):
    for stream in catalog["streams"]:
        if stream["tap_stream_id"] == stream_id:
            return stream
    return None


async def do_sync(session, state, catalog):
    selected_stream_ids = get_selected_streams(catalog)

    # sync streams in parallel
    streams = []

    for stream in catalog["streams"]:
        stream_id = stream["tap_stream_id"]
        stream_schema = stream["schema"]
        mdata = stream["metadata"]

        # if it is a "sub_stream", it will be sync'd by its parent
        if not SYNC_FUNCTIONS.get(stream_id):
            continue

        # if stream is selected, write schema and sync
        if stream_id in selected_stream_ids:
            singer.write_schema(stream_id, stream_schema, stream["key_properties"])

            # get sync function and any sub streams
            sync_func = SYNC_FUNCTIONS[stream_id]
            sub_stream_ids = SUB_STREAMS.get(stream_id, None)

            # sync stream
            if not sub_stream_ids:
                streams.append(sync_func(session, stream_schema, state, mdata))

            # handle streams with sub streams
            else:
                stream_schemas = {stream_id: stream_schema}

                # get and write selected sub stream schemas
                for sub_stream_id in sub_stream_ids:
                    if sub_stream_id in selected_stream_ids:
                        sub_stream = get_stream_from_catalog(sub_stream_id, catalog)
                        stream_schemas[sub_stream_id] = sub_stream["schema"]
                        singer.write_schema(
                            sub_stream_id,
                            sub_stream["schema"],
                            sub_stream["key_properties"],
                        )

                # sync stream and it's sub streams
                streams.append(sync_func(session, stream_schemas, state, mdata))

    streams_resolved = await asyncio.gather(*streams)

    # update bookmark by merging in all streams
    for substream in streams_resolved:
        for (resource, extraction_time) in substream:
            state = write_bookmark(state, resource, extraction_time)
    singer.write_state(state)


async def run_async(config, state, catalog):
    auth_headers = {"AcuiteApiKey": config["api_key"]}
    async with aiohttp.ClientSession(headers=auth_headers) as session:
        initialise_semaphore()
        await do_sync(session, state, catalog)


@singer.utils.handle_top_exception(logger)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        do_discover()
    else:
        catalog = args.properties if args.properties else get_catalog()
        asyncio.run(run_async(args.config, args.state, catalog))


if __name__ == "__main__":
    main()
