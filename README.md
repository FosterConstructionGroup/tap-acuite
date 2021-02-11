# tap-acuite

This is a [Singer](https://singer.io) tap that produces JSON-formatted
data from the Acuite API following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [Acuite API](https://api.acuite.co.nz/swagger/ui/index#/)
- Extracts the following resources from Acuite:
  - Projects
  - Audits
  - Health and safety events
  - Event categories
  - Event subcategories
  - Companies
  - Locations
  - People
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick start

1. Install

   We recommend using a virtualenv:

   ```bash
   > virtualenv -p python3 venv
   > source venv/bin/activate
   > pip install -e .
   ```

2. Get your Acuite API key

   Contact Acuite support to get an API key.

3. Create the config file

   Create a JSON file called `config.json` containing the access token you were provided.

   ```json
   { "api_key": "yourapikey" }
   ```

4. Run the tap in discovery mode to get properties.json file

   ```bash
   tap-acuite --config config.json --discover > properties.json
   ```

5. In the properties.json file, select the streams to sync

   Each stream in the properties.json file has a "schema" entry. To select a stream to sync, add `"selected": true` to that stream's "schema" entry. For example, to sync the pull_requests stream:

   ```
   ...
   "tap_stream_id": "projects",
   "schema": {
     "selected": true,
     "properties": {
   ...
   ```

6. Run the application

   `tap-acuite` can be run with:

   ```bash
   tap-acuite --config config.json --properties properties.json
   ```
