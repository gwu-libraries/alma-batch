# AlmaBatch
#### A Python library for using Ex Libris' Alma APIs in asynchronous fashion. 
This is an alpha release. 

## How It Works
- The `AlmaBatch` object can work with either a pandas `DataFrame` or a Python list of dictionaries. For each row in the data, it makes an asynchronous API call, using the columns or keys as parameters. Calls can be executed in batches (_e.g_, 500 at a time) or as one single batch. Errors are caught and reported for the relevant rows, and API output can be serialized and saved locally.
- The current release handles only `GET` operations, but I plan to build in support for `POST` and `PUT` ops in a future release.


## How to Use It
1. Clone this repo.
2. Create a new environment (preferably using Python 3.8+).
```
python -m venv ENV
source ENV/bin/activate
```
3. Install the requirements.
```
pip install -r requirements.txt
```
4. Edit `_config.yml_` with the following information:
   - The URL for [the OpenAPI specs](https://developers.exlibrisgroup.com/blog/openapi-support-in-alma-apis/) (in JSON) for the particular API endpoint you're using (from the Ex Libris Dev Center documentation).
   - A valid Ex Libris API key.
   - The base URL for your Alma API endpoint.
   - The particular endpoint you are using (from the Ex Libris Dev Center documentation).
   - The name of an output file (in CSV format) where AlmaBatch will report the results of each call (including the path to it on your local machine).
   - An optional path to a folder (on your local machine) for serializing the results from the API calls.
5. Call `AlmaBatch` from a separate script or Jupyter Notebook (as shown in `alma_batch_examples.ipynb`). The notebook contains a worked example for retrieving items from an Alma Physical Item set and scanning those items in. It also contains an example for scanning in items from a CSV file (such as output by an Alma Analytics report). 
   