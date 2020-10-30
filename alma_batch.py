import requests
import json
from urllib.parse import urlparse
import yaml
import pandas as pd
from asyncio_throttle import Throttler
import aiohttp
import asyncio
from pathlib import Path
from alma_batch_utils import *
from uuid import uuid4
from datetime import datetime
from functools import partial
import traceback
import sys
import logging

handler = logging.FileHandler('./alma_batch_log.txt', 'w')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s:%(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger('AlmaBatch')
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

class AlmaBatch:
    
    def __init__(self, config: str):
        '''
            config: A path to a YAML config file that includes the following
                    - the user's API key
                    - the type of API query (as specified by an EX Libris API endpoint)
        '''
        self._load_config(config)
        self.api_doc = self._load_openapi(self.openapi)
        self.header = self._create_header()
        self.results = [] # Store batch results 
        self.errors = [] # Store batch errors
        self.batch_idx = 0 # Tracks the number processed in each batch (when batching requests)
        self.num_workers = 25 # Default value for number of async workers
        self.limit = 100 # Number of results per page (default is Ex Libris maximum)
    
    def _load_config(self, path: str):
        '''Loads the config file. 
                 path: should point to a YAML file.'''
        # Required elements in the config file
        required_keys = ['apikey', 
                        'endpoint',
                        'operation',
                        'openapi']
        try:
            with open(path, 'r') as f:
                config = yaml.load(f, Loader=yaml.FullLoader)
            # Check for required config elements
            if not all(k in config for k in required_keys):
                raise MissingConfiguration('One or more required configuration elements is missing.')
            # Dynamically set config values
            for key, value in config.items():
                setattr(self, key, value)
        except Exception as e:
            print('Error loading configuration.')
            raise
    
    def _load_openapi(self, openapi: str):
        '''Loads the OpenAPI documentation for the desired endpoint.
            openapi: either a URL or a local file path. The file should be in JSON format.'''
        try:
            # tests whether the argument is a URL or not
            if urlparse(openapi).scheme:
                doc = requests.get(openapi)
                doc.raise_for_status()
                return doc.json()
            else:
                with open(openapi, 'r') as f:
                    return json.load(f)
        except Exception as e:
            print('Failed to load OpenAPI documentation.')
            raise
    
    def _create_header(self):
        '''Creates the header for the API request, using the supplied API key from the config file and 
        (optionally) a supplied accept parameter (default is JSON) and content type (required if operation is put/post)'''
        try:
            header = {'Authorization': f'apikey {self.apikey}'}
            if hasattr(self, 'accepts'):
                header['Accept'] = self.accepts
            else:
                header['Accept'] = self.accepts = 'application/json'
            # If a put or post operation, we need to include the content type 
            if self.operation in ['post', 'put']:
                header['Content-Type'] = self.content_type
            return header
        except AttributeError:
            print('Configuration error. "content_type" is a required parameter for POST or PUT operations.')
            raise
    
    def load_csv(self, path_to_csv: str, clean_columns: bool = True):
        '''Loads the CSV file at the supplied path and assigns to self.
        Second argument is a flag for whether the columns should be converted to snake case.'''
        try:
            self.data = pd.read_csv(path_to_csv)
            if clean_columns:
                self.data.columns = convert_column_case(self.data.columns)
            return self
        except Exception as e:
            print('Failed to load CSV.')
            raise e
    
    def validate_data(self):
        '''Checks that the columns or keys in self.data correspond to the parameters in the specified API.'''
        # Check to see if the data object has "columns," i.e., is a DataFrame
        if hasattr(self.data, 'columns'):
            columns = set(self.data.columns)
        # Otherwise, should be a list of dictionaries -- get the set of the keys across all entries
        else:
            columns = {k for row in self.data for k in row.keys()}
        try:
            # Get the params associated with the specified endpoint
            params = self.api_doc['paths'][self.endpoint][self.operation]['parameters']
            query_params = {p['name'] for p in params if p['in'] == 'query'}
            path_params = {p['name'] for p in params if p['in'] == 'path'}
            # Path parameters should be a subset of the columns
            if (not path_params <= columns) and (not 'link' in columns):
                raise InvalidParameters('One or more path parameters missing from the data you supplied.')
            # Intersection of query params and columns should not be empty if no path params
            elif (not path_params) and not (query_params & columns):
                raise InvalidParameters('No valid parameters present in the data you supplied.')
            self.path_params = path_params
            self.query_params = query_params
            return self
        except Exception as e:
            print('Error validating parameters')
            raise

    def _make_batch(self):
        # If self.data is a DataFrame, convert to native Python structure (list of dicts) for processing
        # Include the index of the row as a separate value
        # For each row, we initialize the page to 0 (for paginated results)
        if hasattr(self.data, 'columns'):
            rows = [{'idx': i, 
                    'page': 0,
                    'row_data': row._asdict()} for i, row in enumerate(self.data.itertuples(index=False))]
        else:
            rows = [{'idx': i, 
                    'page': 0,
                    'row_data': row} for i, row in enumerate(self.data)]
        if self.batch_size > 0:
            batches = chunk_list(rows, self.batch_size)
        # If batch_size = 0, use a single batch
        else:
            batches = [rows]
        return batches

    def _construct_request(self, row_data: dict, page: int):
        '''Given a dict of keys and values, returns a url and a set of query parameters for an API call.
        page should be a value for paginated results, or 0 for the first page of results.'''

        # In some cases, we may be able to use the pre-constructed URL returned by another ExL API.
        if 'link' in row_data:
            url = row_data['link']
        else:
            # Construct the URL
            url = self.exl_base + self.endpoint
            url = url.format(**row_data)
        #Include the remaining key-value pairs in the row if in the params for this endpoint
        params = {k: v for k,v in row_data.items() if k in self.query_params}    
        # Check for an offset to include
        if page > 0:
            params['offset'] = self.limit * page
        params['limit'] = self.limit
        # Check for fixed parameters, i.e., params that every call should include
        if hasattr(self, 'fixed_params'):
            params.update(self.fixed_params)
        # Return the parameters together with the headers as an object for the aiohttp client, along with the formatted URL
        args = {'params': params,
               'headers': self.header}
        return url, args

    def _check_for_pagination(self, result: dict):
        '''Checks a result from the API for paginated results. If the total_record_count attribute > self.limit, then returns a number of pages still needed. Otherwise, returns 0.'''
        total_results = result.get('total_record_count')
        if total_results and total_results > self.limit:
            return int(total_results / self.limit) # Number of (additional) iterations needed to get the rest of the results
        return 0

    def _update_data(self):
        '''Updates the data supplied as parameters to the API calls, indicating which rows have been completed.'''
        if len(self.results) == 0:
            print('No successful results to update.')
            return
        try:
            # If self.data is not already a a DataFrame, convert to one
            if not hasattr(self.data, 'columns'):
                data = pd.DataFrame.from_records(self.data)
            else:
                data = self.data.copy()
            # Create another out of the successful results
            # For results without pagination, the total_record_count will be absent, so we default to 1
            successes = pd.DataFrame.from_records([{'idx': result['idx'], 
                                                    'page': result['page'] + 1,
                                                    'total_results': result['result'].get('total_record_count', 1)}
                                                    for result in self.results]).sort_values(by=['idx', 'page'])
            # Compute the precentage success
            # First, translate the number of pages into results
            summary = successes.groupby(['idx', 'total_results']).page.apply(lambda x: len(x)*self.limit)    
            # Then represent that as a percentage of the total results as indicated by the API
            summary = summary / summary.index.get_level_values('total_results')
            # Finally, correct for values > 1 (since the last page of results may not be a full page)
            summary = summary.where(summary < 1, 1) * 100
            # Give a name to the percentage column
            summary.name = 'percentage_complete'
            # Join the summary to the input data
            summary_df = summary.reset_index().join(data, on='idx', how='right')
            # Drop the index column and replace missing values with zeroes
            summary_df = summary_df.drop('idx', axis=1)
            summary_df.percentage_complete = summary_df.percentage_complete.fillna(0)
            summary_df.to_csv(self.output_file, index=False)
        except Exception as e:
            raise
        return self
    
    def dump_output(self, batch: int = None):
        '''Saves the API output to disk, as JSON map from row index to result object.
        If a batch parameter is supplied and the batch_size is non-zero, save the output in batches.'''
        if self.batch_size > 0:
            # Save everything added since the last batch, if anything was added
            if not len(self.results) > self.batch_idx: 
                return self
            api_data = self.results[self.batch_idx:]
            # Update the counter for the next iteration
            self.batch_idx = len(self.results)
        else:
            api_data = self.results
        # Timestamp to add as part of metadata
        timestamp = datetime.now().strftime("%d-%m-%Y %H:%M")
        # Unique part of filename
        file_id = uuid4()
        api_metadata = {'timestamp': timestamp,
                       'endpoint': self.endpoint,
                       'operation': self.operation}
        api_output = {'metadata': api_metadata,
                     'data': api_data}
        try:
            with open(Path(self.path_for_api_output) / f'api_results_batch-{batch}-{file_id}.json', 'w') as f:
                json.dump(api_output, f)
        except Exception as e:
            print("Error saving API output.")
            raise
        return self

    def _do_after_requests(self, iteration: int):
        if hasattr(self, 'output_file'):
            self._update_data()
        if hasattr(self, 'path_for_api_output') and self.serialize_return:
            print(f'Saving API output for batch {iteration+1}')
            self.dump_output(batch=iteration+1)
    
    async def _async_request(self, row_data: dict, idx: int, page: int=0, payload=None):
        '''row should be a dict mapping path & query parameters. 
        idx should be a numeric row index from the original dataset.
        page should be an integer for paginated results. 
        If payload is present, it will typically be dictionary, too.'''
        url, args = self._construct_request(row_data, page)
        # Optional payload (for PUT/POST)
        # TO DO: Accept XML payloads
        if payload and (getattr(self, 'content_type', None) == 'application/json'):
            args['json'] = payload
        # Capture the index and page for the results/errors
        output = {'idx': idx,
                'page': page}
        # Get the client method corresponding to the desired HTTP method
        client_method = getattr(self.client, self.operation)
        logger.debug(f'Making request for row {idx}, page {page}.')
        try:
            # Wrap the call in the throttler context manager
            async with self.throttler:    
                async with client_method(url, **args) as session:
                    # Check for an API error (which will not be sent as JSON or XML)
                    if (session.content_type != self.accepts) or (session.status != 200):
                        error = await session.text()
                        raise APIException(error)
                    elif self.content_type == 'application/json':
                        result = await session.json()
                        logger.debug(f'Received data for row {idx}, page {page}.')
                    else:
                        result = await session.text()
                        # Save the result
            output['result'] = result
            self.results.append(output)
            if page == 0:
                return self._check_for_pagination(result)
            # If this isn't the first page of results, assume that the pagination has already been accounted for; we shouldn't need to add more tasks to the queue
            return 0
        except Exception as e:
            # If this row throws an exception, record it in the errors list
            exc_info = sys.exc_info()
            output['error'] = traceback.format_exception(*exc_info)
            logger.error(f'Error {e} on row {idx} page {page}.')
            self.errors.append(output)
            # Can't extract pagination in this case
            return 0
    
    async def _worker(self):
        '''
        Worker to consume tasks from the queue; each task is a single request.
        If the results of the request are paginated, the worker adds new tasks to the queue, one per page.
        '''
        while True:
            request_task = await self.queue.get() # Get a task to process. Request should be a dictionary containing a row of data, an index (idx), and an page 
            logger.debug(f"Task acquired: index {request_task['idx']}, page {request_task['page']}.")
            try:
                more_pages = await self._async_request(**request_task) 
                # Mark the current request task as done
                # If there are more pages, add those requests to the queue
                for page in range(more_pages):
                    # Update the page parameter for each additional page of results
                    new_task = request_task.copy()
                    new_task['page'] = page + 1
                    self.queue.put_nowait(new_task)
                    logger.debug(f"Task queued up: index {request_task['idx']}, page {request_task['page']}.")
                self.queue.task_done()
            except Exception as e:
                print(f"Aborting request {request_task['idx']} without completion.")
                #self.queue.task_done()
                raise e
 
    async def _main(self, batch): 
        '''
        Handles adding tasks to the queue, one per request, and then running the queue with the event loop. This function is blocking: each batch will kick off a "new" asynchronous queue of tasks, which will be finished before the next batch proceeds. 
        '''
        self.queue = asyncio.Queue() # Async task queue => keeps track of the number of tasks left to perform
        # Initialize a new client session
        self.client = aiohttp.ClientSession()
        for row in batch:
            self.queue.put_nowait(row) # Initialize the queue with the initial call to each endpoint (page=0)
        #Create a list of coroutine workers to process the tasks
        tasks = [asyncio.create_task(self._worker()) for i in range(self.num_workers)] # Create the tasks
        # Main call that blocks until all queue items have been marked done
        async with self.client:
            # This usage will cause an exception raised by a worker to bubble up
            # If the worker (as opposed to the request) raises an exception, this will cause the wait function to return.
            # Otherwise, it will return when all tasks in the queue have been consumed (marked done).
            done, _ = await asyncio.wait([self.queue.join(), *tasks], 
                            return_when=asyncio.FIRST_COMPLETED)
        # Test for the existence of an exception (i.e., a cancelled worker)
        tasks_with_exceptions = set(done) & set(tasks)
        if tasks_with_exceptions:
            # propagate the exception -- this should raise an Exception and exit
            await tasks_with_exceptions.pop() 
        # Cancel our workers if no exceptions
        for task in tasks:
            task.cancel()
        # Wait until all worker tasks are cancelled.
        await asyncio.gather(*tasks, return_exceptions=True)

    def make_requests(self, rate_limit: int = 25, batch_size: int = 1000, serialize_return: bool = False):
        '''AManages asynchronous requests in batches.
        Rate limit is set by default to the max allowed by the ExL API (25 reqs/sec).
        The batch size can be set to 0 to operate without batching. 
        Otherwise, asynchronous requests will be batched, and in betweeen batches, if an output file path is specified 
        in the config file, then the data will be updated and saved to disk as a CSV file. 
        This allows the user to keep track of which rows have been processed in the event of application crash, serious network interruption, etc.
        Set the serialize_return flag to true to save the current API ouput to disk, too.'''
        self.batch_size = batch_size
        self.serialize_return = serialize_return
        # Context manager for throttling the requests
        self.throttler = Throttler(rate_limit)
        batches = self._make_batch()
        # Run in batches, pausing between each to update the data
        for j, batch in enumerate(batches):
            try:
                print(f'Running batch {j+1}...')
                asyncio.run(self._main(batch))
                self._do_after_requests(iteration=j)
            except Exception as e:
                print(f'Exception encountered on batch {j+1}. Proceeding to next batch.')
                #continue
                raise # For debugging
        print('All requests completed.')
        return self