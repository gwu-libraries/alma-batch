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

class AlmaBatch:
	
	def __init__(self, config: str):
		'''
			config: A path to a YAML config file that includes the following
					- the user's API key
					- the type of API query (as specified by an EX Libris API endpoint)
		'''
		self.load_config(config)
		self.api_doc = self.load_openapi(self.openapi)
		self.header = self.create_header()
		self.results = [] # Store batch results 
		self.errors = [] # Store batch errors
		self.batch_idx = 0 # Tracks the number processed in each batch (when batching requests)
	
	def load_config(self, path: str):
		'''Loads the config file. 
				 path: should point to a YAML file.'''
		# Required elements in the config file
		required_keys = ['apikey', 
						'endpoint',
						'operation']
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
	
	def load_openapi(self, openapi: str):
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
	
	def create_header(self):
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
			if not path_params <= columns:
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

	def _construct_request(self, row: dict):
		'''Given a dict of keys and values, returns a url and a set of query parameters for an API call.'''

		# In some cases, we may be able to use the pre-constructed URL returned by another ExL API.
		if 'link' in row:
			url = row['link']
		else:
			# Construct the URL
			url = self.exl_base + self.endpoint
			url = url.format(**row)
		#Include the remaining key-value pairs in the row if in the params for this endpoint
		params = {k: v for k,v in row.items() if k in self.query_params}	
		# Check for fixed parameters, i.e., params that every call should include
		if hasattr(self, 'fixed_params'):
			params.update(self.fixed_params)
		# Return the parameters together with the headers as an object for the aiohttp client, along with the formatted URL
		args = {'params': params,
			   'headers': self.header}
		return url, args
	
	async def _async_request(self, row: dict, idx: int, payload=None):
		'''Row should be a dict mapping path & query parameters. 
		Idx should be a numeric row index from the original dataset.
		If payload is present, it will typically be dictionary, too.'''
		url, args = self._construct_request(row)
		# Optional payload (for PUT/POST)
		# TO DO: Accept XML payloads
		if payload and (getattr(self, 'content_type', None) == 'application/json'):
			args['json'] = payload
		# Assign the index to the row dict for recording in the result/errors lists
		row['idx'] = idx
		# Get the client method corresponding to the desired HTTP method
		client_method = getattr(self.client, self.operation)
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
					else:
						result = await session.text()
					# Save the result
			row['result'] = result
			self.results.append(row)
			return 
		except Exception as e:
			# If this row throws an exception, record it in the errors list
			row['error'] = e
			self.errors.append(row)
			return
	
	def update_data(self):
		'''Updates the data supplied as parameters to the API calls, indicating which rows have been completed.'''
		if len(self.results) == 0:
			print('No successful results to update.')
			return
		# If a DataFrame, use the indices recorded in the results to update the original
		if hasattr(self.data, 'columns'):
			# Find the indices completed to date
			idxs = [r['idx'] for r in self.results]
			errors_idx = [r['idx'] for r in self.errors]
			# Flag successful rows
			self.data.loc[self.data.iloc[idxs].index, 'completed'] = True
			# Flag the errors, too
			self.data.loc[self.data.iloc[errors_idx].index, 'errors'] = True
		#Otherwise, no need to update self.data, because we can use the entries in self.errors & self.results
		return self
			
	def write_csv(self):
		'''Writes the data to CSV, using the output_path value from the config file.'''
		try:
			if hasattr(self.data, 'columns'):
				self.data.to_csv(self.output_file, index=False)
				return self
			# If not already a DataFrame, create one out of the results & errors
			# Drop the column that holds the actual API result, which will be a JSON object
			# Add a column indicating that these have been completed
			results_df = pd.DataFrame.from_records(self.results).drop('result', axis=1)
			results_df['completed'] = True
			# Likewise with the error messages
			errors_df = pd.DataFrame.from_records(self.errors).drop('error', axis=1)
			errors_df['errors'] = True
			# Join them together and write to disk, dropping the extra index column
			pd.concat([results_df, errors_df], axis=0).drop('idx', axis=1).to_csv(self.output_file, index=False)
			return self
		except Exception as e:
			print('Failed to save output file.')
			raise
	
	def dump_output(self, batch: int = None):
		'''Saves the API output to disk, as JSON map from row index to result object.
		If a batch parameter is supplied and the batch_size is non-zero, save the output in batches.'''
		if self.batch_size > 0:
			api_data = {row['idx']: row['result'] 
							for row in self.results[(batch-1)*self.batch_size:batch*self.batch_size]}
		else:
			api_data = {row['idx']: row['result'] for row in self.results}
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
	
	async def _gather_requests(self, batch: list):
		'''Encapsulates multiple asynchronous API calls.
		Batch should be a list of tuples. The first element should be a row index, the second the data for parametrizing the API call. '''
		# Initialize aiohttp client
		# We need to do this before each batch of requests, otherwise it will time out
		self.client = aiohttp.ClientSession()
		async with self.client:
			await asyncio.gather(*(self._async_request(row=row, idx=i) for i, row in batch))

	def do_after_requests(self, iteration: int):
		self.update_data()
		if hasattr(self, 'output_file'):
			self.write_csv()
		if hasattr(self, 'path_for_api_output') and self.serialize_return:
			print(f'Saving API output for batch {iteration+1}')
			self.dump_output(batch=iteration+1)


	def make_batch(self):
		# If self.data is a DataFrame, convert to native Python structure (list of dicts) for processing
		# Include the index of the row as a separate value
		if hasattr(self.data, 'columns'):
			rows = [(i, row._asdict()) for i, row in enumerate(self.data.itertuples(index=False))]
		else:
			rows = [(i, row) for i, row in enumerate(self.data)]
		if self.batch_size > 0:
			batches = chunk_list(rows, self.batch_size)
		# If batch_size = 0, use a single batch
		else:
			batches = [rows]
		return batches

	async def make_requests_async(self, rate_limit: int = 25, batch_size: int = 1000, serialize_return: bool = False):
		self.batch_size = batch_size
		self.serialize_return = serialize_return
		# Context manager for throttling the requests
		self.throttler = Throttler(rate_limit)

		batches = self.make_batch()
		for j, batch in enumerate(batches):
			try:
				print(f'Running batch {j+1}...')
				await self._gather_requests(batch)
				self.do_after_requests(iteration=j)
			except Exception as e:
				print(f'Exception encountered on batch {j+1}. Proceeding to next batch.')
				#continue
				raise # For debugging
		print('All requests completed.')
		return self
	
	def make_requests(self, rate_limit: int = 25, batch_size: int = 1000, serialize_return: bool = False):
		'''Schedules asynchronous requests in batches.
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
		batches = self.make_batch()
		# Run in batches, pausing between each to update the data
		for j, batch in enumerate(batches):
			try:
				print(f'Running batch {j+1}...')
				asyncio.run(self._gather_requests(batch))
				self.do_after_requests(iteration=j)
			except Exception as e:
				print(f'Exception encountered on batch {j+1}. Proceeding to next batch.')
				#continue
				raise # For debugging
		print('All requests completed.')
		return self