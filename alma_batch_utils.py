EXL_BASE = 'https://api-na.hosted.exlibrisgroup.com'

def convert_column_case(columns: list):
	'''Columns should be a list of strings (or other iterator) corresponding to column names. 
	Returns snake-cased versions of these strings.'''
	return [c.lower().replace(' ', '_') for c in columns]

def chunk_list(items, n): 
	'''Create a chunked list of size n. Last segment may be of length less than n.''' 
	for i in range(0, len(items), n):  
		yield items[i:i + n] 

class InvalidParameters(Exception):
	'''Exception raised when the user-supplied data lacks the appropriate parameters for the API.'''

class MissingConfiguration(Exception):
	'''Exception raised when the configuration file lacks required elements.'''

class APIException(Exception):
	'''Catches internal API errors from the server side.'''