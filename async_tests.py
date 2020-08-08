import asyncio
import aiohttp
from asyncio import Queue
from asyncio_throttle import Throttler
import random

N = 10 # Number of API calls to simulate

def create_api():
	api = {}
	# Each iteration of the outer loop simulates a new API query 
	for i in range(N):
	   # There may be up to 100 results per call
	   total_results = random.randint(1, 101)
	   # Each result contains the query number and an offset
	   results = [{'query': i, 
			   'offset': j} for j in range(total_results)]
	   api[i] = results
	return api

# This function serves as the API endpoint, returning up to 10 results per page
def api_call(query, offset=0):
	data = api[query]
	page = data[offset:offset+10]
	return {'total_results': len(data),
			   'results': page}

class AsyncBatch():
	
	def __init__(self, data, api_func):
		# Data should be a list of values to pass to api_func, which should return a result containing two keys: 
			# total_results = integer 
			# and result = list of dicts
		self.data = data
		self.api_func = api_func
		self.results = [] # For accumulating results
		self.errors = [] # For accumualating errors
		#self.queue = Queue() # Async queue
	
	async def worker(self, queue):
		# Consumer uses an infinite loop to run until there are no more tasks for it
		while True:
			row, offset = await queue.get() # Get a row index to process and an offset
			batch = self.api_func(row, offset)
			print(f'Processing batch {(row, offset)}')
			await asyncio.sleep(random.random()) # Simulating latency
			queue.task_done()
			# If this is the first page, check for more pages
			if offset == 0:
				total_results = batch['total_results']
				for j in range(10, total_results, 10): # Add another job to the queue for each additional page
					print(f'Adding batch {(row, j)}')
					await queue.put((row, j))
			self.results.append(batch['result'])
	
	async def run_queue(self, num_workers=25):
		queue = Queue()
		for row in self.data:
			queue.put_nowait((row, 0)) # Initialize the queue with the initial call to each endpoint (offset=0)
		tasks = [asyncio.create_task(self.worker(queue)) for i in range(num_workers)] # Create the tasks
		# Main call that blocks until all queue items have been marked done
		await queue.join()
		# Cancel our worker tasks.
		for task in tasks:
			task.cancel()
		# Wait until all worker tasks are cancelled.
		await asyncio.gather(*tasks, return_exceptions=True)
		return self.results

	def main(self):
		asyncio.run(self.run_queue())

if __name__ == '__main__':
	api = create_api()
	ab = AsyncBatch(list(api.keys()), api_call)
	results = ab.main()
