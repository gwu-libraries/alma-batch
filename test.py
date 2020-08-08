from alma_batch import AlmaBatch

OPENAPI_BIB = 'https://developers.exlibrisgroup.com/wp-content/uploads/alma/openapi/bibs.json'

def test_bibs():
	bibs_batch = AlmaBatch(OPENAPI_BIB, '/Users/dsmith/Documents/code/alma_batch/test_config.yml')
	bibs_batch.load_csv('/Users/dsmith/Documents/code/alma_batch/alma_item_ids.csv').validate_data().make_requests(batch_size=0, serialize_return=True)
	return bibs_batch.errors, bibs_batch.results

if __name__ == '__main__':
	errors, results = test_bibs()
	print(f'Length of results is {len(results)}')
	print(f'Length of errors is {len(errors)}')
