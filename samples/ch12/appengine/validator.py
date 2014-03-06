# Defines a validator / transform for the mapreduce params.

def adjust_spec(params):
  params['files'] = params['files'].split()
  params['output_writer'] = {
    'bucket_name': params['output_bucket'],
    'naming_format': 'test/$id-$num'
    }
