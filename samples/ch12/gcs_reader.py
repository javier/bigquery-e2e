#!/usr/bin/python2.7
# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.

'''Helper class to download GCS files.

This module contains code to read files from Google Cloud Storage.
Usage:
  gcs_reader = GcsReader(gcs_bucket, download_dir)
  gcs_reader.read(gcs_object)
will read the GCS path gs://gcs_bucket/gcs_object and download
it to the directory download_dir. If download_dir is not specified,
the file will just be checked for existence and not actually downloaded.
'''

import os
import sys

# Imports from the Google API client:
from apiclient.errors import HttpError
from apiclient.http import MediaIoBaseDownload

# Imports from local files in this directory:
import auth

# Number of bytes to download per request.
CHUNKSIZE = 1024 * 1024

class GcsReader:
  '''Reads files from Google Cloud Storage.

  Verifies the presence of files in Google Cloud Storage. Will download
  the files as well if download_dir is not None.
  '''

  def __init__(self, gcs_bucket, download_dir=None):
    self.gcs_service = auth.build_gcs_client()
    self.gcs_bucket = gcs_bucket
    self.download_dir = download_dir

  def make_uri(self, gcs_object):
    '''Turn a bucket and object into a Google Cloud Storage path.'''
    return 'gs://%s/%s' % (self.gcs_bucket, gcs_object)

  def check_gcs_file(self, gcs_object):
    '''Returns a tuple of (GCS URI, size) if the file is present.'''
    try:
      metadata = self.gcs_service.objects().get(
          bucket=self.gcs_bucket, object=gcs_object).execute()
      uri = self.make_uri(gcs_object)
      return (uri, int(metadata.get('size', 0)))
    except HttpError, err:
      # If the error is anything except a 'Not Found' print the error.
      if err.resp.status <> 404:
        print err
      return (None, None)

  def make_output_dir(self, output_file):
    '''Creates an output directory for the downloaded results.'''
    output_dir = os.path.dirname(output_file)
    if os.path.exists(output_dir) and os.path.isdir(output_dir):
      # Nothing to do.
      return
    os.makedirs(output_dir)

  def complete_download(self, media):
    while True:
      # Download the next chunk, allowing 3 retries.
      _, done = media.next_chunk(num_retries=3)
      if done: return

  def download_file(self, gcs_object):
    '''Downloads a GCS object to directory download_dir.'''
    output_file_name = os.path.join(self.download_dir, gcs_object)
    self.make_output_dir(output_file_name)
    with open(output_file_name, 'w') as out_file:
      request = self.gcs_service.objects().get_media(
          bucket=self.gcs_bucket, object=gcs_object)
      media = MediaIoBaseDownload(out_file, request,
                                  chunksize=CHUNKSIZE)

      print 'Downloading:\n%s to\n%s' % (
          self.make_uri(gcs_object), output_file_name)
      self.complete_download(media)

  def read(self, gcs_object):
     '''Read the file and returns the file size or None if not found.'''
     uri, file_size = self.check_gcs_file(gcs_object)
     if uri is None:
       return None
     print '%s size: %d' % (uri, file_size)
     if self.download_dir is not None:
       self.download_file(gcs_object)
     return file_size

def main(argv):
  if len(argv) == 0:
     argv = ['bigquery-e2e',
             'shakespeare.json',
             '/tmp/bigquery']
  if len(argv) < 2:
    # Wrong number of args, print the usage and quit.
    arg_names = [sys.argv[0],
                 '<gcs_bucket>',
                 '<gcs_object>',
                 '[output_directory]']
    print 'Usage: %s' % (' '.join(arg_names))
    print 'Got: %s' % (argv,)
    return
  gcs_bucket = argv[0]
  download_dir = argv[2] if len(argv) > 2 else None

  gcs_reader = GcsReader(gcs_bucket=gcs_bucket,
                         download_dir=download_dir)
  gcs_reader.read(argv[1])

if __name__ == "__main__":
    main(sys.argv[1:])
