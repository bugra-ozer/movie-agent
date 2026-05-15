import pathlib as pl

#CI

mock_url='127.0.0.1:8000'
mock_content_length={'Content-Length':'32768'} #dict
mock_fake_path=pl.Path(__file__).parent / 'fake_path'