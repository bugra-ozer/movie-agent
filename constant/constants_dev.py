#CI

mock_url='127.0.0.1:8000'
mock_path='giber/'
mock_content_length={'Content-Length':'32768'} #dict
mock_data_comp_bytes=[b'compressed', b'']
mock_data_response_bytes=([b'a' * 8192, b'b' * 8192, b'c' * 8192, b'd' * 8192])
dummy_dataframe_columns_one=['IMDBid', 'Title']
dummy_dataframe_columns_two=['IMDBid', 'Genre']
dummy_dataframe_data=[['4924', 'Shawshank Redemption'], ['2134', 'Rebellion']]