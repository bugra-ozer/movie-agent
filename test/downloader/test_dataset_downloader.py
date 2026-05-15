import io
import pytest
from unittest.mock import MagicMock
from constant import constants_dev as cons_dev
from unittest import mock
from downloader import downloader

patch = mock.patch

@patch('downloader.downloader.open')
@patch('downloader.downloader.requests.get')
def test_download_write_chunks_b(mock_get, mock_open):
    """Test 8192 byte chunks are written correctly."""
    unit = downloader.DatasetDownloader()
    mock_file = io.BytesIO()
    url = cons_dev.mock_url #constant
    mock_response=MagicMock()
    mock_response.status_code=200
    mock_response.headers=cons_dev.mock_content_length
    mock_response.iter_content.return_value=cons_dev.mock_data_response_bytes
    mock_get.return_value=mock_response
    mock_open.return_value.__enter__.return_value=mock_file
    unit._download_file(url, mock_file)
    assert mock_file.getvalue() == b''.join(mock_response.iter_content.return_value)

@patch('downloader.downloader.open')
@patch('downloader.downloader.gzip.open')
def test_decompress_downloaded(mock_gzip, mock_open):
    """Test decompression of downloaded file(s)."""
    unit = downloader.DatasetDownloader()
    mock_source = MagicMock()
    mock_dest = MagicMock()
    mock_dest.exists.return_value = True #destination look-up
    mock_source.exists.return_value = True #source look-up
    mock_source.stat.return_value.st_size = 100 #for tqdm
    compressed_data = MagicMock()
    compressed_data.read.side_effect = cons_dev.mock_data_comp_bytes #bytes for iter function, when returned byte sentinel, function ends
    compressed_data.fileobj.tell.return_value=100 #for tqdm
    mock_gzip.return_value.__enter__.return_value=compressed_data
    dest_file = io.BytesIO()
    mock_open.return_value.__enter__.return_value = dest_file
    unit._decompress_file(mock_source,mock_dest)
    assert dest_file.getvalue() == b'compressed'