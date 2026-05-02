import json
import requests
import pathlib as pl
import gzip
from constant import constansts as cons
from tqdm import tqdm
import logging

logger = logging.getLogger(__name__)

class DatasetDownloader():
    """Class for making HTTP requests"""
    def __init__(self, json_cfg:str="dataset.json"):
        self.json_cfg=json_cfg
        self.config_dir=cons.CONFIG_DIR
        self.response=None
        self.file= ""

    def _load_config(self):
        """Load configuration file for file operations."""
        pl.Path.mkdir(pl.Path(__file__).parent.parent/self.config_dir, parents=True, exist_ok=True)
        try:
            with open(pl.Path(__file__).parent.parent/self.config_dir/self.json_cfg, "r") as f:
                return json.load(f)
        except ValueError:raise Exception('Failed to open .json config.')
        except FileNotFoundError:raise Exception('Failed to find .json config.')

    def _download_file(self, url, destination, stream=True):
        """Make http request and write to destination."""
        self.response=requests.get(url, stream=stream)
        total=(float(self.response.headers.get('Content-Length')))
        if self.response.status_code == 200:
            bar:tqdm=tqdm(total=total, unit='B', unit_scale=True, bar_format='\033[37m{l_bar}\033[32m{bar}\033[37m{r_bar}',ncols=120, desc=f'downloading dataset as {self.file}')
            with open(destination, "wb") as f:
                for chunk in self.response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    bar.update(len(chunk))
                return True
        else:raise Exception(f'Download failed. Server threw status code: {self.response.status_code}.')

    def _decompress_file(self, source, destination):
        """Decompress .gz files, clean up and save to destination."""
        if source.exists():
            comp_size = pl.Path(source).stat().st_size  # in bytes
            previous = 0
            bar:tqdm=tqdm(total=comp_size, unit='B', unit_scale=True, bar_format='\033[37m{l_bar}\033[32m{bar}\033[37m{r_bar}', ncols=120, desc=f'decompressing dataset {self.file}') #1 MB packets
            with gzip.open(source, "rb") as f_in, open(destination, "wb") as f_out:
                for chunk in iter(lambda: f_in.read(1000000), b""):
                    f_out.write(chunk)
                    current=f_in.fileobj.tell() # noqa
                    bar.update(current-previous)
                    previous=current
            self._delete_file(source) #delete compressed file (.tsv.gz)
        else:raise Exception(f'Decompression failed. {source} not found.')

    def _delete_file(self, source):
        """Delete file"""
        if source.exists():
            pl.Path(source).unlink()
        return self

    def fetch_imdb_dataset(self, requests_config: dict):
        """Read from config, download via HTTP, decompress and clean up."""
        for key, value in requests_config.items():
            url=value['url']
            folder=value['folder']
            self.file=value['filename']
            dec_file=pl.Path(__file__).parent.parent / 'data/' / value['dec_filename']
            tsv_gz_dataset=pl.Path(__file__).parent.parent / folder / self.file
            self._download_file(url, tsv_gz_dataset) #http request to IMDB server
            self._decompress_file(tsv_gz_dataset, dec_file) #decompress and delete old

    def main(self):
        """Set up orchestration for main function."""
        requests_config=self._load_config()
        self.fetch_imdb_dataset(requests_config)

if __name__ == '__main__':
    dataset_obj=DatasetDownloader()
    dataset_obj.main()
