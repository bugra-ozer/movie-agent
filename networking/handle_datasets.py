import json
import requests
import pathlib as pl
import gzip
from tqdm import tqdm


class DatasetDownloader():

    def __init__(self, json_cfg:str="dataset.json"):
        self.json_cfg=json_cfg
        self.config_dir='config'
        self.response=None
        self.file= ""

    def _load_config(self):
        """
        Load configuration file for file operations.
        """
        try:
            with open(pl.Path(__file__).parent.parent/self.config_dir/self.json_cfg, "r") as f:
                return json.load(f)
        except ValueError:
            raise Exception('Failed to open .json config.')
        except FileNotFoundError:
            raise Exception('Failed to find .json config.')

    def _download_file(self, url, destination, stream=True):
        """
        Open and write dataset.
        """
        self.response=requests.get(url, stream=stream)
        total_chunks=(float(self.response.headers.get('Content-Length'))+8191)//8192
        if self.response.status_code == 200:
            with open(destination, "wb") as f:
                for chunk in tqdm(self.response.iter_content(chunk_size=8192), total=total_chunks, unit='B', unit_scale=True, bar_format='\033[37m{l_bar}\033[32m{bar}\033[37m{r_bar}', ncols=120, desc=f'downloading dataset as {self.file}'):
                    f.write(chunk)
                return True
        else:
            raise Exception(f'Download failed. Server threw status code: {self.response.status_code}.')

    def _decompress_file(self, source, destination):
        if source.exists():
            with gzip.open(source, "rb") as f_in, open(destination, "wb") as f_out:
                for chunk in tqdm(iter(lambda: f_in.read(1000000), b""), unit='B', unit_scale=True, bar_format='\033[37m{l_bar}\033[32m{bar}\033[37m{r_bar}', ncols=120, desc=f'decompressing dataset {self.file}'): #1 MB packets
                    f_out.write(chunk)
                self._delete_compressed_file(source)
        else:
            raise Exception(f'Decompression failed. {source} not found.')

    def _delete_compressed_file(self, source):
        if source.exists():
            pl.Path(source).unlink()
        return self

    def fetch_imdb_dataset(self, requests_config: dict):
        """
        Orchestrate full dataset download.
        """
        for key, value in requests_config.items():
            url=value['url']
            folder=value['folder']
            self.file=value['filename']
            dec_file=pl.Path(__file__).parent.parent / 'data/' / value['dec_filename']
            tsv_gz_dataset=pl.Path(__file__).parent.parent / folder / self.file
            self._download_file(url, tsv_gz_dataset) #http request to IMDB server
            self._decompress_file(tsv_gz_dataset, dec_file) #decompress and delete old

    def main(self):
        requests_config=self._load_config()
        self.fetch_imdb_dataset(requests_config)

if __name__ == '__main__':
    dataset_obj=DatasetDownloader()
    dataset_obj.main()
