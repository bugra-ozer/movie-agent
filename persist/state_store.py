import pandas as pd
import pathlib as pl
import json
from cons import constansts as cons

class StateStore():
    """Class that handles file operations for orchestrator class for caching."""
    def __init__(self, json_cfg:str="state.json"):
        """Store properties and set configuration parsing."""
        self.concat=None
        self.data={}
        self.json_cfg=json_cfg
        self.config_dir='config'
        self.config_dict:dict=self._load_config()
        self.path=None

    def save_all_files(self):
        """Process saving all files."""
        for file in self.data:
            self._save_file(file)
        return self

    def load_all_files(self):
        """Load and clear duplicates from all saved files."""
        self._load_memory()
        self._clear_memory_dupli()

    def _clear_memory_dupli(self):
        """Drop duplicates from all loaded files."""
        for file_name, df in self.data.items():
            self.data[file_name]=df.drop_duplicates()

    def _load_memory(self):
        """Load all files or reset it to given fallback property in config file."""
        for file_name, file_config in self.config_dict.items():
            file=self._load_file(file_name)
            if not isinstance(file, pd.DataFrame):
                file=pd.DataFrame(columns=file_config[cons.FALLBACK_KEY])
            else:
                pass
            self.data[file_name]=file
        return True

    def concat_file(self, concat:dict=None):
        """Concat dataframes for expanding files given in state.json.

        Args:
            concat: dict that maps from file names to their update.
            """
        self.concat=concat
        if self.concat is not None:
            for file, value in self.concat.items():
                if file in self.data:
                    self.data[file]=pd.concat(objs=[self.data[file], value], ignore_index=True)
        return self

    def _save_file(self, file: str):
        """Save file to internal config path."""
        if file in self.data:
            self.data[file].to_parquet(str(self.config_dict[file][cons.PATH_KEY]))
        return self

    def _load_file(self, file:str):
        """Load file from internal config path."""
        try:
            file=pd.read_parquet(str(self.config_dict[file][cons.PATH_KEY]))
        except FileNotFoundError:
            return False
        except ValueError:
            return False
        return file

    def _load_config(self):
        """Load configuration file for file operations."""
        try:
            with open(pl.Path(__file__).parent.parent/self.config_dir/self.json_cfg, "r") as f:
                config_dict=json.load(f)
        except ValueError:
            raise Exception('Failed to open .json config.')
        except FileNotFoundError:
            raise Exception('Failed to find .json config.')
        for key, value in config_dict.items():
            try:
                value[cons.PATH_KEY] = pl.Path(__file__).parent.parent / value[cons.PATH_KEY]
            except KeyError:
                raise ValueError(f'Failed to find path for {key}')
        return config_dict