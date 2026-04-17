import pandas as pd
import pathlib as pl
import json
import logging
from ui import cli as ui
from networking import client as client
from scorer import bayesian_algorithm as bayes
from logs import log_handler
from cons import constansts as cons

log_handler.LogHandler()
logger=logging.getLogger(__name__)

class MovieAgent():
    """Container class for managing the state of the dataframe"""

    def __init__(self):
        self.data=pd.DataFrame()
        self.raw_data=None
        self.condition=None
        usecols=cons.COLUMNS_TO_KEEP_LEGACY
        self.data_pipeline=DataPipeline(usecols=usecols)

    def build_agent(self):
        """Orchestrates the flow of code for easy readability."""
        self.data=self.data_pipeline.main()
        self.raw_data=self.data #set raw dataframe before clearing main dataframe
        self._purge_data()
        self.mutate_dataframe()
    
    def rename_columns(self, columns:dict):
        """Make columns in imdb .tsv files more readable and intuitive"""
        try:    
            self.data:pd.DataFrame=self.data.rename(columns=columns)
            return self
        except KeyError as e: raise KeyError(f"Column not found to rename: {e}") from e

    def select_columns(self, *args:str):
        """Internal limitation the data with given columns.
        Call data to be mutated with given arguments.

        *args: Names of the columns to limit"""
        columns_to_limit=[*args]
        if len(columns_to_limit)>0:self.condition=columns_to_limit
        try:self._apply_column_selection()
        except KeyError as e:raise KeyError(f"With given arguments, column not found: {e}") from e
        return self

    def mutate_dataframe(self):
        """Setup imdb data and call on files to be modified"""
        self.rename_columns(cons.COLUMN_RENAME_DICT) #rename the columns to be more intuitive
        self.select_columns(*cons.COLUMNS_TO_KEEP) #Mutate only wanted columns
        return self

    def _apply_column_selection(self):
        """Based on condition, mutate the data to display"""
        if self.data is None:
            raise ValueError(f'Failed to apply condition to the file.')
        if self and self.condition:
            self.data=self.data[self.condition]
            self.condition=None #Consume condition after applying for predictable code
        return self

    def _filter_rows(self, column:str, mask):
        """Remove unwanted records internally."""
        data:pd.DataFrame
        data=self.data[self.data[column]==mask]
        return data

    def _purge_data(self):
        """Remove excessive items with low votes, empty primary titles and genres."""
        self.data = self._filter_rows(cons.TITLE_TYPE_COLUMN_LEGACY, 'movie')  # remove anything else than movie in records
        self.data = self.data[(self.data[cons.PRIMARY_TITLE_COLUMN_LEGACY].notna()) & (self.data[cons.GENRE_COLUMN_LEGACY].notna()) & (self.data[cons.NUMBER_OF_VOTES_COLUMN_LEGACY] > 5000)]  # Purge unsuitable titles
        self.data.dropna(subset=[cons.PUBLISHED_COLUMN_LEGACY], inplace=True)
        return self

class DataPipeline():
    """Orchestrator class owns DataLoader for external pandas dataframe operations."""

    def __init__(self, usecols=None, json_cfg:tuple=("main.json", "dataset.json")):
        self.config_dir='config'
        self.json_cfg=json_cfg
        self.config_dict={}
        self.base_data_path=None
        self.tsv_configs=[]
        self.data_loader=DataLoader(usecols)
        self.dataset_downloader=client.DatasetDownloader()

    def main(self):
        """"""
        self._load_config()
        self._fetch_paths()
        self._convert_config_pl()
        if self.base_data_path is None:
            raise Exception('Failed to load base data')
        elif not self.tsv_configs:
            raise Exception('Failed to load tsv paths')
        if not pl.Path(self.base_data_path).exists(): #check for base_data, if it exists skip all download dataset operation.
            if any(tsv for tsv in [*self.tsv_configs] if not pl.Path(tsv['path']).exists()): #if file paths are empty orchestrate http request.
                self.dataset_downloader.main()
        return self.build_data()

    def _load_config(self):
        """Load configuration file for file operations."""
        for config_file in self.json_cfg:
            try:
                with open(pl.Path(__file__).parent / self.config_dir / config_file, "r") as f:
                    self.config_dict.update(json.load(f))
            except ValueError:
                raise Exception('Failed to open .json config.')
            except FileNotFoundError:
                raise Exception('Failed to find .json config.')
        return self

    def _convert_config_pl(self):
        """Convert string paths to pathlib.Path objects."""
        for key, value in self.config_dict.items():
            if isinstance(value, dict):
                try:
                    value[cons.PATH_KEY] = pl.Path(__file__).parent / value[cons.PATH_KEY]
                except KeyError:
                    raise ValueError(f'Failed to find path for {key}')

    def _fetch_paths(self):
        """Fetch tsv file and base file paths"""
        for key, value in self.config_dict.items():
            if 'base' in str(key):
                self.base_data_path = value['path']
            if 'imdb' in str(key):
                self.tsv_configs.append(value)
        return self

    def build_data(self):
        """Read if processed file exists, else run operations to initiate one."""
        data_frames=[]
        if pl.Path.exists(self.base_data_path):
            logger.info('loading base data file...')
            data=self.data_loader.read_file(str(self.base_data_path), 'parquet')
        else:
            for tsv in self.tsv_configs:
                logger.info('merging tsv file(s)...')
                data_frames.append(self.data_loader.read_file(str(tsv['path']), 'tsv', usecols=tsv['usecols']))
                self.data_loader.delete_file(tsv['path'])
            data=self.data_loader.merge_dataframes(*data_frames, on=cons.IMDB_ID_COLUMN_LEGACY)
            self.data_loader.save_file(data, self.base_data_path)
        logger.info('file load complete!')
        return data

class DataLoader():
    """Pandas dataframe operations class without any business knowledge"""

    def __init__(self, usecols=None):
        self.data=None
        self.usecols=usecols

    @staticmethod
    def merge_dataframes(*args: pd.DataFrame, on=None):
        """Merge pandas Dataframe objects.

        Args:
            on: specific column to merge on"""
        if on is None:
            raise ValueError('on is required')
        result = args[0]
        if len(args) > 1:
            for i in range(1, len(args)):
                result = result.merge(args[i], on=on)
        return result

    @staticmethod
    def read_file(path:str, file_type:str, usecols=None):
        """Read TSV file from given path

        Args:
            path: read from
            file_type: tsv or csv
            usecols: columns to retain, configured in .json"""
        path = pl.Path(path)
        if file_type.strip().lower() == 'tsv':
            try:
                file = pd.read_csv(path, delimiter='\t', encoding='latin-1', on_bad_lines='skip', na_values='\\N', usecols=usecols)  # Read file
            except Exception as e:
                raise IOError(f"Failed to read CSV/TSV: {e}") from e
        elif file_type.strip().lower() == 'parquet':
            try:
                file = pd.read_parquet(path)  # Read file
            except Exception as e:
                raise IOError(f"Failed to read parquet: {e}") from e
        else:
            raise ValueError(f"Failed to read file: {path}")
        return file

    def save_file(self, file:pd.DataFrame, path):
        """Save file to given path."""
        try:
            file.to_parquet(path)
        except Exception as e:
            raise IOError(f"Failed to save file: {e}") from e
        return self

    def delete_file(self, path):
        """Delete file with given absolute path"""
        if path.exists():
            pl.Path(path).unlink()
        return self

class MovieFilter():
    """Class that internally selects and stores selected movies after user filter is applied.\n
    Carries MovieAgent dataframe and MovieAgentBuilder raw_data internally"""

    def __init__(self, df:pd.DataFrame, filter_tools:list[list[str]]):
        """Requires movieAgentBuilder object to initialize
        filter_tools: column_name, operatr, value to be filtered"""
        self.df=df.copy()
        self.sort_column = None
        self.sort_ascending = True
        self.genres=self.df[cons.GENRE_COLUMN].str.lower().str.split(',').explode().unique()
        self.filter_tools=filter_tools
        self.result=self.get_movies(self.filter_tools)

    def get_movies(self, filter_tools:list[list[str]]):
        """Retrieve list of movies with user filter applied.\n
        filter_tools: Filter params: column_name, operator, value such as: Average Rating, >, 7
        """
        #Check if column_name, operatr, value valid in dataframe
        candidates=self.apply_filters(filter_tools)
        self.configure_sort(cons.AVERAGE_RATING_COLUMN, False)
        result=self.sort_candidates(candidates)
        return result
    
    @staticmethod
    def _parse_filter_tools(filter_tools:list[str]):
        """Based on the argument length, assign variables to apply filters.
        This is needed for allowing user to type in titles and genres without explicit operations."""
        operatr=None
        if len(filter_tools)==3:
            column_name, operatr, value=filter_tools
        elif len(filter_tools)==2:
            value=filter_tools[1]
            column_name=filter_tools[0]
        elif len(filter_tools)==1:
            value=filter_tools[0]
            column_name=None
        else:
            return False
        return column_name, operatr, value

    def apply_filters(self, filter_tools:list[list[str]]):
        """Unpacks filter tools and applies each filter in it manually."""
        candidates=self.df
        for filters in filter_tools:
            column_name, operatr, value=self._parse_filter_tools(filters)
            candidates=self._apply_filter(candidates, column_name, operatr, value)
        return candidates

    def _apply_filter(self, candidates, column_name:str, operatr:str, value:str):
        """Apply appropriate value as filter to column_name."""
        value=self._convert_value(candidates, column_name, value)
        condition=self._build_filter_condition(candidates, column_name, operatr, value)
        candidates=candidates[condition]
        return candidates

    @staticmethod
    def _convert_value(candidates, column_name:str, value:str):
        """Convert value if applicable to its column's value type."""
        new_value=value
        if column_name is None:
            return value
        if pd.api.types.is_numeric_dtype(candidates[column_name]):
            try: new_value=int(value)
            except ValueError: 
                try: new_value=float(value) 
                except ValueError:
                    raise ValueError
        return new_value

    def _build_filter_condition(self, candidates, column_name:str, operator:str, value:str):
        """Build pandas condition based on column, operator, and value\n
        contains: Movies tend to have more than one genre. To avoid fixed listing, you can set this setting to true to for instance: your horror movie search includes movies that have horror and action etc."""
        if pd.api.types.is_numeric_dtype(candidates[column_name]):
            try:
                condition=self._build_numeric_condition(candidates, column_name, operator, value)
            except ValueError:
                raise ValueError(f'Filter operation failed. One of the following is invalid: {column_name},{operator},{value}')
        elif value is not None:
            condition=self._build_string_condition(candidates, column_name, value)
        else: raise ValueError(f'Operation failed. One of the following is invalid: {column_name},{operator},{value}')
        return condition

    @staticmethod
    def _build_numeric_condition(candidates, column_name:str, operator:str, value:str):
        condition = candidates[column_name]
        if operator == ">":
            return condition > value
        elif operator == "<":
            return condition < value
        elif operator == "<=":
            return condition <= value
        elif operator == ">=":
            return condition >= value
        elif operator == "==":
            return condition == value
        else:
            return ValueError

    @staticmethod
    def _build_string_condition(candidates, column_name:str, value):
        """Helper function that checks data for broader string matches, not exact."""
        condition=candidates[column_name].str.lower().str.contains(value)
        return condition
    
    def configure_sort(self, column:str, ascend=True):
        """Set sort properties of MoviePicker object based on column parameter."""
        self.sort_ascending=ascend
        self.sort_column=column

    def sort_candidates(self, candidates:pd.DataFrame):
        """Apply sorting properties with respect to candidates parameter."""
        if self.sort_column is not None:
            sorted_candidates=candidates.sort_values(self.sort_column, ascending=self.sort_ascending)
        else:
            sorted_candidates=candidates
        return sorted_candidates

class MovieService():
    """Movie recommendation service that runs end to end."""

    def __init__(self):
        self.file_operator = bayes.MovieFileOperator()  # For bayesian calculations and caching
        self.file_operator.load_all_files()
        self.agent = MovieAgent()
        self.agent.build_agent()
        previous_ids = set(self.file_operator.data_store.get(cons.PREVIOUS_DATA_KEY, pd.DataFrame()).get(cons.IMDB_ID_COLUMN, []))
        self.bayes=bayes.MovieScorer(self.agent.data, previous_ids)
        self.data=self.bayes.data
        self.bayes.score()

    def recommend(self, filter_tools:list[list[str]]):
        """"""
        candidates = MovieFilter(self.data, filter_tools).result
        for i in candidates.to_dict(orient='records'):
            print(i)
            input()
        #TODO PICK TOP N from candidates and CHANGE self.bayes.picks on the following line
        self.file_operator.concat_file({cons.PREVIOUS_DATA_KEY: pd.DataFrame(self.bayes.picks), cons.BAYESIAN_DATA_KEY: self.bayes.data})
        self.file_operator.save_all_files()
        return candidates.to_dict(orient='records')

class AppManager():
    """Main orchestrator that assembles prereq for service.
    TODO: clean up AppManager from MovieService tasks, and make MovieService live in AppManager.
        -cli and api decision lives and decided at this level.
        -service does not care of cli and api difference."""
    
    def __init__(self):
        try:
            self.movie_fetcher=MovieService()
            self.cli=ui.UserInterface()
            self._main()
        except Exception as e: # noqa
            logger.exception(f'Unhandled exception.')

    def _main(self):
        """"""
        self.cli.start()
        self.filter_tools:list[list[str]]=self.cli.all_filter_tools
        self.movie_fetcher.recommend(self.filter_tools)

if __name__ == '__main__':
    AppManager()
    
    '''
    NOTE:  
    
    '''

    


