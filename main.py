import pandas as pd
import pathlib as pl
import pdb
import time
from ui.user_interface import UserInterface
from networking import handle_datasets as hd

class MovieAgent():
    """Main object responsible for clearing, fixing columns and internally consume columns.\n
    Carries data internally."""
    def __init__(self):
        self.data:pd.DataFrame=pd.DataFrame()
        self.condition=None

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

    def _apply_column_selection(self):
        """Based on condition, mutate the data to display"""
        if self.data is None:
            raise ValueError(f'Failed to apply condition to the file.')
        if self and self.condition:
            self.data=self.data[self.condition]
            self.condition=None #Consume condition after applying for predictable code
        return self

class MovieAgentBuilder():
    """Orchestral class for managing callable hierarchy and the internal state of the object MovieAgent.\n
    Orchestrates the MovieAgent object(s) only."""

    def __init__(self):
        self.raw_data=None
        self.movie_agent=MovieAgent()
        self.preprocessed_path = pl.Path(__file__).parent / 'data' / 'preprocessed_data.parquet'
        self.file_assister = DataLoader(self.movie_agent.data, self.preprocessed_path)
        self._build_agent()

    def _build_agent(self):
        """Orchestrates the flow of code for easy readability."""
        self.mutate_dataframe(self.movie_agent)

    def mutate_dataframe(self, movie_agent:MovieAgent):
        """Setup imdb data and call on files to be modified"""
        movie_agent.rename_columns({'tconst': 'IMDBid','averageRating': 'Average Rating',
                                    'numVotes': 'Number of Votes','titleType': 'Title Type',
                                    'primaryTitle': 'Primary Title','originalTitle': 'Original Title','isAdult': 'Is Adult',
                                    'startYear': 'Published','endYear': 'End Year','runtimeMinutes': 'Run Time Minutes','genres': 'Genre'}) #rename the columns to be more intuitive
        movie_agent.select_columns('IMDBid', 'Average Rating', 'Number of Votes', 'Primary Title', 'Published', 'Genre') #Mutate only wanted columns
        self.raw_data=self.file_assister.raw_data
        return movie_agent

class DataLoader():

    def __init__(self, data_frame:pd.DataFrame, path):
        self.data=None
        self.raw_data=data_frame
        self.data_frame=data_frame
        self.preprocessed_path=path
        self.main()

    def main(self):
        self.data=self._run_operations(self.data_frame)
        return self

    def _run_operations(self):
        start = time.time()
        if pl.Path.exists(self.preprocessed_path):
            self.data_frame = pd.read_parquet(self.preprocessed_path)
        else:
            title_imr = pl.Path(__file__).parent / 'data' / 'imdb.title.ratings.tsv'  # imr=id, metadata, rating
            title_basics = pl.Path(__file__).parent / 'data' / 'imdb.title.basics.tsv'  #
            title_basics_df = self.read_tsv_file(str(title_basics))
            title_imr_df = self.read_tsv_file(str(title_imr))
            self.data_frame = self.merge_dataframes(title_imr_df, title_basics_df)  # insert df to be merged
            self.data_frame = self._purge_data()  # retain data quality
            self.data_frame.to_parquet(self.preprocessed_path)
        print(f"Operation runtime: {time.time() - start}")
        return self

    @staticmethod
    def merge_dataframes(*args: pd.DataFrame):
        """Merges .tsv data files. Mutates self.data."""
        result = args[0]
        if len(args) > 1:
            for i in range(1, len(args)):
                result = result.merge(args[i], on='tconst')
        return result

    @staticmethod
    def read_tsv_file(path: str):
        """Read TSV file from given path"""
        path = pl.Path(path)
        try:
            read_tsv = pd.read_csv(path, delimiter='\t')  # Read file
        except Exception as e:
            raise IOError(f"Failed to read CSV: {e}") from e
        return read_tsv

    def _filter_rows(self, column:str, mask):
        """Remove unwanted records internally."""
        data:pd.DataFrame
        data=self.data[self.data[column]==mask]
        return data

    def _purge_data(self):
        """Remove excessive items with low votes, empty primary titles and genres."""
        self.data_frame = self._filter_rows('titleType', 'movie')  # remove anything else than movie in records
        self.data_frame = self.data_frame[(self.data_frame['primaryTitle'].notna()) & (self.data_frame['genres'].notna()) & (self.data_frame['numVotes'] > 5000)]  # Purge unsuitable titles
        return self

class MovieFilter:
    """Class that internally selects and stores selected movies after user filter is applied.\n
    Carries MovieAgent dataframe and MovieAgentBuilder raw_data internally"""

    def __init__(self, movie_agent_builder:MovieAgentBuilder, filter_tools:list[list[str]]):
        """Requires movieAgentBuilder object to initialize
        filter_tools: column_name, operatr, value to be filtered"""
        self.df=movie_agent_builder.movie_agent.data.copy()
        self.raw_data=movie_agent_builder.raw_data.copy()
        self.condition = None
        self.sort_column = None
        self.sort_ascending = True
        self.user=None
        self.genres=self.df['Genre'].str.lower().str.split(',').explode().unique()
        self.column_map={col.lower(): col for col in self.df.columns}
        self.candidates=self.get_movies(filter_tools)

    def get_movies(self, filter_tools:list[list[str]]):
        """Retrieve list of movies with user filter applied.\n
        n: number of movie recommendation\n
        filter_tools: Filter params: column_name, operator, value such as: Average Rating, >, 7
        """
        #Check if column_name, operatr, value valid in dataframe
        candidates=self.apply_all_filters(filter_tools)
        self.configure_sort('Average Rating', False)
        filtered_candidates=self.sort_candidates(candidates) 
        print('\033c') #Remove previous lines
        print(filtered_candidates.to_string(index=False, max_colwidth=45))
        return filtered_candidates
    
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

    def apply_all_filters(self, filter_tools:list[list[str]]):
        """Unpacks filter tools and applies each filter in it manually."""
        candidates=self.df
        for filters in filter_tools:
            column_name, operatr, value=self._parse_filter_tools(filters)
            self.apply_filter(column_name, operatr, value)
            candidates=candidates[self.condition]
        return candidates

    def apply_filter(self, column_name:str, operatr:str, value:str):
        """Apply appropriate value as filter to column_name."""
        value=self._convert_value(column_name, value)
        if column_name == 'Primary Title' or column_name == 'Genre':candidates=self.df[self._build_filter_condition(column_name, operatr, value, True)] #check for genre to make filter more inclusive.
        else:
            condition=self._build_filter_condition(column_name, operatr, value)
            candidates=self.df[condition]
            self._store_condition(condition)
        return candidates
    
    def _store_condition(self, condition:pd.Series):
        """Store condition property for filtering"""
        if condition is None:
            self.condition=None
        else:
            self.condition=condition
    
    def _convert_value(self, column_name:str, value:str):
        """Convert value if applicable to its column's value type."""
        new_value=value
        if column_name is None:
            return value
        if pd.api.types.is_numeric_dtype(self.df[self.column_map[column_name.lower()]]): 
            try: new_value=int(value)
            except ValueError: 
                try: new_value=float(value) 
                except ValueError:
                    raise ValueError
        return new_value

    def _build_filter_condition(self, column_name:str, operator:str, value:str):
        """Build pandas condition based on column, operator, and value\n
        contains: Movies tend to have more than one genre. To avoid fixed listing, you can set this setting to true to for instance: your horror movie search includes movies that have horror and action etc."""
        if operator is not None:
            condition=self.df[self.column_map[column_name.lower()]]
            if operator == ">":
                return condition>value
            elif operator == "<":
                return condition<value
            elif operator == "<=":
                return condition<=value
            elif operator == ">=":
                return condition>=value
            elif operator == "==":
                return condition==value
            else:
                raise ValueError(f'Filter operation failed. One of the following is invalid: {column_name},{operator},{value}')
        elif value is not None:
            condition=self._build_string_condition(column_name, value)
        else: raise ValueError(f'Operation failed. One of the following is invalid: {column_name},{operator},{value}')
        return condition
    
    def _build_string_condition(self, column_name:str, value):
        """Helper function that checks data for broader string matches, not exact."""
        if column_name is not None: #User is given two strings
            condition=self.df[self.column_map[column_name.lower()]].str.lower().str.contains(value)
        else: #User is given single string
            if value.lower() in self.genres:
                condition=self.df[self.column_map['genre']].str.lower().str.contains(value)
            else:
                condition=self.df[self.column_map['primary title']].str.lower().str.contains(value)
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

class AppManager():
    """Main orchestrator that assembles necessary classes and communication."""
    
    def __init__(self):
        self.builder=MovieAgentBuilder()
        self.CLI=UserInterface() #Expects prompts like Average Rating>5 or Shawshank Redemption
        self._main()
        
    def _main(self):
        self.filter_tools:list[list[str]]=self.CLI.all_filter_tools
        self.advice=MovieFilter(self.builder, self.filter_tools)

if __name__ == '__main__':
    AppManager()
    
    '''
    NOTE:  
    
    '''

    


