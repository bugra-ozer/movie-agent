from .test_data import data as data
import pandas as pd
import datetime
import logging
import pathlib as pl
from pandas import Series
import json

logger=logging.getLogger(__name__)

class MoviePicker():
    """Algorithmic class that takes constrained data, outputs the best suitable movie."""
    def __init__(self, candidates:pd.DataFrame, previously_recommended:set=None):
        if previously_recommended is None: previously_recommended=set()
        self.raw_data=candidates.copy()
        self.data=candidates.copy()
        self.date=datetime.date.today()
        self.previously_recommended=previously_recommended
        self.picks=[]
        self._convert_dtypes()

    def recommend(self):
        """Orchestrator method for building and processing the candidates and giving output."""
        self._build_score() #modifies self.data and adds scores as new columns
        self._pick_n_movie(3)
        return self

    def _convert_dtypes(self):
        """For numerical operations, convert columns to appropriate primitive type."""
        self.data['Number of Votes']=self.data['Number of Votes'].astype(int)
        self.data['Average Rating']=self.data['Average Rating'].astype(float)
        self.data['Published']=self.data['Published'].astype(int)

    def _build_score(self):
        """Builds bayesian algorithm taking release year, number of votes and average rating into account."""
        bayes_scores = []
        decay_factors = []
        adjusted_scores = []
        m=self.data['Number of Votes'].mean()
        c=self.data['Average Rating'].mean()
        for index, movie in self.data.iterrows():
            b_score=self._calculate_bayesian_score(movie, m, c)
            d_factor=self._calculate_decay_factor(movie)
            a_score=b_score*d_factor
            bayes_scores.append(b_score)
            decay_factors.append(d_factor)
            adjusted_scores.append(a_score)
        self.data['Bayes Score'] = bayes_scores
        self.data['Decay Factor'] = decay_factors
        self.data['Adjusted Score'] = adjusted_scores
        self.data['Date'] = self.date
        return self
    
    def _pick_n_movie(self, n):
        """From score populated df, picks n amount of movies."""
        candidates=self.data.sort_values('Adjusted Score', ascending=False)
        for hashable, value in candidates.iterrows():
            if value['IMDBid'] not in self.previously_recommended and len(self.picks) < n:
                self.picks.append({'IMDBid': value['IMDBid'], 'Date': self.date})
            elif len(self.picks)>=n:
                break
        return self.picks

    @staticmethod
    def _calculate_bayesian_score(movie, m, c):
        """
        Calculate bayesian score for a single movie.

        Args:
            movie: Single pandas row with all movie details.
            m: Mean of number of votes in entire df.
            c: Mean of average rating in entire df.
        Returns:
            Weighted Bayesian score as a float.
        """
        v=movie['Number of Votes']
        r=movie['Average Rating']
        bayes_score=(v/(v+m)) * r + (m/(v+m) * c)
        return bayes_score
    
    @staticmethod
    def _calculate_decay_factor(movie):
        """Calculate decay factor for single movie."""
        years_old=datetime.date.today().year-int(movie['Published'])
        if years_old<10:
            decay_factor=0.9997**years_old
        elif years_old<15:
            decay_factor=0.9996**years_old
        elif years_old<20:
            decay_factor=0.9995**years_old
        elif years_old<30:
            decay_factor=0.9994**years_old
        elif years_old<45:
            decay_factor=0.9993**years_old
        else:
            decay_factor=0.9992**years_old
        return decay_factor

class MovieFileOperator():
    """Class that handles file operations for orchestrator class for caching."""
    def __init__(self, json_cfg:str="file_operations.json"):
        """Store properties and set configuration parsing."""
        self.concat=None
        self.data_store={}
        self.json_cfg=json_cfg
        self.config_dir='config'
        self.config_dict:dict=self._load_config()
        self.path=None

    def save_all_file(self):
        """Process saving all files."""
        for file in self.data_store:
            self._save_file(file)
        return self

    def load_all_file(self):
        """Load and clear duplicates from all saved files."""
        self._load_memory()
        self._clear_memory_dupli()

    def _clear_memory_dupli(self):
        """Drop duplicates from all loaded files."""
        for file_name, df in self.data_store.items():
            self.data_store[file_name]=df.drop_duplicates()

    def _load_memory(self):
        """Load all files or reset it to given fallback property in config file."""
        for file_name, file_config in self.config_dict.items():
            file=self._load_file(file_name)
            if not isinstance(file, pd.DataFrame):
                file=pd.DataFrame(columns=file_config['fallback'])
            else:
                pass
            self.data_store[file_name]=file
        return True

    def concat_file(self, concat:dict=None):
        """Concat dataframes for expanding files given in file_operations.json.

        Args:
            concat: dict that maps from file names to their update.
            """
        self.concat=concat
        if self.concat is not None:
            for file, value in self.concat.items():
                if file in self.data_store:
                    self.data_store[file]=pd.concat(objs=[self.data_store[file], value], ignore_index=True)
        return self

    def _save_file(self, file: str):
        """Save file to internal config path."""
        if file in self.data_store:
            self.data_store[file].to_parquet(str(self.config_dict[file]['path']))
        return self

    def _load_file(self, file:str):
        """Load file from internal config path."""
        try:
            file=pd.read_parquet(str(self.config_dict[file]['path']))
        except FileNotFoundError:
            return False
        except ValueError:
            return False
        return file

    def _load_config(self):
        """Load configuration file for file operations."""
        try:
            with open(pl.Path(__file__).parent/self.config_dir/self.json_cfg, "r") as f:
                config_dict=json.load(f)
        except ValueError:
            raise Exception('Failed to open .json config.')
        except FileNotFoundError:
            raise Exception('Failed to find .json config.')
        for key, value in config_dict.items():
            try:
                value['path'] = pl.Path(__file__).parent.parent / value['path']
            except KeyError:
                raise ValueError(f'Failed to find path for {key}')
        return config_dict

if __name__ == '__main__':

    candidates_df=pd.DataFrame(data)
    file_op=MovieFileOperator()
    file_op.load_all_file()
    previous_ids=set(file_op.data_store.get('previous_data', pd.DataFrame()).get('IMDBid', []))
    movie_picker=MoviePicker(candidates_df, previous_ids)
    movie_picker.recommend()
    file_op.concat_file({'previous_data': pd.DataFrame(movie_picker.picks), 'bayesian_data': movie_picker.data})
    file_op.save_all_file()