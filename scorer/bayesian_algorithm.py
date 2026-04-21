import datetime
import pandas as pd
import logging
from persist import state_store
from cons import constansts as cons

logger=logging.getLogger(__name__)

class MovieScorer():
    """Algorithmic class that takes transformed data, outputs the top n results based on bayesian score."""
    def __init__(self, candidates:pd.DataFrame):
        self.raw_data=candidates.copy()
        self.data=candidates.copy()
        self.date=datetime.date.today()
        self._convert_dtypes()

    def score(self):
        """Orchestrator method for building and processing the candidates and giving output."""
        self._build_score() #modifies self.data and adds scores as new columns
        return self

    def _convert_dtypes(self):
        """For numerical operations, convert columns to appropriate primitive type."""
        self.data[cons.NUMBER_OF_VOTES_COLUMN]=self.data[cons.NUMBER_OF_VOTES_COLUMN].astype(int)
        self.data[cons.AVERAGE_RATING_COLUMN]=self.data[cons.AVERAGE_RATING_COLUMN].astype(float)
        self.data[cons.PUBLISHED_COLUMN]=self.data[cons.PUBLISHED_COLUMN].astype(int)

    def _build_score(self):
        """Builds bayesian algorithm taking release year, number of votes and average rating into account."""
        bayes_scores = []
        decay_factors = []
        adjusted_scores = []
        m=self.data[cons.NUMBER_OF_VOTES_COLUMN].mean()
        c=self.data[cons.AVERAGE_RATING_COLUMN].mean()
        for index, movie in self.data.iterrows():
            b_score=self._calculate_bayesian_score(movie, m, c)
            d_factor=self._calculate_decay_factor(movie)
            a_score=b_score*d_factor
            bayes_scores.append(b_score)
            decay_factors.append(d_factor)
            adjusted_scores.append(a_score)
        self.data[cons.BAYES_SCORE_COLUMN] = bayes_scores
        self.data[cons.DECAY_FACTOR_COLUMN] = decay_factors
        self.data[cons.ADJUSTED_SCORE_COLUMN] = adjusted_scores
        self.data[cons.DATE_COLUMN] = self.date
        return self

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
        v=movie[cons.NUMBER_OF_VOTES_COLUMN]
        r=movie[cons.AVERAGE_RATING_COLUMN]
        bayes_score=(v/(v+m)) * r + (m/(v+m) * c)
        return bayes_score
    
    @staticmethod
    def _calculate_decay_factor(movie):
        """Calculate decay factor for single movie."""
        years_old=datetime.date.today().year-int(movie[cons.PUBLISHED_COLUMN])
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