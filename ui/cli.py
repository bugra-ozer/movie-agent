import os
import string
import asyncio
import logging
import enum

logger = logging.getLogger(__name__)

class SearchTypes(enum.Enum):
    RATING = 'rating'
    GENRE = 'genre'
    BOTH = 'both'

class UserInterface():
    """Class that provides interface for user-based actions."""

    def __init__(self): #type: ignore
        self.all_filter_tools:list[list[str]]=[]
        self.start()

    def start(self):
        """Prompt user for actions"""
        flag=True
        filter_tools=None
        while flag:
            user_input=self._return_input()
            if self._is_exit(user_input): break
            elif self._is_input_help(user_input): self.display_help(user_input)
            else:
                flag=False
                self._prompt_search()
        return filter_tools

    def _prompt_search(self):
        """Prompt user for search options and guide the user for search."""
        flag=True
        while flag:
            user_input = input('\033c \nOptions to search for movies: \n\t-rating\n\t-genre\n\t-both\n\t-enter "exit" to leave search\n\t')
            if self._is_exit(user_input):
                break
            try:
                search_type=SearchTypes(user_input)
                match search_type:
                    case SearchTypes.RATING:
                        self._rating_search()
                    case SearchTypes.GENRE:
                        self._genre_search()
                    case SearchTypes.BOTH:
                        self._rating_search()
                        self._genre_search()
                break
            except ValueError:
                print('Please enter a valid search type or type "exit" to leave the search.')

    def _rating_search(self):
        """Prompt user for search options and return input"""
        search = input('\033c \nEnter a minimum rating value 1-10.\n\t')
        if self._is_exit(search):pass
        else:
            try:
                search = float(search)
                if 1 <= search <= 10:
                    self.all_filter_tools.append(['Average Rating','>', {search}])
                else:
                    raise ValueError
            except ValueError:
                print('Please enter a valid rating value or type "exit" to leave the search.')
                raise ValueError

    def _genre_search(self):
        """Prompt user for search options and return input"""
        search = input('\033c \nEnter a type of genre. To list genres, type "genre"\n\t')
        if self._is_exit(search):pass
        else:
            if self._is_input_help(search):self.display_help(search)
            self.all_filter_tools.append(['Genre',search])

    @staticmethod
    def _is_exit(user_input:str):
        """Check if user is given exit command to interface, if so return true."""
        exit_list=['quit', 'exit', 'leave']
        if user_input.strip().lower() in exit_list:
            flag=True
        else:
            flag=False
        return flag
        
    @staticmethod
    def _is_input_help(user_input:str):
        """Return boolean based on user input."""
        if user_input in ['genre', 'search', 'filter', 'help', '-help']:
            return True
        else:
            return False

    def display_help(self, user_input:str):
        """Print help instructions based on user request."""
        options='\nOptions:\n\tsearch\n\tgenre\n\tfilter\n\tquit\n'
        flag=True
        while flag:
            if user_input in 'genre':
                logger.info(f'''\033Genres: Action\nAdventure\nAnimation\nBiography\nComedy\nCrime\nDocumentary\nDrama\nFamily\nFantasy\nFilm-Noir\nGame-Show\nHistory\nHorror\nMusic\nMusical\nMystery\nNews\nReality-TV\nRomance\nSci-Fi\nShort\nSport\nTalk-Show\nThriller\nWar\nWestern''')
                user_input=input(options)

            elif user_input in 'search':
                logger.info(f'''\033cTo search for a movie, enter values such as: Average Rating, >, 5 or if looking for titles or genres, try typing and entering: Shawshank Redemption or Horror\n''')
                user_input=input(options)

            elif user_input in 'filter':
                logger.info(f'''\033cTo apply more than one filter, separate the filters by ''')
                user_input=input(options)

            elif self._is_exit(user_input):
                flag=False

    @staticmethod
    def _return_input():
        """Print user for valid options and return input"""
        return input('\033c \t\t\t\t\t\t\tWelcome to movie agent!\n\nYour options: \n\t-press enter to search \n\t-type -help to open instructions menu \n\t-type "exit" to leave\n\t')

if __name__ == "__main__":
    """"""
    ui=UserInterface()