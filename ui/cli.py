import os
import string
import asyncio
import logging
import enum
from cons import constansts, messages

logger = logging.getLogger(__name__)

class SearchTypes(enum.Enum):
    RATING = 'rating'
    GENRE = 'genre'
    BOTH = 'both'

class UserInterface():
    """Class that provides interface for user-based actions."""

    def __init__(self): #type: ignore
        self.all_filter_tools:list[list[str]]=[]

    def start(self):
        """Prompt user for actions"""
        flag=True
        filter_tools=None
        while flag:
            user_input=self._return_input()
            if self._is_exit(user_input): break
            elif self._is_input_help(user_input): self.display_help()
            else:
                flag=False
                self._prompt_search()
        return filter_tools

    def _prompt_search(self):
        """Prompt user for search options and guide the user for search."""
        flag=True
        while flag:
            user_input = input(messages.MAIN_OPTIONS)
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
            except ValueError:
                print(messages.INVALID_INPUT)

    def _rating_search(self):
        """Prompt user for search help and return input"""
        search = input(messages.RATING_SEARCH)
        if self._is_exit(search):pass
        else:
            try:
                search = float(search)
                if 1 <= search <= 10:
                    self.all_filter_tools.append([constansts.AVERAGE_RATING_COLUMN,'>', search])
                else:
                    raise ValueError
            except ValueError:
                print(messages.INVALID_INPUT)
                raise ValueError

    def _genre_search(self):
        """Prompt user for search help and return input"""
        search = input(messages.GENRE_SEARCH)
        if self._is_exit(search):pass
        elif 'genre' in search:
            print(messages.GENRE_INFO)
            input('Press any key to continue...')
        else:
            if self._is_input_help(search):self.display_help()
            self.all_filter_tools.append([constansts.GENRE_COLUMN,search])

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
        if user_input in ['--help', 'help', '-help']:
            return True
        else:
            return False

    def display_help(self):
        """Print help instructions based on user request."""
        flag=True
        while flag:
            user_input = input(messages.HELP_OPTIONS)
            user_input = user_input.lower().strip()
            if 'genre' in user_input:
                print(messages.GENRE_INFO)
                flag=False
                input('Press any key to continue...')

            elif 'search' in user_input:
                print(messages.SEARCH_INFO)
                flag=False
                input('Press any key to continue...')

            elif self._is_exit(user_input):
                flag=False

    @staticmethod
    def _return_input():
        """Print user for valid options and return input"""
        return input(messages.WELCOME)

if __name__ == "__main__":
    """"""
    ui=UserInterface()
    ui.start()