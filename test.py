import logging
import os
import argparse

from league_logic import BiwengerApi

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)

def main() -> None:
    """Run bot."""
    # Init parser
    parser = argparse.ArgumentParser()

    # Add arguments
    parser.add_argument("-u", "--user", required=True)
    parser.add_argument("-p", "--pass", required=True)

    # Read arguments
    args = parser.parse_args()
    user_mail = vars(args)['user']
    user_pass = vars(args)['pass']

    # Init Client
    biwenger = BiwengerApi(user_mail, user_pass)

    # Main functionalities
    biwenger.get_account_info()

    print(biwenger.get_league_balances())


if __name__ == "__main__":
  
    main() # startpoint
