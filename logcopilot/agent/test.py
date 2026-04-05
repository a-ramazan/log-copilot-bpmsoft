import os
from dotenv import load_dotenv

from colorama import Fore, Style

load_dotenv()

def main():
    # print(os.getenv('YC_FOLDER_ID'))
    print("\033[1mЭто жирный текст\033[0m")
    print("\033[92mЭто зеленый текст\033[0m")
    print("\033[1;94mЭто жирный синий текст\033[0m")

    r = input(Fore.GREEN + Style.BRIGHT + 'user>')


if __name__ == "__main__":
    main()
