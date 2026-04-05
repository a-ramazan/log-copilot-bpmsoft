from __future__ import annotations

import argparse

from .agent import stream_agent
from colorama import Fore, Style, init

init(autoreset=True)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="LogCopilot Agent CLI")
    parser.add_argument("--run-id", required=True, help="ID уже обработанного run")
    parser.add_argument(
        "--provider",
        choices=("local", "yandex"),
        default="local",
        help="Какую LLM использовать",
    )
    parser.add_argument("--debug", action="store_true", help="Печатать технический trace агента")
    return parser.parse_args()


def _print_trace(trace: list[str]) -> None:
    print("[trace]")
    for item in trace:
        print(f"- {item}")
    print()


def _chat_once(args: argparse.Namespace, question: str) -> None:
    result, iterator = stream_agent(
        question=question,
        run_id=args.run_id,
        provider=args.provider,
        session_state=args._session_state,
    )
    args._session_state = result.memory

    if args.debug:
        _print_trace(result.trace)

    print(Fore.BLUE + Style.BRIGHT + "log-copilot> " + Style.RESET_ALL, end="", flush=True)
    for token in iterator:
        print(token, end="", flush=True)
    print()



def main() -> None:
    # для командной строки чтоб аргументы красивенько и удобно писать
    args = parse_args()
    args._session_state = {}

    print(Fore.CYAN + Style.BRIGHT + Style.DIM + "LogCopilot Agent. /exit для выхода")
    print(Fore.CYAN + Style.BRIGHT + Style.DIM + f"provider={args.provider}")

    # основной цикл для вопросов от пользователей
    while True:
        try:
            question = input(Fore.GREEN + Style.BRIGHT + "user> "+ Style.RESET_ALL).strip()
        except (EOFError, KeyboardInterrupt):
            print(Fore.RED + Style.BRIGHT + Style.DIM + "\nДо встречи.")
            break

        if not question:
            continue
        if question == "/exit":
            print(Fore.RED + Style.BRIGHT + Style.DIM + "\nДо встречи.")
            break

        try:
            _chat_once(args, question)
        except Exception as exc:
            print(Fore.BLUE + Style.BRIGHT + f"log-copilot> Ошибка: {exc}")


if __name__ == "__main__":
    main()
