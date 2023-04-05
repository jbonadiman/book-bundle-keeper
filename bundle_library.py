import sys
from collections.abc import Sequence
from pathlib import Path

import manager


def parse_args(args: Sequence) -> Path | None:
    if len(args) < 1:
        return None
    input_file_name = args[0]

    return input_file_name


if __name__ == '__main__':
    program_path = Path(sys.argv[0])

    file_path = parse_args(sys.argv[1:])

    if not file_path:
        print(f'Usage: python {program_path.name} input_file_name')
        sys.exit(1)

    books = manager.read_file(file_path)
    if not books:
        print(f'Could not read file: {file_path}')
        sys.exit(1)

    manager.write_to_db(file_path, *books)
