""" delete dirs and files in parallel """

import glob
import os
import sys
from pathlib import Path
from typing import List

from parallel_traversal import parallel_recursive_apply


def force_delete_dir(path: Path, _) -> None:
    """ delete dir even if it is read-only """
    try:
        path.rmdir()
    except PermissionError:
        path.chmod(0o777)
        path.rmdir()


def force_delete_file(path: Path, _) -> None:
    """ delete file even if it is read-only """
    try:
        path.unlink()
    except PermissionError:
        path.chmod(0o777)
        path.unlink()


def parallel_rm_r(paths: List[str], num_max_threads: int = 512) -> None:
    """ delete dirs and files in parallel """
    parallel_recursive_apply(
        paths,
        dir_func=force_delete_dir,
        file_func=force_delete_file,
        pre_order=False,
        num_max_threads=num_max_threads)


def main() -> None:
    """ main function """

    if len(sys.argv) < 2:
        print("Usage: python parallel_rm_r.py <file|directory> <file|directory>"
              " ...")
        print("    You can use wildcards * and ? to specify multiple "
              "files/directories.")
        sys.exit(1)

    # get paths from command line arguments
    paths = []
    for pattern in sys.argv[1:]:
        if sys.version_info >= (3, 11):
            matches = glob.glob(pattern, recursive=True, include_hidden=True)
        else:
            matches = glob.glob(pattern, recursive=True)
        if not matches:
            # if pattern does not match anything, abort
            print(f"Error: {pattern} does not match any files or directories.")
            sys.exit(1)
        paths.extend(matches)

    # delete dirs and files in parallel
    parallel_rm_r(paths)


if __name__ == "__main__":
    main()
