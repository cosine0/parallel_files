""" delete dirs and files in parallel """

import functools
import glob
import os
import sys
from pathlib import Path
from typing import List

from parallel_traversal import parallel_recursive_apply


def force_delete(path: Path, _, is_dir: bool) -> None:
    """ delete file or dir even if it is read-only """
    try:
        if is_dir:
            path.rmdir()
        else:
            path.unlink()
    except PermissionError:
        pass
    else:
        return

    # try again after unsetting read-only flag
    try:
        if sys.platform == 'win32':
            os.chmod(path, 0o777)
        else:
            os.chmod(path, 0o777, follow_symlinks=False)
        if is_dir:
            path.rmdir()
        else:
            path.unlink()
    except PermissionError:
        # try finding locking process and its parent processes to inform user
        import psutil
        for proc in psutil.process_iter():
            try:
                if path in proc.open_files():
                    break
            except psutil.NoSuchProcess:
                pass
        else:
            proc = None
        if proc is not None:
            print(f"Failed to delete {path!r} because it is locked by "
                  f"process {proc.name()} (pid={proc.pid}).")
            print("Parent processes:")
            for i, parent in enumerate(proc.parents()):
                print(f"{'  ' * i}`- {parent.name()} "
                      f"({parent.pid})")
        # reraise anyway
        raise


def parallel_rm_r(paths: List[str], num_max_threads: int = 512) -> None:
    """ delete dirs and files in parallel """
    parallel_recursive_apply(
        paths,
        dir_func=functools.partial(force_delete, is_dir=True),  # type: ignore
        file_func=functools.partial(force_delete, is_dir=False),  # type: ignore
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
            # if pattern does not match anything, skip it
            print(f"Warning: Skipped {pattern}: Does not match "
                  "any file or directory.")
        paths.extend(matches)

    # delete dirs and files in parallel
    parallel_rm_r(paths)


if __name__ == "__main__":
    main()
