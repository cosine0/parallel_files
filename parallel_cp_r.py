""" copy dirs and files in parallel """

import functools
import glob
import os
import shutil
import sys
from pathlib import Path
from typing import List

from parallel_traversal import parallel_recursive_apply


def copy_dir(src: Path, src_root: Path, dest_root: Path,
             as_child) -> None:
    """ copy dir to the corresponding location in dest_root """
    # assume that the parent directory of dest exists
    if as_child:
        dest = dest_root / src_root.name / src.relative_to(src_root)
    else:
        dest = dest_root / src.relative_to(src_root)
    # make directory then copy metadata
    dest.mkdir(parents=False, exist_ok=False)
    shutil.copystat(src, dest, follow_symlinks=False)


def copy_file(src: Path, src_root: Path, dest_root: Path,
              as_child) -> None:
    """ copy file to the corresponding location in dest_root """
    if as_child:
        dest = dest_root / src_root.name / src.relative_to(src_root)
    else:
        dest = dest_root / src.relative_to(src_root)
    # assume that the parent directory of dest exists
    shutil.copy2(src, dest, follow_symlinks=False)


def parallel_cp_r(source_paths: List[str], dest_path: str,
                  num_max_threads: int = 1) -> None:
    """ delete dirs and files in parallel """
    dest = Path(dest_path)
    if dest.exists():
        if not dest.is_dir():
            raise NotADirectoryError(f"{dest_path} is not a directory.")
        # copy directory as a child
        as_child = True
    else:
        if len(source_paths) > 1:
            raise NotADirectoryError(f"{dest_path} must be an existing "
                                     "directory when copying multiple "
                                     "sources.")
        if not os.path.isdir(source_paths[0]):
            shutil.copy2(source_paths[0], dest_path)
            return
        # copy directory as a new name
        as_child = False

    parallel_recursive_apply(
        source_paths,
        dir_func=functools.partial(copy_dir, dest_root=dest,  # type: ignore
                                   as_child=as_child),
        file_func=functools.partial(copy_file, dest_root=dest,  # type: ignore
                                    as_child=as_child),
        pre_order=True,
        num_max_threads=num_max_threads)


def main() -> None:
    """ main function """

    if len(sys.argv) < 3:
        print("Usage: python parallel_cp_r.py <source> <source> ... <dest>")
        print("    You can use wildcards * and ? to specify multiple "
              "files/directories for sources.")
        sys.exit(1)

    # get paths from command line arguments
    paths = []
    for pattern in sys.argv[1:-1]:
        if sys.version_info >= (3, 11):
            paths.extend(
                glob.glob(pattern, recursive=True, include_hidden=True))
        else:
            paths.extend(glob.glob(pattern, recursive=True))

    # copy files and directories
    parallel_cp_r(paths, sys.argv[-1])


if __name__ == "__main__":
    main()
