""" compare two directories in parallel (metadata is ignored) """

import functools
import os
import shutil
import sys
import threading
from pathlib import Path
from typing import Set, Union

from parallel_traversal import parallel_recursive_apply

print_lock = threading.Lock()


def diff_dir(src: Path, src_root: Path, dest_root: Path) -> None:
    """ compare src dir to the corresponding location in dest_root """
    terminal_width = shutil.get_terminal_size().columns
    dest = dest_root / src.relative_to(src_root)
    if not dest.exists():
        with print_lock:
            print(f"\rDELETED Dir: {src} -> x".ljust(terminal_width))
    elif not dest.is_dir():
        with print_lock:
            print(f"\rPROPERTY CHANGED [DIR]{src} -> [FILE]{dest}".ljust(
                terminal_width))
    else:
        # report created direct children
        src_children: Set[Union[str, bytes, os.PathLike]] = set(os.listdir(src))
        dest_children: Set[Union[str, bytes, os.PathLike]] = \
            set(os.listdir(dest))
        for child in dest_children - src_children:
            if (dest / child).is_dir():
                with print_lock:
                    print(f"\rCREATED Dir (maybe also children, "
                          f"not checked): x -> {child}".ljust(terminal_width))
            else:
                with print_lock:
                    print(f"\rCREATED File: x -> {child}".ljust(terminal_width))


def diff_file(src: Path, src_root: Path, dest_root: Path) -> None:
    """ compare src file to the corresponding location in dest_root """
    terminal_width = shutil.get_terminal_size().columns
    dest = dest_root / src.relative_to(src_root)
    if not dest.exists():
        with print_lock:
            print(f"\rDELETED File: {src} -> x".ljust(terminal_width))
    elif not dest.is_file():
        with print_lock:
            print(f"\rPROPERTY CHANGED [FILE] {src} -> [DIR] {dest}".ljust(
                terminal_width))
    else:
        # compare size
        if src.stat().st_size != dest.stat().st_size:
            with print_lock:
                print(f"\rSIZE CHANGED File: [{src.stat().st_size}] {src} -> "
                      f"[{dest.stat().st_size}] {dest}".ljust(terminal_width))
            return
        # compare content
        with src.open("rb") as src_file, dest.open("rb") as dest_file:
            while True:
                src_data = src_file.read(128 * 1024)
                dest_data = dest_file.read(128 * 1024)
                if src_data != dest_data:
                    with print_lock:
                        print(f"\rCONTENT CHANGED File: {src} -> {dest}".ljust(
                            terminal_width))
                    break
                if not src_data:
                    break


def parallel_diff_tree(source_path: str, dest_path: str,
                       num_max_threads: int = 512) -> None:
    """ compare two directories in parallel (metadata is ignored) """
    source = Path(source_path)
    dest = Path(dest_path)
    if not source.exists():
        raise FileNotFoundError(f"{source_path} does not exist.")
    if not source.is_dir():
        raise NotADirectoryError(f"{source_path} is not a directory.")
    if not dest.exists():
        raise FileNotFoundError(f"{dest_path} does not exist.")
    if not dest.is_dir():
        raise NotADirectoryError(f"{dest_path} is not a directory.")

    parallel_recursive_apply(
        [source_path],
        dir_func=functools.partial(diff_dir, dest_root=dest),  # type: ignore
        file_func=functools.partial(diff_file, dest_root=dest),  # type: ignore
        num_max_threads=num_max_threads,
        pre_order=False,
        strict_hierarchical_order=True,
        print_lock=print_lock
    )


def main() -> None:
    """ main function """
    if len(sys.argv) != 3:
        print("Usage: python parallel_diff_tree.py <source_dir> <dest_dir>")
        sys.exit(1)

    source_path, dest_path = sys.argv[1:]
    parallel_diff_tree(source_path, dest_path)


if __name__ == "__main__":
    main()
