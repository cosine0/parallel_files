""" compare two directories in parallel (metadata is ignored) """

import functools
import sys
import threading
from pathlib import Path

from parallel_traversal import parallel_recursive_apply

print_lock = threading.Lock()


def diff_dir(src: Path, src_root: Path, dest_root: Path) -> None:
    """ compare src dir to the corresponding location in dest_root """
    dest = dest_root / src.relative_to(src_root)
    if not dest.exists():
        with print_lock:
            print(f"\rDELETED Dir: {src} -> x")
    elif not dest.is_dir():
        with print_lock:
            print(f"\rPROPERTY CHANGED [DIR]{src} -> [FILE]{dest}")
    else:
        # report created direct children
        src_children = set(src.iterdir())
        dest_children = set(dest.iterdir())
        for child in dest_children - src_children:
            if child.is_dir():
                with print_lock:
                    print(f"\rCREATED Dir: x -> {child}")
            else:
                with print_lock:
                    print(f"\rCREATED File: x -> {child}")


def diff_file(src: Path, src_root: Path, dest_root: Path) -> None:
    """ compare src file to the corresponding location in dest_root """
    dest = dest_root / src.relative_to(src_root)
    if not dest.exists():
        with print_lock:
            print(f"\rDELETED File: {src} -> x")
    elif not dest.is_file():
        with print_lock:
            print(f"\rPROPERTY CHANGED [FILE]{src} -> [DIR]{dest}")
    else:
        # compare size
        if src.stat().st_size != dest.stat().st_size:
            with print_lock:
                print(f"\rSIZE CHANGED File: {src}[{src.stat().st_size}] "
                      f"-> {dest}[{dest.stat().st_size}]")
            return
        # compare content
        with src.open("rb") as src_file, dest.open("rb") as dest_file:
            while True:
                src_data = src_file.read(128 * 1024)
                dest_data = dest_file.read(128 * 1024)
                if src_data != dest_data:
                    with print_lock:
                        print(f"\rCONTENT CHANGED File: {src} -> {dest}")
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
