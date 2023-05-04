""" copy dirs and files in parallel """

import functools
import glob
import os
import shutil
import sys
from pathlib import Path
from typing import List, Dict

from parallel_traversal import parallel_recursive_apply, FileType

src_inode_to_dest_path: Dict[int, Path] = {}


@functools.lru_cache(maxsize=65536)
def get_mount_point(absolute_path: Path) -> Path:
    """ get mount point of path """
    if os.path.ismount(absolute_path):
        return absolute_path
    return get_mount_point(absolute_path.parent)


def relative_to_mount(path: Path) -> Path:
    """ get path relative to its mount point """
    mount_point = get_mount_point(path.absolute())
    return path.absolute().relative_to(mount_point)


def copy_dir(src: Path, src_root: Path, dest_root: Path,
             as_child: bool) -> None:
    """ copy dir to the corresponding location in dest_root """
    # assume that the parent directory of dest exists
    if as_child:
        dest = dest_root / src_root.name / src.relative_to(src_root)
    else:
        dest = dest_root / src.relative_to(src_root)
    # make directory then copy metadata
    try:
        dest.mkdir(parents=False, exist_ok=True)
        shutil.copystat(src, dest, follow_symlinks=False)
    except OSError:
        import traceback
        print(f'\r{traceback.format_exc()}')


def copy_file(src: Path, src_root: Path, dest_root: Path,
              as_child: bool) -> None:
    """ copy file to the corresponding location in dest_root """
    if as_child:
        dest = dest_root / src_root.name / src.relative_to(src_root)
    else:
        dest = dest_root / src.relative_to(src_root)
    # assume that the parent directory of dest exists
    try:
        file_type = FileType.from_path(src)
        # skip special files
        if file_type in (FileType.DEVICE, FileType.UNKNOWN):
            print(f'\rWarning: Skipped {src}: Non-regular file (device, '
                  f'named pipe, socket, etc.)')
            return
        if file_type in \
                (FileType.SYMLINK, FileType.JUNCTION, FileType.WSL_SYMLINK):
            if file_type == FileType.WSL_SYMLINK:
                import reparse_points
                _, reparse_data = reparse_points.get_reparse_info(src)
                link_target = reparse_data.substitute_name
            else:
                link_target = os.readlink(src)
            if link_target.startswith('\\\\?\\Volume{'):
                print(f'\rWarning: Skipped {src}: Volume mount point')
                return
            if link_target.startswith('\\\\?\\'):
                link_target = link_target[4:]
            link_target = Path(link_target)
            if os.path.isabs(link_target):
                dest_target = link_target
                if not os.path.lexists(link_target):
                    target_from_mount = relative_to_mount(link_target)
                    dest_mount = get_mount_point(dest.absolute())
                    if os.path.lexists(dest_mount / target_from_mount):
                        dest_target = dest_mount / target_from_mount
                        link_target = Path(os.path.relpath(dest_target, dest))
                    else:
                        print(f'\rWarning: Skipped {src}: Broken link')
                        return
            else:
                dest_target = dest.parent / link_target
            if sys.platform == 'win32':
                if file_type in (FileType.SYMLINK, FileType.WSL_SYMLINK):
                    if file_type == FileType.WSL_SYMLINK:
                        print('\rWarning: Treating as an ordinary symbolic '
                              f'link: {src}: A symbolic link created in WSL')
                    try:
                        dest.symlink_to(link_target)
                        return
                    except OSError:
                        if not dest_target.is_dir():
                            # junctions can only point to directories
                            print(f'\rWarning: Skipped {src}: No rights to '
                                  'create a symbolic link (to a file)')
                            return
                        import _winapi
                        try:
                            _winapi.CreateJunction(str(link_target), str(dest))
                            print(f'\rWarning: Copied as a junction {src}: '
                                  'No rights to create a symbolic link')
                        except FileNotFoundError:
                            print(f'\rWarning: Skipped {src}: Broken link')
                        return
                elif file_type == FileType.JUNCTION:
                    import _winapi
                    try:
                        _winapi.CreateJunction(str(link_target), str(dest))
                    except FileNotFoundError:
                        print(f'\rWarning: Skipped {src}: Broken link')
                    return
            else:
                dest.symlink_to(link_target)
                shutil.copystat(src, dest, follow_symlinks=False)
                return

        # hard linked files
        file_stat = src.lstat()
        if file_stat.st_nlink > 1:
            if file_stat.st_ino in src_inode_to_dest_path:
                # hard link to a file that has already been copied
                try:
                    dest.hardlink_to(src_inode_to_dest_path[file_stat.st_ino])
                except OSError:
                    pass
                shutil.copystat(src, dest, follow_symlinks=False)
                print(f'\rInfo: {src} is copied as a hard link. '
                      f'source has {file_stat.st_nlink} links, '
                      f'destination has {dest.lstat().st_nlink} links.')
                return

            print(f'\rWarning: {src} is a hard-link. This file has '
                  f'{file_stat.st_nlink} links.')
            src_inode_to_dest_path[file_stat.st_ino] = dest

        shutil.copy2(src, dest, follow_symlinks=False)
    except PermissionError:
        if dest.exists():
            if sys.platform == 'win32':
                dest.chmod(0o777)
            else:
                dest.chmod(0o777, follow_symlinks=False)
            dest.unlink()
            shutil.copy2(src, dest, follow_symlinks=False)
    except OSError:
        import traceback
        print(f'\r{traceback.format_exc()}')


def parallel_cp_r(source_paths: List[str], dest_path: str,
                  num_max_threads: int = 256) -> None:
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
            shutil.copy2(source_paths[0], dest_path, follow_symlinks=False)
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
            matches = glob.glob(pattern, recursive=True, include_hidden=True)
        else:
            matches = glob.glob(pattern, recursive=True)
        if not matches:
            # if pattern does not match anything, skip it
            print(f"Warning: Skipped {pattern}: Does not match "
                  "any file or directory.")
        paths.extend(matches)

    # copy files and directories
    parallel_cp_r(paths, sys.argv[-1])


if __name__ == "__main__":
    main()
