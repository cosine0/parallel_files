# Parallel Files
A simple tool to remove and copy a large number of files and directories in parallel.

## Usage
```
python parallel_rm_r.py path/to/unused/dirs path/to/unused/files.jpg tmp*
python parallel_cp_r.py path/to/src other_file_2023-03-*.txt path/to/dest_dir
```
Alternatively, you can use the `parallel_rm_r.exe` and `parallel_cp_r.exe` binaries. from the [releases](https://github.com/cosine0/parallel_files/releases) page.
```
parallel_rm_r.exe path/to/unused/dirs path/to/unused/files.jpg tmp*
parallel_cp_r.exe path/to/src other_file_2023-03-*.txt path/to/dest_dir
```

## License
GPLv3