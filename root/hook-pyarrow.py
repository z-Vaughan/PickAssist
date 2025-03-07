from PyInstaller.utils.hooks import collect_data_files, collect_submodules, collect_dynamic_libs

# Collect all submodules
hiddenimports = collect_submodules('pyarrow')

# Collect data files
datas = collect_data_files('pyarrow')

# Collect dynamic libraries
binaries = collect_dynamic_libs('pyarrow')