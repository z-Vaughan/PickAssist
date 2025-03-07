from PyInstaller.utils.hooks import collect_data_files, collect_submodules

hiddenimports = collect_submodules('lxml')
datas = collect_data_files('lxml')