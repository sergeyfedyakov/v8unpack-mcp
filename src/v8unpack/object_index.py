"""Пообъектный индекс raw-слоя для гранулярной organized-сборки.

При распаковке с keep_raw строит .v8unpack_raw/objects.json:

    {"folders": {"<Тип>/<Имя>": {"fp": "<sha256 папки>", "files": ["<уровень>/<файл>", ...]}}}

Папка объекта — каталог второго уровня organized-дерева (<Тип>/<Имя>); всё
вложенное (формы, команды, модули, макеты) принадлежит папке своего объекта.
Атрибуция raw-файлов — по uuid из *.id.json: файл <uuid>, <uuid>.<суффикс> или
каталог <uuid>.N относится к объекту с этим uuid (uuid глобально уникален,
уровень контейнера не важен).

При сборке нетронутые папки переносятся из raw-слоя целиком (см.
Decoder.encode_include и v8unpack.build), изменённые и новые кодируются из
organized-дерева как обычно. Служебные файлы контейнера (root, version,
versions, configinfo) и корневой объект всегда собираются кодировщиком.
"""

import hashlib
import json
import os
import shutil

INDEX_FILE = 'objects.json'
_ROOT_OBJECT_JSONS = ('Configuration.json', 'ConfigurationExtension.json',
                      'ExternalDataProcessor.json', 'ExternalReport.json')


def _hash_folder(folder: str) -> str:
    """sha256 содержимого папки: вложенные относительные пути + содержимое файлов."""
    h = hashlib.sha256()
    for dirpath, dirnames, filenames in os.walk(folder):
        dirnames[:] = sorted(dirnames)
        for fn in sorted(filenames):
            full = os.path.join(dirpath, fn)
            h.update(os.path.relpath(full, folder).replace('\\', '/').encode('utf-8'))
            with open(full, 'rb') as f:
                for chunk in iter(lambda: f.read(1 << 20), b''):
                    h.update(chunk)
    return h.hexdigest()


def _stat_folder(folder: str) -> dict:
    """{relpath: [size, mtime_ns]} — дешёвая съёмка состояния папки."""
    stats = {}
    for dirpath, _, filenames in os.walk(folder):
        for fn in filenames:
            full = os.path.join(dirpath, fn)
            st = os.stat(full)
            stats[os.path.relpath(full, folder).replace('\\', '/')] = [st.st_size, st.st_mtime_ns]
    return stats


def _object_folders(out_dir: str) -> dict:
    """{relpath '<Тип>/<Имя>': абсолютный путь} папок объектов верхнего уровня."""
    folders = {}
    for type_name in sorted(os.listdir(out_dir)):
        type_dir = os.path.join(out_dir, type_name)
        if type_name.startswith('.') or not os.path.isdir(type_dir):
            continue
        for obj_name in sorted(os.listdir(type_dir)):
            obj_dir = os.path.join(type_dir, obj_name)
            if os.path.isdir(obj_dir):
                folders[f'{type_name}/{obj_name}'] = obj_dir
    return folders


def _uuid_to_folder(folders: dict) -> dict:
    """{uuid: relpath папки} по всем *.id.json внутри папок объектов."""
    mapping = {}
    for rel, folder in folders.items():
        for dirpath, _, filenames in os.walk(folder):
            for fn in filenames:
                if not fn.endswith('.id.json'):
                    continue
                try:
                    with open(os.path.join(dirpath, fn), encoding='utf-8') as f:
                        uuid = json.load(f).get('uuid')
                except (OSError, ValueError):
                    continue
                if uuid:
                    mapping[str(uuid).lower()] = rel
    return mapping


def _root_uuids(out_dir: str) -> set:
    """uuid корневого объекта (Configuration/ConfigurationExtension/epf/erf).

    Файлы корневого объекта не атрибутируются папкам — он всегда кодируется.
    """
    uuids = set()
    for fn in _ROOT_OBJECT_JSONS:
        path = os.path.join(out_dir, fn)
        if not os.path.isfile(path):
            continue
        try:
            with open(path, encoding='utf-8') as f:
                uuid = json.load(f).get('uuid')
        except (OSError, ValueError):
            continue
        if uuid:
            uuids.add(str(uuid).lower())
    return uuids


def build_and_save(out_dir: str, raw_dir: str) -> dict:
    """Построить индекс «папка объекта → raw-файлы + sha256» и сохранить в raw-слой."""
    folders = _object_folders(out_dir)
    uuid_to_folder = _uuid_to_folder(folders)
    root_uuids = _root_uuids(out_dir)

    files_by_folder = {rel: [] for rel in folders}
    if not os.path.isdir(raw_dir):
        raise FileNotFoundError(raw_dir)
    for level in sorted(os.listdir(raw_dir)):
        level_dir = os.path.join(raw_dir, level)
        if not os.path.isdir(level_dir):
            continue
        for entry in sorted(os.listdir(level_dir)):
            base = entry.split('.')[0].lower()
            folder = uuid_to_folder.get(base)
            if folder is not None and base not in root_uuids:
                files_by_folder[folder].append(f'{level}/{entry}')

    # raw-файлы, которые не удалось отнести ни к одной папке объекта
    # (организатор не материализует такие объекты): при сборке они могут быть
    # потеряны — сохраняем их список в индексе и переносим страховкой в build().
    attributed = {f for files in files_by_folder.values() for f in files}
    unattributed = []
    for level in sorted(os.listdir(raw_dir)):
        level_dir = os.path.join(raw_dir, level)
        if not os.path.isdir(level_dir):
            continue
        for entry in sorted(os.listdir(level_dir)):
            rel = f'{level}/{entry}'
            if rel in attributed or entry.split('.')[0].lower() in root_uuids:
                continue
            unattributed.append(rel)

    root_files = _root_files(out_dir)
    index = {
        'unattributed': unattributed,
        'folders': {},
        'root': {'fp': _hash_root_files(root_files), 'stat': _stat_root_files(root_files)},
    }
    for rel, folder in folders.items():
        index['folders'][rel] = {
            'fp': _hash_folder(folder),
            'stat': _stat_folder(folder),
            'files': files_by_folder[rel],
        }
    with open(os.path.join(raw_dir, INDEX_FILE), 'w', encoding='utf-8') as f:
        json.dump(index, f, ensure_ascii=False, indent=1)
    return index


def get_unattributed(raw_dir: str) -> list:
    """raw-файлы, не отнесённые ни к одной папке объекта (см. build_and_save)."""
    index = load(raw_dir)
    if index is None:
        return []
    return list(index.get('unattributed') or [])


def load(raw_dir: str):
    """Индекс из raw-слоя или None, если его нет/он битый."""
    path = os.path.join(raw_dir, INDEX_FILE)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding='utf-8') as f:
            index = json.load(f)
        if isinstance(index, dict) and isinstance(index.get('folders'), dict):
            return index
    except (OSError, ValueError):
        pass
    return None


def _hash_root_files(files: dict) -> str:
    """sha256 корневых файлов organized-дерева: {rel: полный путь}."""
    h = hashlib.sha256()
    for rel in sorted(files):
        h.update(rel.encode('utf-8'))
        with open(files[rel], 'rb') as f:
            for chunk in iter(lambda: f.read(1 << 20), b''):
                h.update(chunk)
    return h.hexdigest()


def _root_files(out_dir: str) -> dict:
    """Файлы в корне organized-дерева (json версии, version.bin и т.п.)."""
    files = {}
    for fn in sorted(os.listdir(out_dir)):
        full = os.path.join(out_dir, fn)
        if os.path.isfile(full) and not fn.startswith('.'):
            files[fn] = full
    return files


def _stat_root_files(files: dict) -> dict:
    return {rel: [os.stat(full).st_size, os.stat(full).st_mtime_ns] for rel, full in files.items()}


def _folder_unchanged(folder: str, meta: dict) -> bool:
    """Быстрый путь — stat-снимок (size+mtime_ns); полный sha256 при расхождении."""
    saved_stat = meta.get('stat')
    if saved_stat is not None and _stat_folder(folder) == saved_stat:
        return True
    return _hash_folder(folder) == meta.get('fp')


def analyze(out_dir: str, raw_dir: str):
    """Анализ изменений для гранулярной сборки.

    Возвращает (skip_map, all_unchanged):
    - skip_map — {relpath: [raw-файлы]} нетронутых папок (перенести из raw);
    - all_unchanged — ничего не менялось вообще (быстрый путь: raw целиком).
    None — индекса нет/битый (легаси-каталог без objects.json).
    """
    index = load(raw_dir)
    if index is None:
        return None
    folders_now = _object_folders(out_dir)
    skip_map = {}
    for rel, meta in index['folders'].items():
        folder = folders_now.get(rel)
        if folder is not None and _folder_unchanged(folder, meta):
            skip_map[rel] = meta.get('files', [])
    root_meta = index.get('root')
    root_files = _root_files(out_dir)
    root_ok = False
    if root_meta is not None:
        saved_stat = root_meta.get('stat')
        if saved_stat is not None and _stat_root_files(root_files) == saved_stat:
            root_ok = True
        else:
            root_ok = _hash_root_files(root_files) == root_meta.get('fp')
    new_folders = set(folders_now) - set(index['folders'])
    all_unchanged = root_ok and not new_folders and len(skip_map) == len(index['folders'])
    return skip_map, all_unchanged


def copy_raw_object(files, raw_root: str, dest_dir: str) -> list:
    """Перенести raw-файлы одного объекта в каталог контейнера (stage1).

    Возвращает имена перенесённых файлов/каталогов — для file_list, как это
    делает штатный encode объекта.
    """
    copied = []
    for rel in files:
        src = os.path.join(raw_root, *rel.split('/'))
        name = rel.rsplit('/', 1)[-1]
        dst = os.path.join(dest_dir, name)
        if os.path.exists(dst):
            raise FileExistsError(f'файл уже собран кодировщиком, raw-копия конфликтует: {dst}')
        if os.path.isdir(src):
            shutil.copytree(src, dst)
        elif os.path.isfile(src):
            shutil.copy2(src, dst)
        else:
            raise FileNotFoundError(f'raw-файл из индекса отсутствует: {src}')
        copied.append(name)
    return copied
