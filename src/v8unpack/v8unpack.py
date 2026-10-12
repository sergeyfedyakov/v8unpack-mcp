import argparse
import hashlib
import os
import shutil
import sys
import tempfile
from datetime import datetime

from . import helper
from . import object_index
from .helper import RAW_DIR
from .container_reader import extract as container_extract, decompress_and_extract
from .container_writer import build as container_build, compress_and_build
from .decoder import decode, encode
from .ext_exception import ExtException
from .organizer_file import OrganizerFile
from .organizer_file_ce import OrganizerFileCE
from .helper import check_index, load_json
from .index import update_index
from .version import __version__

FINGERPRINT_FILE = 'fingerprint'


def _carry_over_raw_files(raw_dir, stage1_dir) -> int:
    """Донести в stage1 raw-файлы, которых там не оказалось.

    Страховка от молчаливой потери объектов: часть raw-файлов не атрибутируется
    папкам объектов (организатор не материализует такие типы), при гранулярной
    сборке они не переносятся индексом и раньше исчезали из результата.
    """
    carried = 0
    missing = []
    for level in sorted(os.listdir(raw_dir)):
        level_dir = os.path.join(raw_dir, level)
        if not os.path.isdir(level_dir):
            continue  # fingerprint, objects.json и прочие служебные файлы
        dest_level = os.path.join(stage1_dir, level)
        for entry in sorted(os.listdir(level_dir)):
            src = os.path.join(level_dir, entry)
            dst = os.path.join(dest_level, entry)
            if os.path.exists(dst):
                continue
            os.makedirs(dest_level, exist_ok=True)
            if os.path.isdir(src):
                shutil.copytree(src, dst)
            else:
                shutil.copy2(src, dst)
            carried += 1
            missing.append(f'{level}/{entry}')
    if carried:
        print(f'{"Донесено raw-файлов":30}: {carried} ({", ".join(missing[:5])}'
              f'{"..." if len(missing) > 5 else ""})')
    return carried


def _dir_fingerprint(root):
    """Хэш дерева исходников (для определения, редактировались ли они)."""
    h = hashlib.sha256()
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d != RAW_DIR)
        for fn in sorted(filenames):
            rel = os.path.relpath(os.path.join(dirpath, fn), root)
            h.update(rel.encode('utf-8'))
            with open(os.path.join(dirpath, fn), 'rb') as f:
                for chunk in iter(lambda: f.read(1 << 20), b''):
                    h.update(chunk)
    return h.hexdigest()


def _save_raw_snapshot(stage1_dir, out_dir):
    """
    Сохраняет «второй слой» (raw brace-файлы) рядом с исходниками, чтобы при сборке
    можно было вернуть нередактированные объекты без потерь.
    """
    raw_dir = os.path.join(out_dir, RAW_DIR)
    helper.clear_dir(raw_dir)
    for entry in os.listdir(stage1_dir):
        shutil.copytree(os.path.join(stage1_dir, entry), os.path.join(raw_dir, entry))
    with open(os.path.join(raw_dir, FINGERPRINT_FILE), 'w', encoding='utf-8') as f:
        f.write(_dir_fingerprint(out_dir))


def _restore_raw_snapshot(in_dir, stage1_dir):
    """
    Если есть сохранённый «второй слой» и исходники не редактировались — подкладываем
    raw-файлы в stage1 напрямую (сборка без потерь). Возвращает True, если подложили.
    """
    raw_dir = os.path.join(in_dir, RAW_DIR)
    if not os.path.isdir(raw_dir):
        return False
    fp_path = os.path.join(raw_dir, FINGERPRINT_FILE)
    if not os.path.isfile(fp_path):
        return False
    with open(fp_path, 'r', encoding='utf-8') as f:
        saved_fp = f.read()
    if saved_fp != _dir_fingerprint(in_dir):
        return False
    _copy_raw_snapshot(raw_dir, stage1_dir)
    return True


def _copy_raw_snapshot(raw_dir, stage1_dir):
    """Перенести контейнеры raw-слоя в stage1 (служебные файлы не трогаем)."""
    helper.clear_dir(stage1_dir)
    for entry in os.listdir(raw_dir):
        if entry in (FINGERPRINT_FILE, object_index.INDEX_FILE) \
                or not os.path.isdir(os.path.join(raw_dir, entry)):
            continue  # служебные файлы (fingerprint, objects.json) и прочие не-каталоги
        shutil.copytree(os.path.join(raw_dir, entry), os.path.join(stage1_dir, entry))


def extract(in_filename: str, out_dir_name: str, *, temp_dir=None, index=None, processes=None, options=None):
    try:
        begin0 = datetime.now()
        if options is None:
            options = dict()
        print(f"saby v8unpack {__version__}")

        index = check_index(index)
        if not index and index is not None:
            return

        print(f"Начали")

        descent = options.get('descent') if options else None

        if descent is None:
            helper.clear_dir(os.path.normpath(out_dir_name))
        clear_temp_dir = False
        if temp_dir is None:
            clear_temp_dir = True
            temp_dir = tempfile.mkdtemp()
        helper.clear_dir(os.path.normpath(temp_dir))

        dir_stage0 = os.path.join(temp_dir, 'decode_stage_0')
        dir_stage1 = os.path.join(temp_dir, 'decode_stage_1')
        # dir_stage2 = os.path.join(temp_dir, 'decode_stage_2')
        dir_stage3 = os.path.join(temp_dir, 'decode_stage_3')

        pool = helper.get_pool(processes=processes)

        container_extract(in_filename, dir_stage0, False, False)
        decompress_and_extract(dir_stage0, dir_stage1, pool=pool)

        # json_decode(dir_stage1, dir_stage2, pool=pool)

        decode(dir_stage1, dir_stage3, pool=pool, options=options)

        if descent is not None:
            OrganizerFileCE.unpack(dir_stage3, out_dir_name, pool=pool, index=index, descent=descent)
        else:
            OrganizerFile.unpack(dir_stage3, out_dir_name, pool=pool, index=index)

        if options.get('keep_raw'):
            _save_raw_snapshot(dir_stage1, out_dir_name)
            # пообъектный индекс для гранулярной organized-сборки (best effort)
            try:
                from . import object_index
                object_index.build_and_save(out_dir_name, os.path.join(out_dir_name, RAW_DIR))
            except Exception as err:
                print(f'{"Пообъектный индекс не построен":30}: {err}')

        end = datetime.now()
        print(f'{"Готово":30}: {end - begin0}')

        helper.close_pool(pool)
        if clear_temp_dir:
            shutil.rmtree(temp_dir, ignore_errors=True)
    except Exception as err:
        raise ExtException(parent=err) from None


def build(in_dir_name: str, out_file_name: str, *, temp_dir=None, index=None,
          options=None, processes: int = None):
    try:
        begin0 = datetime.now()
        if options is None:
            options = {}

        print(f"saby v8unpack {__version__}")

        index = check_index(index)
        if not index and index is not None:
            return

        print(f"Начали")

        clear_temp_dir = False
        if temp_dir is None:
            clear_temp_dir = True
            temp_dir = tempfile.mkdtemp()
        helper.clear_dir(os.path.normpath(temp_dir))

        dir_stage0 = os.path.join(temp_dir, 'encode_stage_0')
        dir_stage1 = os.path.join(temp_dir, 'encode_stage_1')
        # dir_stage2 = os.path.join(temp_dir, 'encode_stage_2')
        dir_stage3 = os.path.join(temp_dir, 'encode_stage_3')

        pool = helper.get_pool(processes=processes)

        descent = options.get('descent') if options else None

        raw_restored = False
        skip_map = None
        if options.get('use_raw'):
            raw_dir = os.path.join(in_dir_name, RAW_DIR)
            analysis = None
            # Гранулярный анализ: при наличии пообъектного индекса заменяет собой
            # глобальный fingerprint (легаси-каталоги без objects.json — как раньше).
            if index is None and descent is None and os.path.isdir(raw_dir):
                try:
                    analysis = object_index.analyze(in_dir_name, raw_dir)
                except Exception as err:
                    print(f'{"Гранулярная сборка недоступна":30}: {err}')
            if analysis is not None:
                skip_map, all_unchanged = analysis
                if all_unchanged:
                    _copy_raw_snapshot(raw_dir, dir_stage1)
                    raw_restored = True
                    skip_map = None
            else:
                raw_restored = _restore_raw_snapshot(in_dir_name, dir_stage1)

        if not raw_restored:
            # Гранулярная сборка: нетронутые объекты (skip_map) переносим из raw-слоя,
            # кодируем только изменённые и новые. Ключи skip-карты — relpath папок
            # организованного дерева; stage3 зеркалит его (нетронутые — заглушками),
            # organized_root указываем на stage3 после OrganizerFile.pack.
            if skip_map:
                options['skip_objects'] = skip_map
                options['raw_root'] = os.path.join(in_dir_name, RAW_DIR)

            if descent is None:
                OrganizerFile.pack(in_dir_name, dir_stage3, pool=pool, index=index, skip_folders=skip_map)
            else:
                OrganizerFileCE.pack(in_dir_name, dir_stage3, pool=pool, index=index, descent=descent)

            if skip_map:
                options['organized_root'] = os.path.abspath(dir_stage3)

            encode(dir_stage3, dir_stage1, pool=pool, file_name=os.path.basename(out_file_name), options=options)

            if skip_map:
                print(f'{"Гранулярная сборка":30}: из raw перенесено объектов: {len(skip_map)}')
                for key in ('skip_objects', 'raw_root', 'organized_root'):
                    options.pop(key, None)

            # Страховка: ни один raw-файл не должен исчезнуть из результата.
            raw_dir = os.path.join(in_dir_name, RAW_DIR)
            if os.path.isdir(raw_dir):
                carried = _carry_over_raw_files(raw_dir, dir_stage1)
                if carried:
                    options['_raw_carried'] = carried

        # json_encode(dir_stage2, dir_stage1, pool=pool)

        compress_and_build(dir_stage1, dir_stage0, pool=pool)
        container_build(dir_stage0, out_file_name, True)

        helper.close_pool(pool)
        if clear_temp_dir:
            shutil.rmtree(temp_dir, ignore_errors=True)

        end = datetime.now()
        print(f'{"Готово":30}: {end - begin0}')

    except Exception as err:
        raise ExtException(parent=err) from None


def build_all(product_file_name: str, product_code: str = None, processes=None):
    try:
        products = load_json(os.path.abspath(product_file_name))
        if product_code:
            products = {product_code: products[product_code]}
            products[product_code]['disable'] = False
        for product, params in products.items():
            if params.get('disable'):
                continue
            print(f'\nСобираем {product}')
            options = params.get('options', {})
            options['product'] = product
            build(
                params['src'], params['bin'],
                temp_dir=params.get('temp'), index=params.get('index'),
                options=options, processes=processes
            )
        pass
    except Exception as err:
        raise ExtException(parent=err)


def extract_all(product_file_name: str, product_code: str = None, processes=None):
    try:
        products = load_json(os.path.abspath(product_file_name))
        if product_code:
            products = {product_code: products[product_code]}
            products[product_code]['disable'] = False
        for product, params in products.items():
            if params.get('disable'):
                continue
            print(f'\nРазбираем {product}\n')
            extract(
                params['bin'], params['src'],
                temp_dir=params.get('temp'), index=params.get('index'),
                options=params.get('options'), processes=processes
            )
        pass
    except Exception as err:
        raise ExtException(parent=err)


def update_index_all(product_file_name: str, product_code: str = None, dest_dir: str = None):
    try:
        products = load_json(os.path.abspath(product_file_name))
        if product_code:
            products = {product_code: products[product_code]}
            products[product_code]['disable'] = False
        for product, params in products.items():
            if params.get('disable'):
                continue
            print(f'\nФормируем индекс {product}\n')
            update_index(
                params['src'],
                params.get('index'),
                dest_dir
            )
        pass
    except Exception as err:
        raise ExtException(parent=err)


def main():
    parser = argparse.ArgumentParser(
        prog=f'v8unpack {__version__}',
        description='Распаковка и сборка бинарных файлов 1С'
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-E', nargs=2, metavar=('file', 'src'),
                       help='разобрать файл 1С, где '
                            'file - путь до бинарного файла, '
                            'src -  папка куда будут помещены исходники')
    group.add_argument('-B', nargs=2, metavar=('src', 'file'),
                       help='собрать файл 1С, где '
                            'src - путь до папки с исходниками, '
                            'file - путь до бинарного файла')
    group.add_argument('-I', nargs=1, metavar='src',
                       help='сформировать index, где '
                            'src - путь до папки с исходниками'
                       )
    parser.add_argument('--temp', help='путь до временной папки')
    parser.add_argument('--prefix', help='строка добавляемая к имени метаданных 1 уровня')
    parser.add_argument('--core', help='название общей папки добавляемой в индекс по умолчанию, только для режима -I')
    parser.add_argument('--index', help='путь до json файла с словарем копирования,'
                                        'структура файла: {путь исходника: путь общей папки}')
    parser.add_argument('--version',
                        help="версия режима совместимости, необязательно. "
                             " При сборке и разборе расширений и конфигураций подменяет версию режима совместимости. "
                             " Можно указывать только конкретные значения, например для 8.3.6 это 80306"
                             " Режим 'не использовать' в 1С возникает только если версия режима совместимости "
                             " совпадает с текущей версией платформы")
    parser.add_argument('--processes', type=int,
                        help="количество процессов которые будут использоваться для работы, "
                             " если не указано, то количество процессов = количеству ядер компьютера. "
                             " Нет смысла указывать значения превышающее количество ядер")
    parser.add_argument('--descent', type=int,
                        help="включает режим наследования при сборке и разборке,"
                             "четырех значный формат 3.0.75.100 (не более 3 знаков на каждый разряд)"
                             "подробности в инструкции - раздел разработка расширений")
    # parser.add_argument('--gui',
    #                     help="режим совместимости интерфейса 1С, переопределяет значение из исходников "
    #                          "для расширений, если указан устанавливается в соответствующий реквизит. "
    #                          "Допустимые значения: "
    #                          " 0 - Версия 8.2, 1 - Версия 8.2. Разрешить Такси,"
    #                          " 2- Такси. Разрешить Версия 8.2, 3 - Такси")
    parser.add_argument("--auto_include", default=False, action="store_true",
                        help="Если указан содержимое метаданных собирается динамически в зависимости "
                             "от наличия файлов в каталоге, а при разборке оглавление не сохраняется, "
                             "например вы можете подкинуть файлы метаданных в каталог из другого проекта "
                             "минусом является, что вложенные ресурсы всегда сортируются по алфавиту. "
                             "Если не указан, то сборщик не трогает оглавление, вы добавляете и убираете"
                             "вложенные объекты всегда через конфигуратор")
    parser.add_argument("--keep_raw", default=False, action="store_true",
                        help="при разборке сохранять «второй слой» (raw-файлы) в каталог .v8unpack_raw, "
                             "чтобы при сборке нередактированные объекты собирались без потерь")
    parser.add_argument("--use_raw", default=False, action="store_true",
                        help="при сборке использовать сохранённый «второй слой» (.v8unpack_raw), "
                             "если исходники не редактировались")

    group.add_argument('-EA', nargs=1, metavar='file',
                       help='разобрать один или несколько файлов 1С, где '
                            'file - путь до json файла со списком продуктов и параметрами их сборки')
    group.add_argument('-BA', nargs=1, metavar='file',
                       help='собрать один или несколько файлов 1С, где '
                            'file - путь до json файла со списком продуктов и параметрами их сборки')

    group.add_argument('-IA', nargs=1, metavar='file',
                       help='сформировать index по одному или нескольким продуктам, где '
                            'file - путь до json файла со списком продуктов и параметрами их сборки')

    if len(sys.argv) == 1:
        parser.print_help()
        return 1

    args = parser.parse_args()

    options = {}
    options_name = ['prefix', 'auto_include', 'descent', 'version', 'keep_raw', 'use_raw']
    for elem in options_name:
        value = getattr(args, elem, None)
        if value:
            options[elem] = value

    try:
        if args.E is not None:
            extract(os.path.abspath(args.E[0]), os.path.abspath(args.E[1]),
                    index=args.index, temp_dir=args.temp, options=options)
            return

        if args.B is not None:
            build(os.path.abspath(args.B[0]), os.path.abspath(args.B[1]),
                  index=args.index, temp_dir=args.temp, options=options)
            return

        if args.BA is not None:
            build_all(args.BA[0], args.index)
            return

        if args.EA is not None:
            extract_all(args.EA[0], args.index)
            return

        if args.I is not None:
            update_index(args.I[0], args.index, args.core)
            return

        if args.IA is not None:
            update_index_all(args.IA[0], args.index, args.core)
            return

    except Exception as err:
        error = ExtException(parent=err)
        print(f'\n\nОшибка: {error}')
        return 1


if __name__ == '__main__':
    sys.exit(main())
