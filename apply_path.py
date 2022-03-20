
import difflib
import dvc.api

import pandas as pd
from git import Repo


def diff_version(f1: object, f2: object, f3: object) -> object:
    data = difflib.ndiff(f1.readlines(), f2.readlines())
    delta = ''.join(x[2:] for x in data if x.startswith(' ') or x.startswith('+ '))
    f3.write(delta)
    # print(delta)


def export(file_name: str, sep: str):
    csv_file = pd.DataFrame(pd.read_csv(file_name, sep=sep, header=0, index_col=False))
    csv_file.to_json(file_name + "json", orient="records", date_format="epoch", double_precision=10, force_ascii=False,
                     date_unit="ms", default_handler=None)
    csv_file.to_excel(file_name + ".xlsx", sheet_name='export', index=False)


def get_dump_version(dump: str, repo: str, version: str, storage: str):
    with dvc.api.open('./data/' + dump, repo, version.upper(), storage, mode='r', encoding='utf-8') as fd:
        tmp_file = dump.split('.')[0]
        file_name: str = f"./data/{tmp_file}_{version}.DAT".format(tmp_file, version)
        dump_file = open(file_name, mode='w', encoding='utf-8')
        for line in fd:
            dump_file.write(line)


def create_dump_tag_version(dump: str, version: str, storage: str):
    repo = Repo('./')
    dump_file = f"./data/{dump}".format(dump)
    file = open(dump_file, mode='r', encoding='utf-8')
    tmp_file = dump.split('.')[0]
    file_name = f"./data/{tmp_file}_{version}.DAT".format(tmp_file, version)
    file_write = open(file_name, mode='w', encoding='utf-8')
    for line in file:
        file_write.write(line)

    repo.commit()
    repo.create_tag(version, message='Create new version of dump "{0}"'.format(version))


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser()
#     parser.add_argument('v1', help='rev1 ')
#     parser.add_argument('v2', help='rev2')
#     parser.add_argument('repo', help='repo url')
#     parser.add_argument('dump', help='dump')
#     parser.add_argument('storage', help='file storage')
#     args = parser.parse_args()
#     f1 = dvc.api.open('data/' + str(args.dump), str(args.repo), str(args.v1), str(args.storage), mode='r',
#                       encoding='utf-8')
#     f2 = dvc.api.open('data/' + str(args.dump), str(args.repo), str(args.v2), str(args.storage), mode='r',
#                       encoding='utf-8')
#     f3 = open('data/apply.path', mode='w', encoding="utf-8")
#     diff_version(f1, f2, f3)

create_dump_tag_version('KP_CENA_MOD_APR.DAT', 'V1.4', 'myremote')

# get_dump_version('KP_CENA_MOD_APR.DAT', 'C:/dev/projects/apply_patch/', 'v1.0', 'myremote')

# apply_path('KP_CENA_MOD_APR.DAT', 'KP_CENA_MOD_APR_V1.0.DAT', 'KP_CENA_MOD_APR_DIFF.DAT')
