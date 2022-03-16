import argparse
import difflib
import dvc.api

import pandas as pd


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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('v1', help='rev1 ')
    parser.add_argument('v2', help='rev2')
    parser.add_argument('repo', help='repo url')
    parser.add_argument('dump', help='dump')
    parser.add_argument('storage', help='file storage')
    args = parser.parse_args()
    f1 = dvc.api.open('data/' + str(args.dump), str(args.repo), str(args.v1), str(args.storage), mode='r',
                      encoding='utf-8')
    f2 = dvc.api.open('data/' + str(args.dump), str(args.repo), str(args.v2), str(args.storage), mode='r',
                      encoding='utf-8')
    f3 = open('data/diff.path', mode='w', encoding="utf-8")
    diff_version(f1, f2, f3)
