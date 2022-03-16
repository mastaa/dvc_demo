import argparse
import sys

import pandas, csv
import sqlalchemy
from sqlalchemy import MetaData, Table, Column, Integer, String

def import_db(csv: str):
    engine = sqlalchemy.create_engine('postgresql://postgres:postgres@localhost:5432/postgres', echo=True)

    meta = MetaData(schema='dvc_db')
    meta.reflect(engine)
    table = meta.tables['dvc_db.price']
    del_st = table.delete()
    conn = engine.connect()
    res = conn.execute(del_st)
    columns = ["id", "id_cennika", "cena_netto", "dlugosc_loj", "id_model_apr", "data_od", "data_do", "id_taryfy",
               "id_kwota_zobow", "id_kwota_rachunku", "vat_id", "subsyd_apr", "kierunek_migracji", "id_grupa_apr",
               "metoda_platnosci", "operator"]

    df = pandas.read_csv(csv, delimiter='\u007F', keep_default_na=False, names=columns)

    df.to_sql('price', engine, schema='dvc_db', index=False, if_exists='append')
    print("Zaimportowano " + str(df.shape[0]) + " wierszy")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('csv', help='ścieżka do dump-a')
    args = parser.parse_args()

if len(sys.argv) > 1:
    import_db(args.csv)

elif len(sys.argv) == 0:
    print('Proszę podaj ścieżkę do pliku z dumpem')

