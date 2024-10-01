from dotenv import load_dotenv;load_dotenv()
import os
from pandas_wrapper.pg import PandasPG
import pandas as pd
from perdcomp import Perdcomp

PG_HOST = os.getenv("PG_HOST")
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
PG_DB = os.getenv("PG_DB")
PG_PORT = os.getenv("PG_PORT")
SCHEMA  = "perdcomp"
TEST_TABLE = "test_perdcomp"


def extract_cnpjs():

    df = pd.read_excel('/home/luis/Downloads/Lista CNPJs 150k 2024.xlsx')
    cnpj_list = df.iloc[:, 0].tolist()
    return cnpj_list[:10]
 
def save_data(path):

    df = pd.read_parquet(path)

    pg = PandasPG(
        host=PG_HOST,
        user=PG_USER,
        pwd=PG_PASS,
        db=PG_DB,
        port=PG_PORT,
    )

    df = df[
        [
            "cnpj",
            "razao_social",
            "numero_perdcomp",
            "tipo_documento",
            "tipo_credito",
            "data_transmissao",
            "situacao",
            "descricao_situacao",
            "data_processamento",
        ]
    ]

    pg.to_sql_alchemy(df, TEST_TABLE, schema=SCHEMA, if_exists='append')

def main():

    p = Perdcomp(extract_cnpjs_func=extract_cnpjs, save_data_func=save_data)
    p.run()

if __name__ == "__main__":

    main()
