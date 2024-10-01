import requests
import io
import pandas as pd
import os
from time import sleep
import random   
import snakecase
from datetime import datetime
from loguru import logger

logger.add(
    "logs/crawler.log",
    rotation="500 MB",
    retention="10 days",
    compression="zip",
    level="INFO",
)
from json.decoder import JSONDecodeError

SLEEP_TIME_RANGE = (0.45, 0.65)


class Crawler:
    cnpj_list = []

    def __init__(self, cnpj_list) -> None:
        os.makedirs("data/success", exist_ok=True)
        os.makedirs("data/errors", exist_ok=True)
        os.makedirs("data/results", exist_ok=True)
        self.cnpj_list = cnpj_list

    def get_perdcomp(self, cnpj):
        cnpj_fix = str(cnpj).zfill(14)

        headers = {
            "versao_app": "1.0",
        }

        params = {
            "dataInicial": "",
            "dataFinal": "",
        }

        try:
            response = requests.get(
                f"https://p-app-receita-federal.estaleiro.serpro.gov.br/servicos-rfb-apprfb-perdcomp/apprfb-perdcomp/consulta/ni/{cnpj_fix}",
                params=params,
                headers=headers,
                timeout=20,
            )
        except Exception as e:
            logger.error(f"get_perdcomp: {cnpj_fix} - {e}")
            return None

        if response.status_code != 200 and response.status_code != 204:
            logger.warning(cnpj_fix)
            logger.warning(
                f"get_perdcomp: Response returned status {response.status_code}"
            )
            return f"text : {response.text} status_code : {response.status_code}"

        logger.success(f"Crawl {cnpj_fix} success")

        try:
            response.json()
        except JSONDecodeError as e:
            return None

        return response.json()

    def run(self):
        self.cnpj_list = self.cnpj_list
        results = []
        errors = []
        batch_index = 1
        total_len = len(self.cnpj_list)
        for i, cnpj in enumerate(self.cnpj_list):
            logger.info("---------------------------------------")
            logger.info(f"Crawling {i} of {total_len}")
            resp = self.get_perdcomp(cnpj)
            if isinstance(resp, str):
                errors.append({"cnpj": cnpj, "error": resp})
            else:
                if resp != None:
                    results.extend(resp)

            sleep_time = random.uniform(*SLEEP_TIME_RANGE)
            sleep(sleep_time)

            logger.info(f"get_perdcomp: Sleeping for: {sleep_time} seconds")

            if len(results) >= 10000:
                logger.info(f"Saving batch {batch_index:04}")
                errors_df = pd.DataFrame(errors)
                errors_df.to_parquet(f"data/errors/error-part-{batch_index:04}.parquet")
                results_df = pd.DataFrame(results)
                results_df.to_parquet(
                    f"data/success/success-part-{batch_index:04}.parquet"
                )
                batch_index += 1
                results = []
                errors = []

        if len(results) > 0:
            logger.info(f"Saving batch {batch_index:04}")
            errors_df = pd.DataFrame(errors)
            results_df = pd.DataFrame(results)
            results_df.to_parquet(
                f"data/success/success-part-{batch_index:04}.parquet",
            )


class Processor:
    def __init__(self) -> None:
        pass

    def silver(self):
        success_df = pd.read_parquet("data/success")
        success_df.columns = [snakecase.convert(col) for col in success_df.columns]

        contribuinte_df = pd.json_normalize(success_df["contribuinte"])
        concat_df = pd.concat(
            [success_df.drop(columns=["contribuinte"]), contribuinte_df], axis=1
        )

        concat_df.rename(columns={"ni": "cnpj", "nome": "razao_social"}, inplace=True)

        current_date = datetime.now().strftime("%Y-%m-%d")
        concat_df["data_processamento"] = current_date

        concat_df.to_parquet("data/results/perdcomp_lucro_real.parquet", compression="snappy", index=False)

    def to_postgre_schema(self, dataframe, table, schema, mode="append"):
        conn = self.engine.raw_connection()

        try:
            cur = conn.cursor()
            dataframe.head(0).to_sql(
                table, self.engine, schema=schema, index=False, if_exists=mode
            )
            output = io.StringIO()
            dataframe.to_csv(output, sep="\t", header=False, index=False)
            output.seek(0)
            cur.execute(f"SET search_path TO %(schema)s", {"schema": schema})
            cur.copy_from(output, table, null="")  # null values become ''
            cur.close()
            conn.commit()
        except Exception as e:
            logger.exception(e)
            conn.rollback()
        finally:
            conn.close()
