from .crawler import Crawler, Processor
from typing import Callable, Any


class Perdcomp:
    def __init__(
        self,
        extract_cnpjs_func: Callable[[None], list],
        save_data_func: Callable[[dict[str,Any]], None],
    ):
        self.__result_crawler_path = "data/success"
        self.__result_processed_path = "data/results"
        self.extract_func = extract_cnpjs_func
        self.save_data_func = save_data_func

    def run(self):
        cnpjs = self.extract_func()
        assert isinstance(cnpjs, list)
        crawler = Crawler(cnpj_list=cnpjs)
        crawler.run()
        processor = Processor()
        processor.silver()
        self.save_data_func(path=self.__result_processed_path, cnpj_list=cnpjs)
