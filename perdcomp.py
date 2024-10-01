from crawler import Crawler, Processor
from typing import Callable


class Perdcomp():
    def __init__(self, extract_cnpjs_func : Callable[[None], list], save_data_func : Callable[[str], None]):
        self.__result_crawler_path = "data/success"
        self.__result_processed_path = "data/results"
        self.extract_func = extract_cnpjs_func
        self.save_data_func = save_data_func

    def run(self):
        cnpj_list = self.extract_func()
        assert isinstance(cnpj_list, list)
        crawler = Crawler(cnpj_list=cnpj_list)
        crawler.run()
        processor = Processor()
        processor.silver()  
        self.save_data_func(self.__result_processed_path)

