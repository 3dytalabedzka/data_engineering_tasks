import os
import csv 
import logging
from datetime import date
from urllib.parse import urlparse, parse_qs

class URLParser:
    def __init__(self, input_file, output_file_name):
        self.input_file = input_file
        self.output_file_name = output_file_name
        self.url_mapping = {
            "a_bucket":"ad_bucket",
            "a_type":"ad_type",
            "a_source":"ad_source",
            "a_v":"schema_version",
            "a_g_campaignid":"ad_campaign_id",
            "a_g_keyword":"ad_keyword",
            "a_g_adgroupid":"ad_group_id",
            "a_g_creative":"ad_creative"
        }
        if not os.path.exists('task1\logs'):
            os.makedirs('task1\logs')
        today = date.today()
        logs_file_name = f"task1\logs\{today.strftime('%d_%m_%Y')}.log"
        logs_format = '%(asctime)s - %(levelname)s - %(message)s'
        logging.basicConfig(filename=logs_file_name, level=logging.INFO, format=logs_format)

    def read_url_from_file(self):
        try:
            with open(self.input_file, 'r') as file:
                reader = csv.reader(file, delimiter='\t')
                next(reader)
                for row in reader:
                    yield row[0]
        except FileNotFoundError:
            logging.error(f"File {self.input_file} not found, please check if it exists!")
            raise

    def parse_url(self, url):
        try:
            query = urlparse(url).query
            query_dict = parse_qs(query)
            url_parsed = [query_dict.get(url_part, [''])[0] for url_part in self.url_mapping.keys()]
            return url_parsed
        except Exception as e:
            logging.error(f"Cannot parse {url}, exception message:\n{str(e)}")
            return []

    def write_parsed_to_file(self):
        logging.info(f"Parsing URLs from file {self.input_file} and saving to {self.output_file_name}.tsv")
        with open(f"{self.output_file_name}.tsv", 'w', newline='') as file:
            writer = csv.writer(file, delimiter='\t')
            header = ["url"]
            header.extend(self.url_mapping.values())
            writer.writerow(header)
            try:
                for url in self.read_url_from_file():
                    url_parsed = [url]
                    url_parsed.extend(self.parse_url(url))
                    writer.writerow(url_parsed)
            except Exception as e:
                message = "Parsing failed, please check the logs"
                logging.error(f"{message}. \nError: {str(e)}")
                raise Exception(message)

if __name__ == "__main__":
    url_parser = URLParser(r'task1\data\task1_input.tsv', r'task1\data\task1_solution')
    url_parser.write_parsed_to_file()