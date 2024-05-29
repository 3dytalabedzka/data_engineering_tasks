import os
import csv 
import logging
import argparse
from datetime import date
from urllib.parse import urlparse, parse_qs

class URLParser:
    """
    A class to parse URLs from an input tsv file and write the parsed results to an output tsv file.

    Attributes:
    -----------
    input_file : str
        Path to the input file containing URLs.
    output_file_name : str
        Name the output file to save parsed URLs.
    url_mapping : dict
        Dictionary mapping URL query parameters to desired output fields.

    Methods:
    --------
    read_url_from_file():
        Reads URLs from the input file.
    parse_url(url):
        Parses a URL and extracts query parameters based on the url_mapping.
    write_parsed_to_file():
        Writes parsed URLs to the output file.
    """
    def __init__(self, input_file: str, output_file_name: str):
        """
        Constructs all the necessary attributes for the URLParser object.

        Parameters:
        -----------
        input_file : str
            Path to the input file containing URLs.
        output_file_name : str
            Name of the output file to save parsed URLs.
        """
        self.input_file = input_file
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.output_file_name = os.path.join(current_dir, output_file_name)
        self.url_mapping = {
            "a_bucket":"ad_bucket",
            "a_type":"ad_type",
            "a_source":"ad_source",
            "a_v":"schema_version",
            "a_g_campaignid":"ad_campaign_id",
            "a_g_keyword":"ad_keyword",
            "a_g_adgroupid":"ad_adgroup_id",
            "a_g_creative":"ad_creative"
        }
        if not os.path.exists('task1\logs'):
            os.makedirs('task1\logs')
        logs_file_name = f"task1\logs\{date.today()}.log"
        logs_format = '%(asctime)s - %(levelname)s - %(message)s'
        logging.basicConfig(filename=logs_file_name, level=logging.INFO, format=logs_format)

    def read_url_from_file(self):
        """
        Reads URLs one by one from the input file.

        Yields:
        -------
        str
            A URL read from the input file.
        """
        try:
            with open(self.input_file, 'r') as file:
                reader = csv.reader(file, delimiter='\t')
                next(reader)
                for row in reader:
                    yield row[0]
        except FileNotFoundError:
            logging.error(f"File {self.input_file} not found, please check if it exists!")
            raise

    def parse_url(self, url: str):
        """
        Parses a URL and extracts query parameters based on the url_mapping.

        Parameters:
        -----------
        url : str
            The URL to be parsed.

        Returns:
        --------
        list
            A list containing parsed query parameter values.
        """
        try:
            query = urlparse(url).query
            query_dict = parse_qs(query)
            url_parsed = [query_dict.get(url_part, [''])[0] for url_part in self.url_mapping.keys()]
            return url_parsed
        except Exception as e:
            logging.error(f"Cannot parse {url}, exception message:\n{str(e)}")
            return []

    def write_parsed_to_file(self):
        """
        Writes parsed URLs to the output file.
        """
        logging.info(f"Parsing URLs from file {self.input_file} and saving to {self.output_file_name}.tsv")
        with open(f"{self.output_file_name}.tsv", 'w', newline='') as file:
            writer = csv.writer(file, delimiter='\t')
            header = list(self.url_mapping.values())
            header.insert(0, 'url')
            writer.writerow(header)
            try:
                for url in self.read_url_from_file():
                    url_parsed = self.parse_url(url)
                    url_parsed.insert(0, url)
                    writer.writerow(url_parsed)
            except Exception as e:
                logging.error(f"Parsing URLs failed due to error: \n{str(e)}")
                raise Exception("Parsing failed, please check the logs!")

class TestURLParser:
    """
    A class to test the URLParser by comparing its output with a target file.

    Attributes:
    -----------
    input_file : str
        Path to the input file containing URLs.
    target_file : str
        Path to the target file with expected parsed results.
    test_file_name : str
        Name of the file where parsed URLs will be saved.

    Methods:
    --------
    compare_files():
        Compares the parsed result file with the target file.
    """
    def __init__(self, input_file: str, target_file: str, test_file_name: str):
        """
        Constructs all the necessary attributes for the TestURLParser object and initiates the parsing.

        Parameters:
        -----------
        input_file : str
            Path to the input file containing URLs.
        target_file : str
            Path to the target file with expected parsed results.
        test_file_name : str
            Name of the file where parsed URLs will be saved.
        """
        self.parser = URLParser(input_file, test_file_name)
        self.parser.write_parsed_to_file()
        self.input_file = input_file
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.test_file = os.path.join(current_dir, f"{test_file_name}.tsv")
        self.target_file = target_file
        if not os.path.exists('task1\logs'):
            os.makedirs('task1\logs')
        logs_file_name = f"task1\logs\{date.today()}.log"
        logs_format = '%(asctime)s - %(levelname)s - %(message)s'
        logging.basicConfig(filename=logs_file_name, level=logging.INFO, format=logs_format)

    def compare_files(self):
        """
        Compares the parsed result file with the target file.

        Raises:
        -------
        Exception
            If the parsed results do not match the target file.
        """
        logging.info(f"Running parser test based on files \ninput: {self.input_file}, \ntarget: {self.target_file}, \nreturned by parser: {self.test_file} ")
        with open(self.test_file, 'r') as test, open(self.target_file, 'r') as target:
            reader1 = csv.reader(test, delimiter='\t')
            reader2 = csv.reader(target, delimiter='\t')

            for row1, row2 in zip(reader1, reader2):
                try:
                    assert row1 == row2, f"Provided files don't match! \nTest file values:{row1} \nTarget file values:{row2}"
                except AssertionError as ae:
                    logging.error(str(ae))
                    raise Exception("Parser test failed!")
            logging.info("Parser test passed!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="parse URLs")
    parser.add_argument("-in", "--input_file", default=r'task1\data\task1_input.tsv', help="Path to input tsv file", required=False)
    parser.add_argument("-out", "--output_file", default=r'task1\data\task1_output.tsv', help="Path to the target tsv file with expected results", required=False)
    parser.add_argument("-target", "--target_file", default=r'data\task1_solution', help="Name of tsv file in which results will be stored.", required=False)
    args = parser.parse_args()    

    # parser
    # url_parser = URLParser(args.input_file, args.target_file)
    # url_parser.write_parsed_to_file()

    # test
    url_parser_test = TestURLParser(args.input_file, args.output_file, args.target_file)
    url_parser_test.compare_files()
