import csv 
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

    def read_url_from_file(self):
        with open(self.input_file, 'r') as file:
            reader = csv.reader(file, delimiter='\t')
            next(reader)
            for row in reader:
                yield row[0]

    def parse_url(self, url):
        query = urlparse(url).query
        query_dict = parse_qs(query)
        url_parsed = [query_dict.get(url_part, [''])[0] for url_part in self.url_mapping.keys()]
        return url_parsed

    def write_parsed_to_file(self):
        with open(f"{self.output_file_name}.tsv", 'w', newline='') as file:
            writer = csv.writer(file, delimiter='\t')
            header = ["url"]
            header.extend(self.url_mapping.values())
            writer.writerow(header)
            for url in self.read_url_from_file():
                url_parsed = [url]
                url_parsed.extend(self.parse_url(url))
                writer.writerow(url_parsed)

if __name__ == "__main__":
    url_parser = URLParser(r'task1\data\task1_input.tsv', r'task1\data\task1_solution')
    url_parser.write_parsed_to_file()