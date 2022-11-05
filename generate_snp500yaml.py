from csv import DictReader
import yaml

def load_all_infos():
    all_ciks = {}
    def load_cik(ciks):
        with open (ciks, 'r') as cik_nums:
            for line in cik_nums:
                tokenized = line.split(",")
                all_ciks[tokenized[0].upper()] = tokenized[1]
        return all_ciks

    def load_csv(constituents):
        with open (constituents, 'r') as data:
            load_dictionary = DictReader(data)
            all_dictionaries = list(load_dictionary)
            return all_dictionaries

    def reshape_dictionary(dictionaries, cik_dictionary):
        compiled_dict = {}
        for dict in dictionaries:
            try:
                dict['CIK'] = int(cik_dictionary[dict.get('Symbol')].strip())
                dict['Watch'] = False
                compiled_dict[dict.get('Symbol')] = dict
            except:
                print(dict.get('Symbol') + " cik not found")
        return compiled_dict

    def load_to_yaml(dictionary):
        try:
            with open('snp500.yaml', "w+") as stonks:
                yaml.dump(dictionary, stonks, default_flow_style=False)
            return True
        except:
            return False

    def all_sectors(dictionary):
        all_sectors = []
        for company in dictionary:
            if dictionary[company].get("Sector") not in all_sectors:
                all_sectors.append(dictionary[company].get("Sector"))
        return " - " + "\n - ".join(all_sectors)

    original_dictionary = load_csv('constituents_csv.csv')
    cik_dictionary = load_cik('ticker_and_edgar_cik.csv')
    new_dictionary = reshape_dictionary(original_dictionary, cik_dictionary)
    success = load_to_yaml(new_dictionary)

    print("\n__________________________")
    print("    writing to file...    \n")
    print("successful:")
    print(success)
    print("all sectors:")
    print(all_sectors(new_dictionary))
    print("__________________________\n")

if __name__ == "__main__":
    load_all_infos()
