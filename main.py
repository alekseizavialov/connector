import configparser
import json
import requests
import psycopg2
import logging

config = configparser.ConfigParser()
config.sections()
config.read('config.ini')
config.sections()
logging.basicConfig(level=config['logging']['log_level'],
                    format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
                    filename=config['logging']['filename'])
log = logging.getLogger("logger")

class pgsql:
    connection = psycopg2.connect(user=config['postgresql']['user'],
                          password=config['postgresql']['password'],
                          host=config['postgresql']['host'],
                          port=config['postgresql']['port'],
                          database=config['postgresql']['database'])
    cursor = connection.cursor()

    def insert(self, postgres_insert_query, record_to_insert):
        try:
            self.cursor.execute(postgres_insert_query, record_to_insert)
            self.connection.commit()
            count = self.cursor.rowcount
        except:
            count = 0
        return count

if __name__ == '__main__':
    respone = requests.get('https://random-data-api.com/api/cannabis/random_cannabis?size=10').content.decode('utf8')
    json_respone = json.loads(respone)
    log.info(f"Resived {len(json_respone)} items from api")
    pgsql = pgsql()
    postgres_insert_query = """INSERT INTO cannabis (id, uid, strain, cannabinoid_abbreviation, 
    cannabinoid, terpene, medical_use, health_benefit, category, "type", buzzword, brand)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    rows_inserted = 0
    for cannabis in json_respone:
        record_to_insert = (cannabis["id"], cannabis["uid"], cannabis["strain"],
                            cannabis["cannabinoid_abbreviation"], cannabis["cannabinoid"], cannabis["terpene"],
                            cannabis["medical_use"], cannabis["health_benefit"], cannabis["category"],
                            cannabis["type"], cannabis["buzzword"], cannabis["brand"])
        row_count = pgsql.insert(postgres_insert_query, record_to_insert)
        rows_inserted += row_count
    log.info(f"Inserted to table {rows_inserted} rows")