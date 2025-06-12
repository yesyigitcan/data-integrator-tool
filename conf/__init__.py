import yaml
import os




def read_spark_properties() -> dict:
    conf_dir = os.path.dirname(os.path.abspath(__file__))
    conf_spark_dir = os.path.join(conf_dir, "spark.yml")
    with open(conf_spark_dir, 'r', encoding='utf-8') as file:
        return dict(yaml.safe_load(file))
    
def read_catalog_properties() -> dict:
    conf_dir = os.path.dirname(os.path.abspath(__file__))
    conf_catalog_dir = os.path.join(conf_dir, "catalog.yml")
    with open(conf_catalog_dir, 'r', encoding='utf-8') as file:
        return dict(yaml.safe_load(file))

def read_jdbc_properties() -> dict:
    conf_dir = os.path.dirname(os.path.abspath(__file__))
    conf_catalog_dir = os.path.join(conf_dir, "jdbc.yml")
    with open(conf_catalog_dir, 'r', encoding='utf-8') as file:
        return dict(yaml.safe_load(file))