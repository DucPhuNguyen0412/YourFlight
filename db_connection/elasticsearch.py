from elasticsearch import Elasticsearch

def create_index(index_name):
    es = Elasticsearch(['localhost:9200'])

    mapping = {
        "mappings": {
            "properties": {
                "price": { "type": "double" },
                "company_names": { "type": "text" },
                "start_end_time": { "type": "text" },
                "duration": { "type": "text" },
                "num_stops": { "type": "integer" },
                "stop_destinations": { "type": "text" }
            }
        }
    }

    es.indices.create(index=index_name, body=mapping)

# Call these functions before writing data to Elasticsearch
create_index("one_way_index")
create_index("round_trip_index")
