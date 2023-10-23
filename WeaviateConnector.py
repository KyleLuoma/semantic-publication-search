import weaviate
import json

class WeaviateConnector:
    """
    Wrapper for the Weaviate client.
    """

    def __init__(self):
        with open('.local/weaviate.json') as config_file:
            config = json.load(config_file)
            self.url = config['url']
            self.client = weaviate.Client(self.url)
            if self.client.is_live():
                print("Weaviate is live")

    def get_client(self):
        return self.client

if __name__ == '__main__':
    connector = WeaviateConnector()
    connector.client.schema.delete_class("Sentence")
    
    