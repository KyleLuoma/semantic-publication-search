import PDFTextExtractor
import WeaviateConnector
import pandas as pd
import json
from sentence_transformers import SentenceTransformer
from tqdm import tqdm



def main():

    sps = SemanticPubSearcher()
    # sps.embed_and_upload("./pubs/da_pam/ARN30948-PAM_670-1-000-WEB-1.pdf")

    print("Searching")
    results = sps.search("commander directed")
    for result in results:
        print(result)



class SemanticPubSearcher:

    def __init__(self):
        self.pte = PDFTextExtractor.PDFTextExtractor()
        self.model = SentenceTransformer('bert-base-nli-mean-tokens')
        self.model.to('cuda')
        self.weviate_connector = WeaviateConnector.WeaviateConnector()

    def embed_and_upload(self, filepath: str) -> None:
        """
        Embeds each sentence in a PDF file and uploads the embeddings to Weaviate.

        Args:
            filepath: The path to the PDF file.
        """
        embedding_dict = self.get_embeddings_from_pdf(filepath)
        self.upload_embeddings(embedding_dict)

    def search(self, query: str) -> list:
        embedding = self.model.encode(query)
        near_vector = {"vector": embedding}
        search_results = self.weviate_connector.client.query.get(
            "Sentence", ["sentence", "page", "sentence_index", "pub_filename"]
        ).with_near_vector(
            near_vector
        ).with_limit(15).do()
        json_result = json.dumps(search_results)
        result = json.loads(json_result)
        return result['data']['Get']['Sentence']

    def upload_embeddings(self, embedding_dict: dict) -> None:
        """
        Uploads the embeddings for each sentence in a PDF file to Weaviate.

        Args:
            embedding_dict: A dictionary containing:
                'page': page_nums, 
                'sentence': sentences, 
                'sentence_index': sentence_indices for lookup,
                'embedding': embeddings,
                'pub_filename': filename of the PDF file.
        """

        df = pd.DataFrame(embedding_dict)
        df.to_excel("sentences.xlsx")

        print("Uploading embeddings to Weaviate...")
        with self.weviate_connector.client.batch as batch:
            for ix, row in tqdm(df.iterrows(), total=df.shape[0]):
                if len(row['sentence'].split()) < 5:
                    continue
                batch.add_data_object(
                    data_object={
                        "page": row['page'],
                        "sentence": row['sentence'],
                        "sentence_index": row['sentence_index'],
                        "pub_filename": row['pub_filename'],
                    },
                    class_name="Sentence",
                    vector=row["embedding"]
                )
        print("Done.")


    def get_embeddings_from_pdf(self, filepath: str) -> dict:
        """
        Generates embeddings for each sentence in a PDF file.

        Args:
            filepath: The path to the PDF file.

        Returns:
            A dictionary containing the page number, the sentence, the embedding, 
            and the filename.
        """
        text = self.pte.catalogue_pdf(filepath)

        sentences = []
        sentence_ixs = []
        page_nums = []
        embeddings = []
        filenames = []

        progress_bar = tqdm(
            total=len(text),
            unit="pages"
            )

        print("Extracting embeddings...")
        for ix, page in enumerate(text):
            for s_ix, line in enumerate(page):
                sentences.append(line)
                sentence_ixs.append(s_ix)
                page_nums.append(ix)
                embeddings.append(self.model.encode(line))
                filenames.append(filepath.split("/")[-1])
            progress_bar.update(1)

        embedding_dict = {
            'page': page_nums, 
            'sentence': sentences, 
            'sentence_index': sentence_ixs,
            'embedding': embeddings,
            'pub_filename': filenames
            }
        return embedding_dict


if __name__ == '__main__':
    main()