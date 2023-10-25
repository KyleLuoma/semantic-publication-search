import PDFTextExtractor
import WeaviateConnector
import pandas as pd
import json
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
import os
import pypdf.errors
import openpyxl.utils.exceptions
from collections import defaultdict


def main():
    sps = SemanticPubSearcher()
    user_input = ""
    while True:
        user_input = input("Enter a query: ")
        if user_input.lower() in ["quit", "exit", "q"]:
            break
        print("Searching")
        results = sps.search(
            user_input,
            distance=0.5,
            )
        line_tuple_list = sps.combine_adjacent_lines(results)
        for entry in line_tuple_list:
            print(f"\n\n----- Pub: {entry[0]}, Page#: {entry[1]}, Sentence# {entry[2]} -----")
            for line in entry[3]:
                print("    " + line)


def batch_upload():
    sps = SemanticPubSearcher()
    folder_path = "./pubs/army_dir"
    already_uploaded = os.listdir("./excel_exports")
    for filename in os.listdir(folder_path):
        if filename.endswith(".pdf") and filename.replace(".pdf", ".xlsx") not in already_uploaded:
            completion_log = open("completion_log.txt", "a")
            print("Uploading " + filename)
            file_path = os.path.join(folder_path, filename).replace("\\", "/")
            try:
                sps.embed_and_upload(file_path, save_as_excel=True)
                completion_log.write(filename + "\n")
            except pypdf.errors.PdfStreamError as e:
                print("Error: " + str(e))
                # move file to error folder
                completion_log.write(filename + " - ERROR\n")
                os.rename(file_path, file_path.replace("army_dir", "army_dir/need_repair"))

            
            completion_log.close()


class SemanticPubSearcher:

    def __init__(
            self, 
            model_name: str = 'all-MiniLM-L6-v2'
            ):
        self.pte = PDFTextExtractor.PDFTextExtractor()
        self.model = SentenceTransformer(model_name)
        self.model.to('cuda')
        self.weviate_connector = WeaviateConnector.WeaviateConnector()

    def embed_and_upload(
            self, 
            filepath: str,
            save_as_excel: bool = False
            ) -> None:
        """
        Embeds each sentence in a PDF file and uploads the embeddings to Weaviate.

        Args:
            filepath: The path to the PDF file.
        """
        embedding_dict = self.get_embeddings_from_pdf(filepath)
        if save_as_excel:
            filename = filepath.split("/")[-1].replace(".pdf", ".xlsx")
            df = pd.DataFrame(embedding_dict)
            try:
                df.to_excel(f"./excel_exports/{filename}")
            except openpyxl.utils.exceptions.IllegalCharacterError:
                print("Illegal character in filename")
                return

        self.upload_embeddings(embedding_dict)

    def search(
            self, 
            query: str,
            limit: int = None,
            distance: float = None
            ) -> list:
        """
        Submit a query to Weaviate and return the results.

        Args:
            query: The query string.
            limit: The maximum number of results to return.
            distance: The maximum distance from the query vector to return results from.
        """
        embedding = self.model.encode(query)
        near_vector = {
            "vector": embedding
            }
        if distance != None:
            near_vector["distance"] = distance
        search_results = self.weviate_connector.client.query.get(
            "Sentence", ["sentence", "page", "sentence_index", "pub_filename"]
        ).with_near_vector(
            near_vector
        )
        if limit != None:
            search_results = search_results.with_limit(limit)
        search_results = search_results.do()
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


    def get_embeddings_from_pdf(
            self, 
            filepath: str,
            batch_size: int = 128
            ) -> dict:
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
            results = self.model.encode(
                page,
                batch_size=batch_size
                )
            for s_ix, line in enumerate(page):
                sentences.append(line)
                sentence_ixs.append(s_ix)
                page_nums.append(ix)
                embeddings.append(results[s_ix])
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
    
    def combine_adjacent_lines(
            self,
            results: list,
            distance: int = 6
        ) -> dict:
        """
        combines nearby adjacent lines on the same page to minimize redundant 
        overlapping results

        args:
            results: a list of results returned from SemanticPubSearcher.search()
            distance: distance from start to end of line to cover before jumping 
            to a new line entry

        returns:
            a list of tuples with the format (pub, page number, line  number, line list)
        """
        #Combine nearly overlapping results
        pub_page_result_dict = defaultdict(list)
        combined_page_result_dict = defaultdict(list)
        result_pub_page_order = []
        for result in results:
            tuple_key = (result['pub_filename'], result['page'])
            pub_page_result_dict[tuple_key].append(result['sentence_index'])
            if tuple_key not in result_pub_page_order:
                result_pub_page_order.append(tuple_key)
        #Iterate through pages and remove lines that fall within a provided distance from prior lines
        for k in pub_page_result_dict.keys():
            line_list = pub_page_result_dict[k]
            line_list.sort()
            last_line = line_list[0] + distance
            combined_page_result_dict[k].append(line_list[0])
            for line in line_list:
                if line > last_line:
                    combined_page_result_dict[k].append(line)
                    last_line = line + distance
        #Fetch lines from db and print
        pub_page_line_lines_tuple_list = []
        for k in result_pub_page_order:
            for line_ix in combined_page_result_dict[k]:
                lines = self.pte.get_lines_from_db(
                    pub_filename=k[0],
                    page_ix=k[1],
                    start_ix=line_ix,
                    num_lines=8
                )
                pub_page_line_lines_tuple_list.append((k[0], k[1], line_ix, lines))
        return pub_page_line_lines_tuple_list


if __name__ == '__main__':
    main()
    # batch_upload()