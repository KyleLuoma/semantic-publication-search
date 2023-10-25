from pypdf import PdfReader
import sqlite3
import os

def catalogue_batch():
    folders = ["ar", "da_pam", "army_dir", "ago"]
    pdf_text_extractor = PDFTextExtractor()
    conn = sqlite3.connect("./sqlite_dbs/pub_sentences.db")
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS sentences (file_page_line, pub_filename text, page integer, line integer, text text)")
    conn.commit()
    for folder in folders:
        print("Cataloguing " + folder)
        for filename in os.listdir("./pubs/" + folder):
            if filename.endswith(".pdf"):
                print("   " + filename)
                pdf_text_extractor.catalogue_pdf(
                    f"./pubs/{folder}/{filename}",
                    sqlite_db="./sqlite_dbs/pub_sentences.db"
                    )
    c.execute("CREATE INDEX IF NOT EXISTS idx_file_page_line ON sentences (file_page_line)")
    conn.commit()
    conn.close()

class PDFTextExtractor:

    def __init__(self):
        pass

    def catalogue_pdf(
            self, 
            pdf_file,
            sqlite_db: str = None
            ) -> list:
        """ 
        Extracts the text from a PDF file and returns a list of pages, where each page is a list of sentences.
        Optionally, creates a SQLite database and stores the text in it with an index on publication filename, page and line.

        Args:
            pdf_file: The path to the PDF file.

        Returns:
            A list of pages, where each page is a list of sentences.
        """
        if sqlite_db != None:
            conn = sqlite3.connect(sqlite_db)
            c = conn.cursor()
        else:
            conn = None
            c = None
        pdf = PdfReader(pdf_file)
        text = ""
        pages = []
        for page_ix, page in enumerate(pdf.pages):
            text = page.extract_text()
            lines = text.replace("\n", " ").split(". ")
            lines_cleaned = []
            for line_ix, line in enumerate(lines):
                line = line.strip()
                while "  " in line:
                    line = line.replace("  ", " ")
                lines_cleaned.append(line)
                if sqlite_db != None:
                    filename = pdf_file.split("/")[-1]
                    c.execute(
                        "INSERT INTO sentences VALUES (?, ?, ?, ?, ?)", 
                        (
                            filename + "_pg" + str(page_ix) + "_ln" + str(line_ix), 
                            filename, 
                            page_ix, 
                            line_ix, 
                            line
                        )
                    )
            pages.append(lines_cleaned)
        if sqlite_db != None:
            conn.commit()
            conn.close()
        return pages
    
    def get_lines_from_pdf(
            self,
            pdf_file: str,
            page_ix: int,
            start_ix: int,
            num_lines: int = 6
            ) -> list:
        pdf_pages = self.catalogue_pdf(pdf_file)
        lines = []
        page = pdf_pages[page_ix]
        end_ix = min(start_ix + num_lines, len(page))
        for ix in range(start_ix, end_ix):
            lines.append(page[ix])
        return lines
    
    def get_lines_from_db(
            self,
            pub_filename: str,
            page_ix: int,
            start_ix: int,
            sqlite_db: str = "./sqlite_dbs/pub_sentences.db",
            num_lines: int = 6
            ) -> list:
        conn = sqlite3.connect(sqlite_db)
        c = conn.cursor()
        lines = []
        page_cursor = page_ix
        line_cursor = start_ix
        for i in range(num_lines):
            composit_key = f"{pub_filename}_pg{page_cursor}_ln{line_cursor}"
            c.execute("SELECT text FROM sentences WHERE file_page_line=?", (composit_key,))
            result = c.fetchone()
            if result != None:
                lines.append(result[0])
            else:
                page_cursor += 1
                line_cursor = 0
                composit_key = f"{pub_filename}_pg{page_cursor}_ln{line_cursor}"
                c.execute("SELECT text FROM sentences WHERE file_page_line=?", (composit_key,))
                result = c.fetchone()
                if result != None:
                    lines.append(result[0])
            line_cursor += 1
        conn.close()
        return lines


def get_lines_from_db_test():
    print("TEST: get_lines_from_db_test")
    pdf_text_extractor = PDFTextExtractor()
    lines = pdf_text_extractor.get_lines_from_db(
        pub_filename="ARN30948-PAM_670-1-000-WEB-1.pdf",
        page_ix=36,
        start_ix=35,
        sqlite_db="./sqlite_dbs/pub_sentences.db",
        num_lines=6
        )
    try:
        assert len(lines) == 6
        print("Test passed.")
        print(lines)
    except AssertionError:
        print("Test failed.")
        print(lines)

if __name__ == '__main__':
    # catalogue_batch()
    get_lines_from_db_test()