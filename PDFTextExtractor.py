from pypdf import PdfReader

class PDFTextExtractor:

    def __init__(self):
        pass

    def catalogue_pdf(self, pdf_file) -> list:
        """ 
        Extracts the text from a PDF file and returns a list of pages, where each page is a list of sentences.

        Args:
            pdf_file: The path to the PDF file.

        Returns:
            A list of pages, where each page is a list of sentences.
        """
        pdf = PdfReader(pdf_file)
        text = ""
        pages = []
        for page in pdf.pages:
            text = page.extract_text()
            lines = text.replace("\n", " ").split(". ")
            lines_cleaned = []
            for line in lines:
                line = line.strip()
                while "  " in line:
                    line = line.replace("  ", " ")
                lines_cleaned.append(line)
            pages.append(lines_cleaned)
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

if __name__ == '__main__':
    pdf_text_extractor = PDFTextExtractor()
    text = pdf_text_extractor.catalogue_pdf("./pubs/da_pam/ARN30948-PAM_670-1-000-WEB-1.pdf")
    for ix, page in enumerate(text):
        print("Page: " + str(ix))
        for line in page:
            print("   " + line)