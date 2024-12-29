import re

class Document:
    """
    A simple class to simulate the Document object from Langchain.
    """
    def __init__(self, page_content, metadata):
        self.page_content = page_content
        self.metadata = metadata

    def __repr__(self):
        return f"Document(content={self.page_content[:30]}, metadata={self.metadata})"


# Data Cleaning functions

def merge_hyphenated_words(text):
    return re.sub(r"(\w)-\n(\w)", r"\1\2", text)


def fix_newlines(text):
    return re.sub(r"(?<!\n)\n(?!\n)", " ", text)


def remove_multiple_newlines(text):
    return re.sub(r"\n{2,}", "\n", text)


def clean_text(text):
    """
    Cleans the text by passing it through a list of cleaning functions.

    Args:
        text (str): Text to be cleaned

    Returns:
        str: Cleaned text
    """
    cleaning_functions = [merge_hyphenated_words, fix_newlines, remove_multiple_newlines]
    for cleaning_function in cleaning_functions:
        text = cleaning_function(text)
    return text


def split_text_into_chunks(text, chunk_size=2048, chunk_overlap=128):
    """
    Splits text into smaller chunks with overlap.

    Args:
        text (str): The text to be split.
        chunk_size (int): The maximum size of each chunk.
        chunk_overlap (int): The number of characters that overlap between chunks.

    Returns:
        List[str]: List of text chunks.
    """
    # Split the text into chunks, adding overlap
    chunks = []
    start = 0
    while start < len(text):
        end = min(start + chunk_size, len(text))
        chunk = text[start:end]
        chunks.append(chunk)
        start = end - chunk_overlap  # overlap between chunks
    return chunks


def text_to_docs(text, metadata):
    """
    Converts input text to a list of Documents with metadata.

    Args:
        text (str): A string of text.
        metadata (dict): A dictionary containing the metadata.

    Returns:
        List[Document]: List of documents.
    """
    doc_chunks = []
    chunks = split_text_into_chunks(text)  # Split text into chunks
    for chunk in chunks:
        doc = Document(page_content=chunk, metadata=metadata)
        doc_chunks.append(doc)
    return doc_chunks


def get_doc_chunks(text, metadata):
    """
    Processes the input text and metadata to generate document chunks.

    This function takes the raw text content and associated metadata, cleans the text,
    and divides it into document chunks.

    Args:
        text (str): The raw text content to be processed.
        metadata (dict): Metadata associated with the text content.

    Returns:
        List[Document]: List of documents.
    """
    text = clean_text(text)
    doc_chunks = text_to_docs(text, metadata)
    return doc_chunks
