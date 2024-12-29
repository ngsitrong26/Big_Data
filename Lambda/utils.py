from transformers import GPT2LMHeadModel, GPT2Tokenizer
from chromadb import Client
import torch
from text_to_doc import get_doc_chunks
from Lambda.web_crawler import get_data_from_website
from prompt import get_prompt

# Initialize the GPT-2 model and tokenizer
model_name = "gpt2"  # Use the GPT-2 model
model = GPT2LMHeadModel.from_pretrained(model_name)
tokenizer = GPT2Tokenizer.from_pretrained(model_name)

def get_chroma_client():
    """
    Returns a chroma vector store instance.

    Returns:
        chromadb.Client: ChromaDB client instance.
    """
    client = Client()
    collection = client.create_collection(name="website_data")
    return collection


def store_docs(url):
    """
    Retrieves data from a website, processes it into document chunks, and stores them in a vector store.

    Args:
        url (str): The URL of the website to retrieve data from.

    Returns:
        None
    """
    text, metadata = get_data_from_website(url)
    docs = get_doc_chunks(text, metadata)
    
    # Get Chroma client
    collection = get_chroma_client()

    # Store each document directly in ChromaDB with corresponding metadata
    for doc in docs:
        # Store text and metadata in Chroma collection
        collection.add([doc.page_content], metadatas=[doc.metadata])


def generate_response(context, question, chat_history, organization_name, organization_info, contact_info):
    """
    Generates a response using GPT-2 model.

    Args:
        context (str): The context or background information for the query.
        question (str): The user's question.
        chat_history (str): The history of previous chat messages.
        organization_name (str): The name of the organization.
        organization_info (str): Information about the organization.
        contact_info (str): Contact details for customer support.

    Returns:
        str: Generated response from the model.
    """
    # Format the system prompt with the provided input
    formatted_prompt = get_prompt().format(
        context=context,
        question=question,
        chat_history=chat_history,
        organization_name=organization_name,
        organization_info=organization_info,
        contact_info=contact_info
    )

    # Tokenize the input prompt
    inputs = tokenizer.encode(formatted_prompt, return_tensors='pt')

    # Generate the response
    with torch.no_grad():
        outputs = model.generate(
            inputs,
            max_length=512,  # Adjust the max length based on your needs
            num_return_sequences=1,
            no_repeat_ngram_size=2,
            top_p=0.9,
            top_k=50,
            temperature=0.7,
            pad_token_id=tokenizer.eos_token_id
        )

    # Decode and return the generated text
    response = tokenizer.decode(outputs[0], skip_special_tokens=True)
    return response


def make_chain():
    """
    Creates a retrieval chain for document-based question answering.
    
    Returns:
        function: Function to get the response based on the input question.
    """
    # This function will simulate a retrieval-based question answering chain
    # It fetches relevant documents and generates a response with GPT-2
    
    # You can implement a basic retriever here by querying the Chroma collection
    def retrieval_chain(query):
        collection = get_chroma_client()
        
        # Perform a simple retrieval: retrieve all docs for now (you can refine this logic)
        results = collection.query(query)  # Make sure to implement the query method correctly
        
        # Extract the relevant context (for now we use all the documents)
        context = "\n".join([doc["text"] for doc in results["documents"]])
        
        # Use GPT-2 to generate a response with the context and question
        return generate_response(context, query, "", "BestCompany", "BestCompany is a leader in the industry.", "Contact at support@bestcompany.com")
    
    return retrieval_chain


def get_response(question, organization_name, organization_info, contact_info):
    """
    Generates a response based on the input question.

    Args:
        question (str): The input question to generate a response for.
        organization_name (str): The name of the organization.
        organization_info (str): Information about the organization.
        contact_info (str): Contact information for the organization.

    Returns:
        str: The response generated by the GPT-2 model.
    """
    chain = make_chain()
    response = chain(question)
    return response
