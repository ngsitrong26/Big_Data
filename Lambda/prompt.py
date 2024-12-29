from transformers import GPT2LMHeadModel, GPT2Tokenizer
import torch

# Initialize the model and tokenizer
model_name = "gpt2"  # You can use a smaller GPT-2 model or another suitable one for your use case
model = GPT2LMHeadModel.from_pretrained(model_name)
tokenizer = GPT2Tokenizer.from_pretrained(model_name)

# Updated system prompt template for the chatbot, adjusted for dynamic website content interaction
system_prompt = """You are an AI-powered chatbot designed to assist users by answering questions about the content on {website_name}. {website_info}

Your task is to answer user queries by extracting and providing accurate information from the content available on {website_name}. You should always provide relevant and useful information, without introducing any external sources unless explicitly mentioned in the content. If you don't know the answer, inform the user politely and suggest they refer to the website or contact support. 

The ways to contact support are: {contact_info}.
Never make up information, and always ask follow-up questions if necessary for clarification. If there are multiple offerings related to the user’s query, make sure to ask them for further details or provide them with choices.

Use the following context to answer the user's question:

----------------

{context}
{chat_history}
Follow-up question: """

def get_prompt():
    """
    Generates the system prompt template for the chatbot based on website content.

    Returns:
        str: Formatted system prompt.
    """
    return system_prompt

def generate_response(context, question, chat_history, website_name, website_info, contact_info):
    """
    Generates a response using GPT-2 model.
    
    Args:
        context (str): The context or background information for the query, like recent products or services.
        question (str): The user's question.
        chat_history (str): The history of previous chat messages to maintain context.
        website_name (str): The name of the website being queried.
        website_info (str): Information about the website or the services/products offered.
        contact_info (str): Contact details for customer support.

    Returns:
        str: Generated response from the model.
    """
    # Format the system prompt with the provided input
    formatted_prompt = get_prompt().format(
        context=context,
        question=question,
        chat_history=chat_history,
        website_name=website_name,
        website_info=website_info,
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
