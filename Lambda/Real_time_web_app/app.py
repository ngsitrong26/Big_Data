from flask import Flask, request, jsonify
from transformers import pipeline
import logging

# Initialize Flask app
app = Flask(__name__)

# Set up logging for better visibility of errors and requests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load pre-trained GPT-2 model for text generation
chatbot = pipeline("text-generation", model="gpt-2")

@app.route('/ask', methods=['POST'])
def ask():
    """
    Endpoint to handle user queries and generate chatbot responses.

    Accepts a JSON payload with a 'query' field and returns a generated response
    based on the chatbot model.
    """
    # Get query from the request JSON
    query = request.json.get('query')

    if not query:
        logger.error("No query provided.")
        return jsonify({"error": "Query is required"}), 400

    # Log the query for monitoring purposes
    logger.info(f"Received query: {query}")

    try:
        # Generate a response using the GPT-2 model
        response = chatbot(query, max_length=100)  # You can adjust max_length as needed
        generated_text = response[0]['generated_text']

        # Log the generated response for monitoring purposes
        logger.info(f"Generated response: {generated_text}")

        # Return the response as JSON
        return jsonify({"response": generated_text})

    except Exception as e:
        logger.error(f"Error generating response: {str(e)}")
        return jsonify({"error": "Error generating response, please try again later"}), 500

if __name__ == '__main__':
    app.run(debug=True)
