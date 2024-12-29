import sys
sys.path.append('F:/ITTN_Project/Big_Data/Big-Data-Project/Lambda')

import requests
from bs4 import BeautifulSoup
import html2text
import logging
import time
from Lambda.producer import send_message

# Setup logging
logging.basicConfig(level=logging.INFO)

def get_data_from_website(url):
    """
    Retrieve text content and metadata from a given URL.
    
    Args:
        url (str): The URL to fetch content from.

    Returns:
        tuple: A tuple containing the text content (str) and metadata (dict).
    """
    # Send a GET request to the URL
    try:
        response = requests.get(url, timeout=10)  # 10 seconds timeout
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching the URL {url}: {e}")
        return None, None

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    # Remove JS and CSS elements
    for script in soup(["script", "style"]):
        script.extract()

    # Convert HTML to Markdown
    html2text_instance = html2text.HTML2Text()
    html2text_instance.images_to_alt = True
    html2text_instance.body_width = 0
    html2text_instance.single_line_break = True
    text = html2text_instance.handle(str(soup))

    # Extract page metadata
    page_title = soup.title.string.strip() if soup.title else "Untitled Page"
    meta_description = soup.find("meta", attrs={"name": "description"})
    meta_keywords = soup.find("meta", attrs={"name": "keywords"})

    description = meta_description.get("content") if meta_description else page_title
    keywords = meta_keywords.get("content") if meta_keywords else ""

    metadata = {
        'title': page_title,
        'url': url,
        'description': description,
        'keywords': keywords
    }

    return text, metadata

def crawl_and_send_to_kafka(url, batch_mode=False):
    """
    Crawl a website and send data to Kafka.
    
    Args:
        url (str): The URL to crawl.
        batch_mode (bool): Whether the crawl is for batch processing (default: False).
    """
    text, metadata = get_data_from_website(url)

    if text and metadata:
        logging.info(f"Successfully crawled {url}. Sending data to Kafka...")
        
        data = {
            'text': text,
            'metadata': metadata,
        }
        
        topic = 'real_time_data'
        
        if batch_mode == True:
            topic = 'historical_data'
        
        # Send to Kafka
        send_message(data, topic)
        logging.info(f"Data sent to Kafka for {url}.")
    else:
        logging.warning(f"Failed to crawl or process data for {url}.")

def start_crawling(urls, batch_mode=False):
    """
    Start crawling multiple URLs and send data to Kafka.
    
    Args:
        urls (list): List of URLs to crawl.
        batch_mode (bool): Whether the crawl is for batch processing (default: False).
    """
    for url in urls:
        crawl_and_send_to_kafka(url, batch_mode)
        time.sleep(2)  # Delay between crawls to avoid overloading server
    