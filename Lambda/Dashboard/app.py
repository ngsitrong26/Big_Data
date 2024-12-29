from flask import Flask, render_template, request
from prometheus_client import Counter, Histogram, generate_latest
import time

app = Flask(__name__)

# Initialize Prometheus metrics
QUERY_COUNT = Counter('query_count', 'Total number of queries received')
RESPONSE_TIME = Histogram('response_time_seconds', 'Histogram of response times for queries')

@app.before_request
def before_request():
    # Track the start time of the request
    request.start_time = time.time()

@app.route('/')
def dashboard():
    return render_template('index.html', query_count=QUERY_COUNT._value.get(), avg_response_time=RESPONSE_TIME._sum.get() / QUERY_COUNT._value.get())

@app.route('/metrics')
def metrics():
    global QUERY_COUNT, RESPONSE_TIME
    query_count = QUERY_COUNT.inc()  # Increment the query count
    response_time = time.time() - request.start_time
    RESPONSE_TIME.observe(response_time)  # Observe the response time
    return generate_latest()  # Expose the metrics in Prometheus format

if __name__ == '__main__':
    app.run(debug=True)
