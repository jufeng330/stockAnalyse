import requests

def test_entry_decision_analyze():
    # Use the stock_id provided in the prompt
    watch_stock_id = 'WS-16BBC7F983FB'
    url = f"http://192.168.1.12:38080/api/trading-decision/watch-stocks/{watch_stock_id}/entry-decision/analyze"

    print(f"Testing URL: {url}")
    try:
        response = requests.post(url, json={})
        print(f"Status Code: {response.status_code}")
        print(f"Response Body: {response.text}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    test_entry_decision_analyze()
