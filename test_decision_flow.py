import requests
import time

def test_entry_decision_flow():
    # 1. Start the analysis
    watch_stock_id = 'WS-16BBC7F983FB'
    base_url = "http://192.168.1.12:38080/api/trading-decision/watch-stocks"
    url = f"{base_url}/{watch_stock_id}/entry-decision/analyze"

    print(f"Starting analysis for: {watch_stock_id}")
    response = requests.post(url, json={})
    if response.status_code != 200:
        print(f"Failed to start: {response.text}")
        return

    data = response.json().get('data', {})
    session_id = data.get('session_id')
    print(f"Session ID: {session_id}")

    # 2. Poll the status
    poll_url = f"http://192.168.1.12:38080/api/trading-decision/entry-decisions/{session_id}"
    for i in range(10):
        print(f"Polling status {i+1}...")
        res = requests.get(poll_url)
        print(f"Status: {res.status_code}, Body: {res.text[:100]}...")
        if res.status_code == 200:
            state = res.json().get('data', {})
            # Assuming status field exists in the returned session object
            if state.get('status') == 'completed':
                print("Analysis completed!")
                break
        time.sleep(2)

if __name__ == '__main__':
    test_entry_decision_flow()
