import json
import requests

def post_teams(webhook_url: str, message: str) -> None:
    print("\n============== TEAMS message ==============")
    print(message)
    print("\n============== END TEAMS message ==============")

    payload = {"body": message}

    response = requests.post(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        timeout=30
    )

    print("Status:", response.status_code)
    response.raise_for_status()