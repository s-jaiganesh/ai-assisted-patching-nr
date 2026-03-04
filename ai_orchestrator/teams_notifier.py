import requests

def post_teams(webhook_url: str, message: str) -> None:
    print("\n============== TEAMS message ==============")
    print(message)
    print("\n============== END TEAMS message ==============")
    r = requests.post(webhook_url, json={"body": message}, timeout=30)
    r.raise_for_status()
