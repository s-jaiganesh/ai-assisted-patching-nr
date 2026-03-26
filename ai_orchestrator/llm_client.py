import requests
import litellm

def get_gas_token(cfg: dict) -> str:
    r = requests.post(
        cfg["token_url"],
        data={
            "grant_type": "client_credentials",
            "client_id": cfg["client_id"],
            "client_secret": cfg["client_secret"]
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def call_llm(cfg: dict, system_prompt: str, user_prompt: str) -> str:

    token = get_gas_token(cfg)

    response = litellm.completion(
        model=cfg["model"],
        api_base=cfg["llm_proxy_url"],
        api_key=token,
        timeout=30,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )
    return response.choices[0].message.content
#    r = requests.post(
#        cfg["base_url"],
#        json={
#            "system": system_prompt,
#            "prompt": user_prompt
#        },
#        headers={
#            "Authorization": f"Bearer {token}",
#            "Content-Type": "application/json"
#        },
#        timeout=90,
#    )
#
#    r.raise_for_status()
#    data = r.json()
#
#    for k in ("text","output","response","content"):
#        if k in data and isinstance(data[k], str):
#            return data[k].strip()
#
#    return str(data)[:4000]
#