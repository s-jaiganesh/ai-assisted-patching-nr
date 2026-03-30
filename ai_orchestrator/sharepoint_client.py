import requests

class SharePointClient:
    def __init__(self, tenant_id, client_id, client_secret, hostname, site_path, drive_name="Documents"):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.hostname = hostname
        self.site_path = site_path
        self.drive_name = drive_name
        self.token = None
        self.site_id = None
        self.drive_id = None

    # -----------------------------
    # AUTH
    # -----------------------------
    def get_access_token(self):
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://graph.microsoft.com/.default"
        }
        res = requests.post(url, data=data, timeout=30)
        res.raise_for_status()
        self.token = res.json()["access_token"]
        return self.token

    # -----------------------------
    # GET SITE ID
    # -----------------------------
    def get_site_id(self):
        if not self.token:
            self.get_access_token()
        url = f"https://graph.microsoft.com/v1.0/sites/{self.hostname}:{self.site_path}"
        headers = {"Authorization": f"Bearer {self.token}"}
        res = requests.get(url, headers=headers, timeout=30)
        res.raise_for_status()
        self.site_id = res.json()["id"]
        return self.site_id

    # -----------------------------
    # GET DRIVE ID
    # -----------------------------
    def get_drive_id(self):
        if not self.site_id:
            self.get_site_id()

        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives"
        headers = {"Authorization": f"Bearer {self.token}"}
        res = requests.get(url, headers=headers, timeout=30)
        res.raise_for_status()
        drives = res.json().get("value", [])
        for d in drives:
            if d.get("name") == self.drive_name:
                self.drive_id = d["id"]
                return self.drive_id
        raise RuntimeError(f"Drive '{self.drive_name}' not found")

    # -----------------------------
    # UPLOAD FILE
    # -----------------------------
    def upload_file(self, folder_path: str, file_name: str, local_path: str) -> str:
        if not self.drive_id:
            self.get_drive_id()
        upload_url = (
            f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.drive_id}"
            f"/root:/{folder_path}/{file_name}:/content"
        )
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/octet-stream"
        }
        with open(local_path, "rb") as f:
            res = requests.put(upload_url, headers=headers, data=f, timeout=60)
        res.raise_for_status()
        return res.json().get("webUrl")