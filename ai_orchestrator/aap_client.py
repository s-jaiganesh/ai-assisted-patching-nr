import time
import requests
from typing import Callable, Optional


class AAPClient:
    def __init__(self, base_url: str, token: str, verify_ssl: bool = True):
        self.base_url = base_url.rstrip("/")
        self.verify_ssl = verify_ssl
        self.s = requests.Session()
        self.s.headers.update(
            {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            }
        )

    def launch_job_template(self, job_template_id: int, limit: str, extra_vars: dict) -> int:
        url = f"{self.base_url}/api/v2/job_templates/{job_template_id}/launch/"
        payload = {"limit": limit, "extra_vars": extra_vars}
        r = self.s.post(url, json=payload, verify=self.verify_ssl, timeout=60)
        r.raise_for_status()
        job_id = r.json().get("job")
        if not job_id:
            raise RuntimeError(f"Missing job id in launch response: {r.text[:500]}")
        return int(job_id)

    def get_job(self, job_id: int) -> dict:
        url = f"{self.base_url}/api/v2/jobs/{job_id}/"
        r = self.s.get(url, verify=self.verify_ssl, timeout=60)
        r.raise_for_status()
        return r.json()

    def wait_for_job(
        self,
        job_id: int,
        poll_seconds: int,
        timeout_seconds: int,
        heartbeat_seconds: int = 600,
        on_heartbeat: Optional[Callable[[dict], None]] = None,
    ) -> str:
        start = time.time()
        last_heartbeat = 0.0

        while True:
            j = self.get_job(job_id)
            status = j.get("status", "unknown")

            now = time.time()
            if on_heartbeat and (now - last_heartbeat) >= max(60, heartbeat_seconds):
                last_heartbeat = now
                try:
                    on_heartbeat(j)
                except Exception:
                    pass

            if status in ("successful", "failed", "error", "canceled"):
                return status
            if now - start > timeout_seconds:
                raise TimeoutError(f"AAP job {job_id} timeout after {timeout_seconds}s")
            time.sleep(max(5, poll_seconds))

    def get_hosts_by_group_name(self, inventory_id: int, group_name: str) -> list[str]:
        """
        Fetches all hostnames for a given group name within a specific inventory.
        First finds the group's ID, then fetches its hosts, handling pagination.
        """
        group_id_url = f"{self.base_url}/api/v2/inventories/{inventory_id}/groups/"
        params = {"name": group_name}
        r = self.s.get(group_id_url, params=params, verify=self.verify_ssl, timeout=60)
        r.raise_for_status()
        group_results = r.json().get("results")
        if not group_results:
            raise RuntimeError(f"Group '{group_name}' not found in inventory ID {inventory_id}.")
        group_id = group_results[0].get("id")
        hosts = []
        hosts_url = f"{self.base_url}/api/v2/groups/{group_id}/all_hosts/"
        while hosts_url:
            r = self.s.get(hosts_url, verify=self.verify_ssl, timeout=60)
            r.raise_for_status()
            data = r.json()
            for host_data in data.get("results", []):
                hosts.append(host_data["name"])
            hosts_url = data.get("next")

        return hosts

    def get_all_group_names(self, inventory_id: int) -> list[str]:
        """
        Fetches all group names from a specific inventory, handling pagination.
        """
        groups = []
        groups_url = f"{self.base_url}/api/v2/inventories/{inventory_id}/groups/"
        while groups_url:
            r = self.s.get(groups_url, verify=self.verify_ssl, timeout=60)
            r.raise_for_status()
            data = r.json()
            for group_data in data.get("results", []):
                groups.append(group_data["name"])
            groups_url = data.get("next")
        return groups

