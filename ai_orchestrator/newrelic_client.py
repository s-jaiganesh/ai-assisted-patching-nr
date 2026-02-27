import requests
from typing import Optional, Dict, Any, List


class NewRelicClient:
    def __init__(
        self,
        api_key: str,
        graphql_url: str = "https://api.newrelic.com/graphql",
        debug: bool = False,
    ):
        self.graphql_url = graphql_url
        self.s = requests.Session()
        self.s.headers.update(
            {
                "Content-Type": "application/json",
                "API-Key": api_key,
            }
        )
        self.debug = debug

    def _gql(self, query: str) -> dict:
        if self.debug:
            print(f"\n[NR-DEBUG] GQL query:\n{query}\n")

        r = self.s.post(self.graphql_url, json={"query": query}, timeout=30)
        r.raise_for_status()
        data = r.json()

        if self.debug:
            print(f"\n[NR-DEBUG] GQL response:\n{data}\n")

        if "errors" in data and data["errors"]:
            raise RuntimeError(str(data["errors"][0]))

        return data

    def find_host(self, hostname: str) -> Optional[Dict[str, Any]]:
        hostname = hostname.replace('"', '\\"')

        q = f"""
{{
  actor {{
    entitySearch(query: "type = 'HOST' AND name = '{hostname}'") {{
      results {{
        entities {{
          name reporting entityType guid accountId
        }}
      }}
    }}
  }}
}}
"""
        data = self._gql(q)

        ents = (
            data.get("data", {})
            .get("actor", {})
            .get("entitySearch", {})
            .get("results", {})
            .get("entities", [])
        )

        return ents[0] if ents else None

    def host_health(self, host_guid: str) -> Dict[str, Any]:
        q = f"""
{{
  actor {{
    entity(guid: "{host_guid}") {{
      name
      reporting
      alertSeverity
      recentAlertViolations(count: 1) {{
        label
        openedAt
      }}
    }}
  }}
}}
"""
        data = self._gql(q)

        ent = data.get("data", {}).get("actor", {}).get("entity", {}) or {}

        viol = ent.get("recentAlertViolations")
        if isinstance(viol, list) and viol:
            v0 = viol[0]
        else:
            v0 = {}

        return {
            "infra_reporting": ent.get("reporting"),
            "infra_alert": ent.get("alertSeverity"),
            "infra_alert_label": v0.get("label"),
            "infra_alert_openedAt": v0.get("openedAt"),
        }

    def find_apm_app_by_name(self, app_name: str) -> Optional[Dict[str, Any]]:
        app_name = app_name.replace('"', '\\"')

        q = f"""
{{
  actor {{
    entitySearch(query: "type = 'APPLICATION' AND name = '{app_name}'") {{
      results {{
        entities {{
          name guid accountId
        }}
      }}
    }}
  }}
}}
"""
        data = self._gql(q)

        ents = (
            data.get("data", {})
            .get("actor", {})
            .get("entitySearch", {})
            .get("results", {})
            .get("entities", [])
        )

        return ents[0] if ents else None

    def related_apm_apps(self, host_guid: str) -> List[Dict[str, Any]]:
        q = f"""
{{
  actor {{
    entity(guid: "{host_guid}") {{
      relatedEntities(
        filter: {{entityDomainTypes: {{include: {{domain: "APM", type: "APPLICATION"}}}}}}
      ) {{
        results {{
          target {{
            guid
            accountId
            entity {{ accountId }}
          }}
        }}
      }}
    }}
  }}
}}
"""
        data = self._gql(q)

        results = (
            data.get("data", {})
            .get("actor", {})
            .get("entity", {})
            .get("relatedEntities", {})
            .get("results", [])
        )

        apps = []

        for r in results:
            t = (r or {}).get("target") or {}
            guid = t.get("guid")
            acct = t.get("accountId") or (t.get("entity") or {}).get("accountId")

            if not guid or not acct:
                continue

            try:
                acct = int(acct)
            except (TypeError, ValueError):
                continue

            apps.append({"app_guid": guid, "account_id": acct})

        return apps

    def apm_alert(self, app_guid: str) -> Dict[str, Any]:
        q = f"""
{{
  actor {{
    entity(guid: "{app_guid}") {{
      name
      alertSeverity
    }}
  }}
}}
"""
        data = self._gql(q)

        ent = data.get("data", {}).get("actor", {}).get("entity", {}) or {}

        return {
            "apm_alert": ent.get("alertSeverity"),
            "apm_name": ent.get("name"),
        }

    def apm_traffic_rate(
        self, account_id: int, app_guid: str, minutes: int = 5
    ) -> float:
        q = f"""
{{
  actor {{
    account(id: {account_id}) {{
      nrql(query: "SELECT rate(count(*), 1 minute) FROM Transaction WHERE entityGuid = '{app_guid}' SINCE {minutes} minutes ago") {{
        results
      }}
    }}
  }}
}}
"""
        data = self._gql(q)

        results = (
            data.get("data", {})
            .get("actor", {})
            .get("account", {})
            .get("nrql", {})
            .get("results", [])
        )

        if not results:
            return 0.0

        row = results[0] or {}

        for v in row.values():
            try:
                return float(v)
            except (TypeError, ValueError):
                continue

        return 0.0

    def snapshot_for_host(
        self, hostname: str, traffic_minutes: int = 5
    ) -> Dict[str, Any]:

        host = self.find_host(hostname)

        if not host:
            return {
                "hostname": hostname,
                "nr_found": False,
                "reason": "HOST_NOT_FOUND",
            }

        host_guid = host.get("guid")
        acct = host.get("accountId")

        if not host_guid:
            return {
                "hostname": hostname,
                "nr_found": False,
                "reason": "HOST_GUID_MISSING",
            }

        host_health_data = self.host_health(host_guid)

        base_snapshot = {
            "nr_found": True,
            "hostname": hostname,
            "host_guid": host_guid,
            "host_account_id": acct,
            **host_health_data,
        }

        apps_to_process = self.related_apm_apps(host_guid)

        if not apps_to_process:
            if self.debug:
                print(
                    f"[NR-DEBUG] No related APM apps found for {hostname}. Searching by name fallback."
                )

            found_app = self.find_apm_app_by_name(hostname)

            if found_app:
                apps_to_process = [
                    {
                        "app_guid": found_app.get("guid"),
                        "account_id": found_app.get("accountId"),
                    }
                ]

        apm_services_list = []

        for app in apps_to_process:
            app_guid = app.get("app_guid")
            account_id = app.get("account_id")

            if not app_guid or not account_id:
                continue

            apm_alert_data = self.apm_alert(app_guid)
            traffic = self.apm_traffic_rate(
                account_id, app_guid, minutes=traffic_minutes
            )

            apm_services_list.append(
                {
                    "guid": app_guid,
                    "account_id": account_id,
                    "name": apm_alert_data.get("apm_name"),
                    "alert": apm_alert_data.get("apm_alert"),
                    "traffic": traffic,
                }
            )

        return {
            **base_snapshot,
            "apm_services": apm_services_list,
        }