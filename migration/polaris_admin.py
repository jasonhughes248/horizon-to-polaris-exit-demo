"""Thin client for the Polaris management API and register endpoint, as the root principal."""

from __future__ import annotations

import requests

from migration.common import DEMO_USERS, env, has_state, polaris_uri, read_state

REALM_HEADER = {"Polaris-Realm": "POLARIS"}


class PolarisAdmin:
    def __init__(self):
        root_id, root_secret = env("POLARIS_ROOT_CREDENTIAL").split(":", 1)
        resp = requests.post(
            f"{polaris_uri()}/v1/oauth/tokens",
            data={"grant_type": "client_credentials", "client_id": root_id, "client_secret": root_secret, "scope": "PRINCIPAL_ROLE:ALL"},
            headers=REALM_HEADER,
            timeout=30,
        )
        resp.raise_for_status()
        self.headers = {"Authorization": f"Bearer {resp.json()['access_token']}", **REALM_HEADER}
        self.mgmt = polaris_uri().removesuffix("/api/catalog") + "/api/management/v1"

    def _call(self, method: str, url: str, ok_conflict: bool = False, **kwargs) -> requests.Response:
        resp = requests.request(method, url, headers=self.headers, timeout=60, **kwargs)
        if ok_conflict and resp.status_code == 409:
            return resp
        if resp.status_code >= 400:
            raise RuntimeError(f"{method} {url.split('/api/')[-1]} -> {resp.status_code}: {resp.text[:400]}")
        return resp

    # catalogs / namespaces / tables
    def create_catalog(self, body: dict) -> bool:
        return self._call("POST", f"{self.mgmt}/catalogs", ok_conflict=True, json={"catalog": body}).status_code != 409

    def get_catalog(self, name: str) -> dict:
        return self._call("GET", f"{self.mgmt}/catalogs/{name}").json()

    def create_namespace(self, catalog: str, namespace: str) -> None:
        self._call("POST", f"{polaris_uri()}/v1/{catalog}/namespaces", ok_conflict=True, json={"namespace": [namespace], "properties": {}})

    def load_table(self, catalog: str, namespace: str, table: str) -> dict:
        return self._call("GET", f"{polaris_uri()}/v1/{catalog}/namespaces/{namespace}/tables/{table}").json()

    def register_table(self, catalog: str, namespace: str, table: str, metadata_location: str,
                       force: bool = False) -> dict:
        """Register `table` at `metadata_location`.

        If the table already exists (409) and points at the same metadata file, the
        existing entry is returned. If it points elsewhere (e.g. commits were made
        through Polaris after registration), it is NOT re-pointed unless force=True;
        the existing entry is returned with "conflict": True so the caller can stop.
        """
        url = f"{polaris_uri()}/v1/{catalog}/namespaces/{namespace}/register"
        body = {"name": table, "metadata-location": metadata_location}
        resp = self._call("POST", url, ok_conflict=True, json=body)
        if resp.status_code != 409:
            return resp.json()
        existing = self.load_table(catalog, namespace, table)
        if existing.get("metadata-location") == metadata_location:
            return existing
        if not force:
            return {**existing, "conflict": True}
        return self._call("POST", url, json={**body, "overwrite": True}).json()

    # RBAC
    def credentials_work(self, creds: dict) -> bool:
        """True if these client credentials can obtain a token from this Polaris."""
        try:
            resp = requests.request(
                "POST", f"{polaris_uri()}/v1/oauth/tokens", headers=REALM_HEADER, timeout=30,
                data={"grant_type": "client_credentials", "client_id": creds["client_id"],
                      "client_secret": creds["client_secret"], "scope": "PRINCIPAL_ROLE:ALL"},
            )
        except requests.RequestException:
            return False
        return resp.status_code == 200

    @staticmethod
    def saved_credentials(name: str) -> dict | None:
        key = next((k for k, u in DEMO_USERS.items() if u["user"] == name), None)
        if key is None or not has_state("polaris_principals.json"):
            return None
        return read_state("polaris_principals.json").get(key)

    def create_principal(self, name: str) -> dict:
        """Returns {'client_id', 'client_secret', 'rotated'}.

        New principal: its fresh credentials. Existing principal: the saved
        credentials from state/polaris_principals.json if they still authenticate
        (nothing changes, so step 7's catalog integration keeps working); otherwise
        the secret is rotated and 'rotated' is True (step 7 must be rerun).
        """
        resp = self._call("POST", f"{self.mgmt}/principals", ok_conflict=True, json={"principal": {"name": name}})
        if resp.status_code == 409:
            saved = self.saved_credentials(name)
            if saved and self.credentials_work(saved):
                return {"client_id": saved["client_id"], "client_secret": saved["client_secret"], "rotated": False}
            print(f"  NOTE: {name} exists but its saved credentials no longer authenticate; rotating. "
                  "Rerun step 7 so Snowflake's catalog integration gets the new HORIZON_DEMO_RO secret.")
            resp = self._call("POST", f"{self.mgmt}/principals/{name}/rotate")
            creds = resp.json()["credentials"]
            return {"client_id": creds["clientId"], "client_secret": creds["clientSecret"], "rotated": True}
        creds = resp.json()["credentials"]
        return {"client_id": creds["clientId"], "client_secret": creds["clientSecret"], "rotated": True}

    def create_principal_role(self, name: str) -> None:
        self._call("POST", f"{self.mgmt}/principal-roles", ok_conflict=True, json={"principalRole": {"name": name}})

    def create_catalog_role(self, catalog: str, name: str) -> None:
        self._call("POST", f"{self.mgmt}/catalogs/{catalog}/catalog-roles", ok_conflict=True, json={"catalogRole": {"name": name}})

    def grant(self, catalog: str, catalog_role: str, grant: dict) -> None:
        self._call("PUT", f"{self.mgmt}/catalogs/{catalog}/catalog-roles/{catalog_role}/grants", json={"grant": grant})

    def list_grants(self, catalog: str, catalog_role: str) -> list[dict]:
        return self._call("GET", f"{self.mgmt}/catalogs/{catalog}/catalog-roles/{catalog_role}/grants").json().get("grants", [])

    def revoke(self, catalog: str, catalog_role: str, grant: dict) -> None:
        self._call("POST", f"{self.mgmt}/catalogs/{catalog}/catalog-roles/{catalog_role}/grants", json={"grant": grant})

    def assign_catalog_role(self, principal_role: str, catalog: str, catalog_role: str) -> None:
        self._call("PUT", f"{self.mgmt}/principal-roles/{principal_role}/catalog-roles/{catalog}", json={"catalogRole": {"name": catalog_role}})

    def assign_principal_role(self, principal: str, principal_role: str) -> None:
        self._call("PUT", f"{self.mgmt}/principals/{principal}/principal-roles", json={"principalRole": {"name": principal_role}})
