"""Legacy API CLI shim.

The dashboard and production services now run entirely on the Postgres-backed
v2 API. Keep `python -m src.edu_cti.api` working by delegating to the v2 API
entrypoint instead of the deprecated SQLite-era public surface.
"""

from __future__ import annotations

from src.edu_cti_v2.api_server import main as v2_main


def main() -> None:
    print("Delegating legacy API entrypoint to the Postgres-backed v2 API server.")
    v2_main()


if __name__ == "__main__":
    main()
