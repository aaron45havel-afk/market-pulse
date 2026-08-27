"""The ops surface, mounted into the existing app with two lines.

ARCHITECTURE.md §3: "market-pulse-v21/main.py gains one include_router
call per portal and nothing else." One router covering all of them is
fewer lines still, and keeps the ops routes discoverable in one place
for the enumeration guard in tests/test_ops_schema.py.
"""
from fastapi import APIRouter

from . import auth_routes, portals

router = APIRouter()
router.include_router(auth_routes.router)
router.include_router(portals.router)

__all__ = ["router"]
