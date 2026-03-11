"""API route stubs — full implementation to follow."""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter()


# --- Health ---
@router.get("/health")
async def health():
    return {"status": "ok"}


# --- Clients ---
# GET    /clients
# POST   /clients
# GET    /clients/{id}
# PATCH  /clients/{id}

# --- Institutions ---
# GET    /institutions
# POST   /institutions
# GET    /institutions/{id}
# PATCH  /institutions/{id}
# GET    /institutions/{id}/watch-terms
# POST   /institutions/{id}/watch-terms

# --- Findings ---
# GET    /findings              (paginated, filterable)
# GET    /findings/{id}
# PATCH  /findings/{id}         (update status, notes, assignment)
# GET    /findings/stats        (counts by severity, status, institution)

# --- Sources ---
# GET    /sources
# POST   /sources
# PATCH  /sources/{id}
# POST   /sources/{id}/poll     (trigger manual poll)

# --- Dashboard ---
# GET    /dashboard/overview    (aggregate stats)
# GET    /dashboard/timeline    (findings over time)

# --- Auth ---
# POST   /auth/login
# POST   /auth/logout
# GET    /auth/me

# --- Notifications ---
# GET    /notifications
# PATCH  /notifications/{id}/read
# POST   /notifications/read-all

# --- Alert Rules ---
# GET    /alert-rules
# POST   /alert-rules
# PATCH  /alert-rules/{id}
# DELETE /alert-rules/{id}
