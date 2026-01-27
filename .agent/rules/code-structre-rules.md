---
trigger: always_on
---

# Vertical Slice Architecture (VSA) + AI Agents

This guide is written for **both humans and AI coding agents**.

* **Humans**: learn Vertical Slice Architecture quickly and apply it to real FastAPI services.
* **Agents**: use this as repo instructions (copy/paste into `AGENTS.md`).

---

## What is Vertical Slice Architecture?

**Vertical Slice Architecture (VSA)** organizes code by **features / use-cases** instead of technical layers.

### Layered (NOT VSA)

```txt
routers/
services/
repositories/
models/
```

### Vertical Slice (VSA)

```txt
features/
  create_user/
  get_user/
  update_user/
```

Each slice contains everything needed for that feature end-to-end:

* API endpoint
* request/response schemas
* business logic
* persistence logic (repo) or an adapter
* tests

**Core idea:**

> Keep code that changes together **together**.

---

## Why VSA is good for AI coding agents

AI coding agents work best when:

* changes are **local** (few files)
* the scope is **explicit**
* tests provide **verifiable feedback**

VSA naturally improves:

### ✅ Context locality

A slice is a “bundle” the agent can load into context and understand.

### ✅ Predictable navigation

Agents can always find:

* `endpoint.py` (entry point)
* `schemas.py` (contracts)
* `service.py` or `use_case.py` (behavior)
* `repository.py` (persistence)

### ✅ Easier debugging

Bugs are usually fixed within the slice.

### ✅ Testing becomes obvious

Agents can add tests per slice without needing full system knowledge.

---

## Recommended Folder Structure (FastAPI)

```txt
app/
  main.py                     # app wiring
  config/
    settings.py               # environment config
  common/
    db.py                     # session creation / engine
    errors.py                 # shared exceptions
    logging.py                # observability
  features/
    users/
      create_user/
        endpoint.py
        schemas.py
        service.py
        repository.py
        tests.py
      get_user/
        endpoint.py
        schemas.py
        service.py
        repository.py
        tests.py
```

### Naming conventions (important)

Keep consistent filenames across slices:

* `endpoint.py`
* `schemas.py`
* `service.py`
* `repository.py`
* `tests.py`

Agents rely on consistency.

---

## Slice Anatomy (what goes where)

### 1) `schemas.py` (contracts)

Only:

* Pydantic request/response models
* Validation constraints

✅ Good

* `CreateUserRequest`
* `CreateUserResponse`

❌ Not here

* database calls
* business logic

---

### 2) `endpoint.py` (HTTP boundary)

Only:

* FastAPI route definitions
* mapping request → service
* returning response schema

✅ Good

* dependency injection (`Depends(get_db)`)
* minimal glue

❌ Not here

* complex business logic
* SQL queries

---

### 3) `service.py` (use-case / business behavior)

Only:

* business rules
* orchestration across dependencies

✅ Good

* normalize input
* apply domain rules
* call repository

❌ Not here

* FastAPI specifics
* raw SQL

---

### 4) `repository.py` (persistence)

Only:

* DB queries
* inserts / updates

✅ Good

* SQLAlchemy calls
* query functions

❌ Not here

* business rules
* HTTP response formatting

---

## FastAPI Example Slice (reference)

> **Goal**: `POST /users` creates a user.

### `features/users/create_user/schemas.py`

```python
from pydantic import BaseModel, EmailStr

class CreateUserRequest(BaseModel):
    name: str
    email: EmailStr

class CreateUserResponse(BaseModel):
    id: int
    name: str
    email: EmailStr
```

### `features/users/create_user/repository.py`

```python
from sqlalchemy.orm import Session
from app.models import User

def insert_user(db: Session, name: str, email: str) -> User:
    user = User(name=name, email=email)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user
```

### `features/users/create_user/service.py`

```python
from sqlalchemy.orm import Session
from .repository import insert_user

def create_user(db: Session, name: str, email: str):
    email = email.lower().strip()
    return insert_user(db, name=name, email=email)
```

### `features/users/create_user/endpoint.py`

```python
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.common.db import get_db
from .schemas import CreateUserRequest, CreateUserResponse
from .service import create_user

router = APIRouter(tags=["users"])

@router.post("/users", response_model=CreateUserResponse)
def create_user_endpoint(payload: CreateUserRequest, db: Session = Depends(get_db)):
    user = create_user(db, name=payload.name, email=payload.email)
    return CreateUserResponse(id=user.id, name=user.name, email=user.email)
```

### `app/main.py`

```python
from fastapi import FastAPI
from app.features.users.create_user.endpoint import router as create_user_router

app = FastAPI()
app.include_router(create_user_router)
```

---

## Testing Rules (agent-friendly)

### Test types

1. **Unit tests** → test `service.py` logic quickly.
2. **Integration tests** → test `endpoint.py` behavior with a test DB.

### Minimal unit test example

```python
from app.features.users.create_user.service import create_user

def test_email_normalization(fake_db_session):
    user = create_user(fake_db_session, name="Ava", email="TEST@EXAMPLE.COM ")
    assert user.email == "test@example.com"
```

### Minimal endpoint test example

```python
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_create_user_endpoint():
    res = client.post("/users", json={"name": "Ava", "email": "ava@test.com"})
    assert res.status_code == 200
    assert res.json()["email"] == "ava@test.com"
```

---

## Anti-Patterns (what breaks VSA)

### ❌ 1) Giant shared `services/` folder

If everything ends up in shared services, VSA is lost.

### ❌ 2) Slices calling other slices directly

Instead:

* share via **common module** (strictly minimal)
* or publish domain-level utilities

### ❌ 3) Too many micro-files per slice

Example (bad):

* `use_case_impl_v2_helpers_final.py`

Keep slices small and predictable.

### ❌ 4) Business logic inside `endpoint.py`

Endpoints must be glue only.

---

## AI Agent Rules (copy into `AGENTS.md`)

### Architecture rules

* This repo uses **Vertical Slice Architecture**.
* Implement new features by creating a new slice under `app/features/`.
* Keep changes inside the slice unless there is a clear reason.

### Slice rules

For every new endpoint slice, create:

* `schemas.py`
* `endpoint.py`
* `service.py`
* `repository.py`
* `tests.py`

### Dependency rules

* `endpoint.py` may import `service.py`
* `service.py` may import `repository.py`
* `repository.py` may import DB models

Forbidden:

* `repository.py` importing FastAPI
* `endpoint.py` containing business logic

### Testing rules

* Every feature change must include a test update.
* Prefer unit tests for service logic.
* Add integration test if endpoint behavior changes.

### Style rules

* Keep code explicit and readable.
* Avoid hidden global state.
* Prefer type hints.

### Agent workflow

When implementing a change:

1. Identify the slice folder.
2. Read `schemas.py` → `endpoint.py` → `service.py` → `repository.py`.
3. Implement the smallest correct change.
4. Add/adjust tests.
5. Ensure tests pass.

---

## Agent Task Checklist

When you finish a task, confirm:

* [ ] logic is located in the correct file
* [ ] slice names follow conventions
* [ ] tests cover the new behavior
* [ ] code remains local to the slice

---

## Optional: Advanced VSA (Ports & Adapters inside slices)

For larger systems, you can evolve slices into:

* `application/` (use-cases)
* `ports.py` (interfaces)
* `adapters/` (db/external)

But start simple.

---

## Quick Mental Model

**One feature = one folder.**

If the agent is working on a feature and needs to change 10 unrelated files, VSA is failing.
