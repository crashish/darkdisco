"""DarkDisco management CLI."""

from __future__ import annotations

import asyncio
import sys

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import bcrypt
from darkdisco.common.models import User, UserRole
from darkdisco.config import settings


async def create_user(username: str, password: str, role: str = "admin") -> None:
    engine = create_async_engine(settings.database_url, echo=False)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with session_factory() as session:
        existing = await session.execute(select(User).where(User.username == username))
        if existing.scalar_one_or_none():
            print(f"User '{username}' already exists — updating password and role.")
            user = (await session.execute(select(User).where(User.username == username))).scalar_one()
            user.hashed_password = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
            user.role = UserRole(role)
            user.disabled = False
        else:
            user = User(
                username=username,
                hashed_password=bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode(),
                role=UserRole(role),
            )
            session.add(user)
            print(f"Creating user '{username}' with role '{role}'.")

        await session.commit()
        print("Done.")

    await engine.dispose()


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python -m darkdisco.cli <command> [args]")
        print("Commands:")
        print("  createuser <username> <password> [role]  - Create or update a user (role: admin|analyst)")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "createuser":
        if len(sys.argv) < 4:
            print("Usage: python -m darkdisco.cli createuser <username> <password> [role]")
            sys.exit(1)
        username = sys.argv[2]
        password = sys.argv[3]
        role = sys.argv[4] if len(sys.argv) > 4 else "admin"
        asyncio.run(create_user(username, password, role))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
