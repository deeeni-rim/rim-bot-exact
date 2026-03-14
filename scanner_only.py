import asyncio

from db import init_db
from scanner import run_scanner


async def main():
    init_db()
    print("scanner_only.py: before run_scanner()", flush=True)
    await run_scanner(None)


if __name__ == "__main__":
    asyncio.run(main())