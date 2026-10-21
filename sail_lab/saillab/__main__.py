"""python -m saillab api | containers"""
import logging
import os
import sys


def main() -> None:
    cmd = sys.argv[1] if len(sys.argv) > 1 else "api"
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if cmd == "api":
        import uvicorn
        from .api import create_app
        uvicorn.run(create_app(), host="0.0.0.0", port=int(os.environ.get("PORT", "8010")))
    elif cmd == "containers":
        from .containers import main as containers
        containers()
    else:
        sys.exit(f"unknown command {cmd!r}; use api or containers")


if __name__ == "__main__":
    main()
