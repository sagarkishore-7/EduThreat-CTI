"""
API Server CLI entry point.

Run with: python -m src.edu_cti.api

For Railway deployment, set PORT environment variable.
"""

import argparse
import os
import uvicorn


def main():
    parser = argparse.ArgumentParser(
        description="Start the EduThreat-CTI API server"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Host to bind to (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Port to bind to (default: 8000 or PORT env var)",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload for development",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of worker processes (default: 1)",
    )
    
    args = parser.parse_args()
    
    # Use PORT env var (Railway provides this), fallback to CLI arg, then default
    port = args.port or int(os.environ.get("PORT", 8000))
    
    print(f"Starting EduThreat-CTI API server on {args.host}:{port}")
    print(f"API documentation available at: http://localhost:{port}/docs")
    
    uvicorn.run(
        "src.edu_cti.api.main:app",
        host=args.host,
        port=port,
        reload=args.reload,
        workers=args.workers if not args.reload else 1,
    )


if __name__ == "__main__":
    main()

