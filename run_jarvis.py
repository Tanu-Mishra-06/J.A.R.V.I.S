"""
J.A.R.V.I.S. Platform Runner
Justified Analysis, Reasoning & Visualization of Incident Systems

Starts the complete AIOps backend server & observability dashboard.
Open in your browser at: http://localhost:5000/
"""

import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.join(BASE_DIR, "backend")
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from app.main import app
from app.core.config import settings

if __name__ == "__main__":
    port = settings.PORT
    print("=" * 65)
    print(" J.A.R.V.I.S. - AIOps Incident Intelligence Platform")
    print(f" Observability Dashboard: http://localhost:{port}/")
    print(f" REST API Base URL:       http://localhost:{port}/api/")
    print(f" OpenTelemetry OTLP:      http://localhost:{port}/v1/")
    print("=" * 65)
    app.run(host="0.0.0.0", port=port, debug=False)
