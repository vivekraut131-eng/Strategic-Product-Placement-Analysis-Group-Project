#!/usr/bin/env python3
"""
Flask web application — embeds Tableau Public dashboard for
Strategic Product Placement Analysis.
"""

import os

from flask import Flask, render_template

# Resolve project root so templates/static live alongside flask_app/
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

app = Flask(
    __name__,
    template_folder=os.path.join(BASE_DIR, "templates"),
    static_folder=os.path.join(BASE_DIR, "static"),
)


@app.route("/")
def dashboard():
    """Serve main dashboard page with embedded Tableau visualization."""
    return render_template("dashboard.html")


if __name__ == "__main__":
    # Production: use a WSGI server (gunicorn/waitress). Dev: Flask built-in.
    app.run(host="127.0.0.1", port=5000, debug=True)
