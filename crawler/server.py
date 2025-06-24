from flask import Flask, request, jsonify
import subprocess
import os
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__)

@app.route('/crawl', methods=['POST'])
def run_main():
    post_limit = request.json.get("post_limit", 3)
    reel_limit = request.json.get("reel_limit", 1)

    try:
        cmd = [
            "python3",
            os.path.join(BASE_DIR, "main.py"),
            "--post_limit", str(post_limit),
            "--reel_limit", str(reel_limit)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode == 0:
            return jsonify({
                "status": "success",
                "stdout": result.stdout
            }), 200
        else:
            return jsonify({
                "status": "error",
                "stderr": result.stderr
            }), 500

    except Exception as e:
        return jsonify({
            "status": "exception",
            "message": str(e)
        }), 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True, port=5000)
