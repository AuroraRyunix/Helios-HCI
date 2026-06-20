import os
import sys
import json
import base64
import urllib.request
import urllib.error

def get_file_sha(owner, repo, path, token):
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"token {token}")
    req.add_header("Accept", "application/vnd.github.v3+json")
    req.add_header("User-Agent", "Helios-HCI-Provisioning-Toolkit")
    try:
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())
            return data.get("sha")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        print(f"Error checking file SHA for {path}: HTTP {e.code} - {e.reason}")
        raise e

def upload_file_to_github(owner, repo, local_path, repo_path, token, commit_message):
    if not os.path.exists(local_path):
        print(f"Local file {local_path} does not exist. Skipping.")
        return False

    with open(local_path, "rb") as f:
        content_bytes = f.read()
    
    # Base64 encode file content
    content_base64 = base64.b64encode(content_bytes).decode('utf-8')
    
    # Check if the file already exists on remote to get its SHA (required for updates)
    sha = get_file_sha(owner, repo, repo_path, token)
    
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{repo_path}"
    
    body = {
        "message": commit_message,
        "content": content_base64
    }
    if sha:
        body["sha"] = sha
        
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="PUT")
    req.add_header("Authorization", f"token {token}")
    req.add_header("Accept", "application/vnd.github.v3+json")
    req.add_header("Content-Type", "application/json")
    req.add_header("User-Agent", "Helios-HCI-Provisioning-Toolkit")
    
    try:
        with urllib.request.urlopen(req) as response:
            res_data = json.loads(response.read().decode())
            print(f"Successfully uploaded {local_path} to {owner}/{repo}:{repo_path} (SHA: {res_data['content']['sha'][:7]})")
            return True
    except urllib.error.HTTPError as e:
        print(f"Failed to upload {local_path}: HTTP {e.code} - {e.reason}")
        # Print detailed error if available
        try:
            error_details = json.loads(e.read().decode())
            print(f"Details: {error_details.get('message')}")
        except Exception:
            pass
        return False

def main():
    print("=== GitHub API File Uploader ===")
    
    # Try to get credentials from environment variables if present
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        token = input("Enter your GitHub Personal Access Token (PAT): ").strip()
        if not token:
            print("Token is required.")
            sys.exit(1)
            
    repo_input = os.environ.get("GITHUB_REPO")
    if not repo_input:
        repo_input = input("Enter repository (format: owner/repo, e.g., AuraFlight/container-hci): ").strip()
        if not repo_input or "/" not in repo_input:
            print("Repository format must be 'owner/repo'.")
            sys.exit(1)
            
    owner, repo = repo_input.split("/", 1)
    
    files_to_upload = [
        ("docs/vali.md", "docs/vali.md"),
        ("deploy_updates.py", "deploy_updates.py")
    ]
    
    commit_msg = input("Enter commit message [Default: 'docs: update VM hardware standards']: ").strip()
    if not commit_msg:
        commit_msg = "docs: update VM hardware standards"
        
    success = True
    for local_file, repo_file in files_to_upload:
        print(f"Uploading {local_file}...")
        if not upload_file_to_github(owner, repo, local_file, repo_file, token, commit_msg):
            success = False
            
    if success:
        print("\nAll files uploaded successfully!")
    else:
        print("\nSome files failed to upload.")

if __name__ == "__main__":
    main()
