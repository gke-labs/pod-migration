#!/usr/bin/env python3
import os
import sys

def main():
    if not os.path.exists('deploy.yaml'):
        print("Error: Must be run from the controller directory (deploy.yaml not found)", file=sys.stderr)
        sys.exit(1)

    with open('deploy.yaml', 'r') as f:
        deploy_content = f.read()

    documents = deploy_content.split('---\n')
    role_path = 'config/rbac/role.yaml'
    if not os.path.exists(role_path):
        print(f"Error: {role_path} not found. Run 'make manifests' first.", file=sys.stderr)
        sys.exit(1)

    with open(role_path, 'r') as f:
        role_content = f.read()

    role_content = role_content.strip()
    if role_content.startswith('---'):
        role_content = role_content[3:].strip()

    found = False
    for i, doc in enumerate(documents):
        if 'kind: ClusterRole\n' in doc or doc.startswith('kind: ClusterRole\n'):
            documents[i] = role_content + '\n'
            found = True
            break

    if not found:
        print("Error: ClusterRole section not found in deploy.yaml", file=sys.stderr)
        sys.exit(1)

    new_deploy_content = '---\n'.join(documents)
    with open('deploy.yaml', 'w') as f:
        f.write(new_deploy_content)

    print("Successfully assembled deploy.yaml with config/rbac/role.yaml")

if __name__ == '__main__':
    main()
