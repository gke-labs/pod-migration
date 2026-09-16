#!/usr/bin/env python3
import os
import re
import sys

# Centralized single source of truth for namespaces excluded from admission webhooks.
EXCLUDED_NAMESPACES = [
    "pod-migration-system",
    "kube-system",
]

def format_namespace_selector(indent=4):
    pad = " " * indent
    lines = [
        f"{pad}namespaceSelector:",
        f"{pad}  matchExpressions:",
        f"{pad}    - key: kubernetes.io/metadata.name",
        f"{pad}      operator: NotIn",
        f"{pad}      values:",
    ]
    for ns in EXCLUDED_NAMESPACES:
        lines.append(f"{pad}        - {ns}")
    return "\n".join(lines)

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

    clusterrole_sub_count = 0
    webhook_sub_count = 0
    for i, doc in enumerate(documents):
        if re.search(r'^kind: ClusterRole\s*$', doc, re.MULTILINE) and re.search(r'^\s*name:\s*pod-migration-controller-role\s*$', doc, re.MULTILINE):
            documents[i] = role_content + '\n'
            clusterrole_sub_count += 1
        elif 'kind: ValidatingWebhookConfiguration' in doc or 'kind: MutatingWebhookConfiguration' in doc:
            pattern = r'(    namespaceSelector:\n      matchExpressions:\n        - key: kubernetes\.io/metadata\.name\n          operator: NotIn\n          values:\n(?:            - [^\n]+\n)+)'
            formatted = format_namespace_selector(indent=4) + '\n'
            new_doc, count = re.subn(pattern, formatted, doc)
            if count == 0:
                print(f"Error: namespaceSelector replacement failed in document {i}", file=sys.stderr)
                sys.exit(1)
            webhook_sub_count += count
            documents[i] = new_doc

    if clusterrole_sub_count != 1:
        print(f"Error: Expected exactly 1 ClusterRole (name: pod-migration-controller-role) replacement, got {clusterrole_sub_count}", file=sys.stderr)
        sys.exit(1)

    if webhook_sub_count != 3:
        print(f"Error: Expected 3 webhook namespaceSelector replacements, got {webhook_sub_count}", file=sys.stderr)
        sys.exit(1)

    new_deploy_content = '---\n'.join(documents)
    with open('deploy.yaml', 'w') as f:
        f.write(new_deploy_content)

    print("Successfully assembled deploy.yaml with config/rbac/role.yaml and webhook namespace selectors")

if __name__ == '__main__':
    main()
