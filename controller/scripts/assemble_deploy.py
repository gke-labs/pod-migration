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

    found = False
    for i, doc in enumerate(documents):
        if re.search(r'^[ \t]*kind:[ \t]*ClusterRole[ \t]*$', doc, re.MULTILINE) and not re.search(r'^[ \t]*kind:[ \t]*ClusterRoleBinding[ \t]*$', doc, re.MULTILINE):
            documents[i] = role_content + '\n'
            found = True
        elif 'kind: ValidatingWebhookConfiguration' in doc or 'kind: MutatingWebhookConfiguration' in doc:
            pattern = r'(    namespaceSelector:\n      matchExpressions:\n        - key: kubernetes\.io/metadata\.name\n          operator: NotIn\n          values:\n(?:            - [^\n]+\n)+)'
            formatted = format_namespace_selector(indent=4) + '\n'
            documents[i] = re.sub(pattern, formatted, doc)

    if not found:
        print("Error: ClusterRole section not found in deploy.yaml", file=sys.stderr)
        sys.exit(1)

    new_deploy_content = '---\n'.join(documents)
    with open('deploy.yaml', 'w') as f:
        f.write(new_deploy_content)

    print("Successfully assembled deploy.yaml with config/rbac/role.yaml and webhook namespace selectors")

if __name__ == '__main__':
    main()
