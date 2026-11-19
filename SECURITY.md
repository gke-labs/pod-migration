# Security Policy

## Reporting Security Vulnerabilities

If you believe you have discovered a vulnerability in this project, please report it privately. **Do not create public GitHub issues or discussions for security vulnerabilities.**

To report a vulnerability:

- Please report vulnerabilities to Google's Open Source Security team through the [Google Vulnerability Reward Program (VRP)](https://bughunter.withgoogle.com/) or via email to [security@google.com](mailto:security@google.com).
- You may also submit a draft private advisory through GitHub Security Advisories on this repository: [Security Advisories](https://github.com/gke-labs/pod-migration/security/advisories/new).

Please include:

- A descriptive title and summary of the suspected vulnerability.
- Affected components (controller, admission webhooks, CRIU engine agent, holder wrapper).
- Steps to reproduce or proof-of-concept exploit, including Kubernetes/GKE versions, node OS, and container runtime.
- Assessment of impact (e.g. privilege escalation, host boundary compromise, unauthorized container restore).

We will acknowledge receipt of your report within 48 hours and coordinate triage, patch development, and responsible disclosure.

## Supported Versions

| Version        | Supported          |
| -------------- | ------------------ |
| `main` branch  | :white_check_mark: |
| v0.1.x (alpha) | :white_check_mark: |

## Security & Isolation Model

For detailed documentation on the CRIU engine's security posture, threat model, node agent privileges, and restore path isolation, see [Threat Model and Security Guidance for the CRIU Engine](docs/threat-model-criu.md).
