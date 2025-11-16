# Workstation Setup

Automated setup script for DevOps engineering workstation with essential tools and utilities.

## Overview

The `workstation.sh` script automates the installation of a comprehensive DevOps toolkit, including development environments, cloud tools, container platforms, and productivity applications.

## Prerequisites

- Ubuntu/Debian-based Linux distribution
- sudo privileges
- Internet connection for package downloads
- At least 2GB free disk space for all installations

## Installation

```bash
# Make the script executable
chmod +x workstation.sh

# Run the setup script
./workstation.sh
```

**⚠️ Important**: This script performs system-wide installations and modifies system configurations. Review the script contents before execution.

## Installed Tools

### Development Environment
- **Node.js v24.11.1** via nvm (Node Version Manager)
- **Python 3** with virtual environment support
- **Git** version control system

### Container & Infrastructure
- **Docker** with Docker Compose
- **Terraform** for Infrastructure as Code

### Cloud & DevOps Tools
- **AWS CLI** for Amazon Web Services
- **AWS SAM CLI** for Serverless Application Model
- **k6** for load testing

### Networking & Remote Access
- **nmap** network scanner
- **remmina** remote desktop client
- **L2TP VPN** client support
- **WinBox** Mikrotik router management

### Productivity & Utilities
- **vim** text editor
- **solaar** Logitech Unifying Receiver management
- **gnome-shell-extension-gsconnect** KDE Connect integration
- **indicator-multiload** system monitor
- **Windsurf** code editor

## Modifying Versions
- Change version numbers in respective installation sections
- Update verification commands accordingly
- Test compatibility with other tools

## System Requirements

- **OS**: Ubuntu 20.04+ / Debian 10+
- **RAM**: Minimum 4GB (8GB recommended)
- **Storage**: 2GB free space minimum
- **Network**: Stable internet connection

## Security Considerations

- Script installs packages from official repositories when possible
- Some tools require third-party repositories (reviewed for security)
- Docker group membership grants container management privileges
- AWS CLI requires credential configuration (not automated)

## Support

For tool-specific issues:
- Docker: [Docker Documentation](https://docs.docker.com/)
- Node.js: [Node.js Docs](https://nodejs.org/docs/)
- Terraform: [Terraform Docs](https://www.terraform.io/docs)
- AWS: [AWS CLI Documentation](https://docs.aws.amazon.com/cli/)

For script issues:
1. Check individual tool installation logs
2. Verify system requirements
3. Test installations manually

---

**Last Updated**: Script installs latest stable versions as of creation date. Verify versions for critical production deployments.
