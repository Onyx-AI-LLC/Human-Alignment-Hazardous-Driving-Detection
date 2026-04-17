# Contributing to HAHD

Thank you for your interest in contributing to the Human-Aligned Hazardous Driving (HAHD) project. This document outlines the process for contributing code, documentation, and research.

## Getting Started

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/Human-Alignment-Hazardous-Driving-Detection.git
   cd Human-Alignment-Hazardous-Driving-Detection
   ```
3. Add the upstream remote:
   ```bash
   git remote add upstream https://github.com/Onyx-AI-LLC/Human-Alignment-Hazardous-Driving-Detection.git
   ```

## Branch Naming

Use the following prefixes:

| Prefix | Use case |
|--------|----------|
| `feature/` | New features or functionality |
| `bugfix/` | Bug fixes |
| `chore/` | Maintenance, dependencies, tooling |
| `docs/` | Documentation only changes |
| `research/` | Research experiments or analysis |

Example: `feature/add-gaze-heatmap-visualization`

## Development Workflow

1. Sync your fork with upstream before starting:
   ```bash
   git fetch upstream
   git checkout main
   git merge upstream/main
   ```
2. Create a new branch from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. Make your changes and commit with a clear message:
   ```bash
   git commit -m "feat: add gaze heatmap visualization to EDA pipeline"
   ```
4. Push to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```
5. Open a Pull Request against `main` in the upstream repo

## Pull Request Guidelines

- Keep PRs focused — one concern per PR
- Include a clear description of what changed and why
- Reference any related issues with `Closes #123`
- Ensure your branch is up to date with `main` before requesting review
- All PRs require at least one approving review before merge

## Coding Standards

**Python:**
- Follow PEP 8
- Use descriptive variable names
- Keep functions small and focused

**JavaScript / TypeScript:**
- Follow the existing ESLint configuration
- Use TypeScript types where applicable
- Avoid `any` types

**Environment Variables:**
- Never hardcode credentials or secrets
- Document any new environment variables in the relevant README

## Reporting Issues

Open an issue on GitHub with:
- A clear title and description
- Steps to reproduce (for bugs)
- Expected vs. actual behavior
- Your environment (OS, Python version, Node version)

## Questions

For questions about the research or project direction, open a GitHub Discussion or reach out via the contact information in the main README.
