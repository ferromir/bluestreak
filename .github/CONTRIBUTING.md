# Contributing to Bluestreak

Thank you for your interest in contributing to Bluestreak! This document provides guidelines and instructions for contributing.

## Development Setup

1. Fork and clone the repository
2. Install dependencies:

   ```bash
   npm install
   ```

3. Run tests:
   ```bash
   npm test
   ```

## Pull Request Process

1. **Before submitting a PR**, ensure all checks pass locally:

   ```bash
   npm run format:check  # Check code formatting
   npm run test:coverage # Run tests with coverage
   ```

2. **Code Quality Requirements**:
   - All tests must pass
   - Code coverage must be at least 80% for all metrics (statements, branches, functions, lines)
   - Code must be formatted with Prettier

3. **Formatting**:
   - Run `npm run format` to automatically format your code before committing
   - The CI pipeline will fail if code is not properly formatted

4. **Testing**:
   - Add tests for any new functionality
   - Update tests for any modified functionality
   - Ensure coverage thresholds are maintained

## CI/CD Pipeline

Our GitHub Actions pipeline automatically:

- ✅ Checks code formatting with Prettier
- ✅ Runs tests with coverage on Node.js 18.x, 20.x, and 22.x
- ✅ Enforces 80% minimum coverage threshold
- ✅ Generates and commits coverage badges

All checks must pass before a PR can be merged.

## Code Style

- We use Prettier for code formatting
- ES modules (`import`/`export`) syntax
- Private class fields for encapsulation (`#field`)
- JSDoc comments for all public APIs

## Commit Messages

- Use clear, descriptive commit messages
- Start with a verb in the imperative mood (e.g., "Add", "Fix", "Update")
- Reference issues when applicable (e.g., "Fix #123")

## Questions?

Feel free to open an issue for any questions or concerns!
