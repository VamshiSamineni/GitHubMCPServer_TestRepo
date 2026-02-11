# Contributing Guidelines

Thank you for your interest in contributing to the GitHubMCPServer_TestRepo project! This document provides guidelines and standards for contributing to this repository.

## Code of Conduct

- Be respectful and inclusive in all interactions
- Focus on constructive feedback
- Help maintain a positive and welcoming environment

## How to Contribute

### Reporting Issues

When reporting issues, please include:
- A clear and descriptive title
- Steps to reproduce the issue
- Expected vs. actual behavior
- Any relevant data samples or error messages

### Making Changes

1. **Fork the Repository**
   ```bash
   git fork https://github.com/VamshiSamineni/GitHubMCPServer_TestRepo.git
   ```

2. **Create a Feature Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make Your Changes**
   - Follow the documentation standards outlined below
   - Ensure data file integrity is maintained
   - Update documentation as needed

4. **Commit Your Changes**
   ```bash
   git commit -m "Brief description of changes"
   ```

5. **Push to Your Fork**
   ```bash
   git push origin feature/your-feature-name
   ```

6. **Submit a Pull Request**
   - Provide a clear description of the changes
   - Reference any related issues
   - Ensure all changes are documented

## Documentation Standards

### README.md Structure

The README should maintain the following structure:
1. **Title and Brief Description**: Clear project title and one-line description
2. **Overview**: Detailed purpose and context
3. **Repository Contents**: Organized list of all files with descriptions
4. **Purpose**: Explanation of repository goals and use cases
5. **Getting Started**: Prerequisites and setup instructions
6. **Contributing**: Link to contribution guidelines
7. **Additional Sections**: As needed (License, Contact, etc.)

### File Documentation

When adding or modifying files:
- Update README.md with file descriptions
- Include file format, encoding, and purpose
- Document any special handling requirements
- Maintain consistent formatting

### Commit Message Guidelines

Follow these conventions for commit messages:
- Use present tense ("Add feature" not "Added feature")
- Use imperative mood ("Move file to..." not "Moves file to...")
- Keep first line under 50 characters
- Add detailed description after blank line if needed

Examples:
- `Add ClarityData.csv with analytics information`
- `Update README with data file descriptions`
- `Fix formatting in CONTRIBUTING.md`

## Data File Standards

### CSV Files

- **Encoding**: Maintain UTF-8 encoding
- **Line Endings**: Use CRLF for consistency
- **Headers**: First row should contain column headers
- **Format**: Ensure valid CSV structure

### JSON Files

- **Format**: Use proper JSON syntax
- **Indentation**: Use 2 or 4 spaces consistently
- **Validation**: Ensure JSON is valid before committing

### Document Files

- **Formats**: Support .docx, .pdf, and plain text formats
- **Naming**: Use descriptive names without special characters
- **Size**: Keep file sizes reasonable (<10MB when possible)

## Pull Request Process

1. **Before Submitting**:
   - Ensure your branch is up to date with main
   - Test your changes locally
   - Update all relevant documentation
   - Check for data integrity

2. **PR Description Should Include**:
   - Summary of changes
   - Motivation and context
   - Type of change (bug fix, feature, documentation)
   - Related issue numbers

3. **Review Process**:
   - At least one approval required
   - Address all review comments
   - Keep PR scope focused and manageable

## File Naming Conventions

- Use descriptive, kebab-case names: `user-analytics-data.csv`
- Avoid spaces in filenames
- Include version numbers when applicable: `report-v2.csv`
- Use standard file extensions

## Testing

When contributing code or scripts:
- Include test cases when applicable
- Ensure existing tests still pass
- Document testing procedures

## Questions?

If you have questions about contributing:
- Open an issue for discussion
- Review existing issues and pull requests
- Contact repository maintainers

Thank you for contributing to this project!