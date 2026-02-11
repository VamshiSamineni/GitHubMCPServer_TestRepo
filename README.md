# GitHubMCPServer_TestRepo

## Overview

This is a test repository for GitHub MCP Server containing sample data files and documentation related to Microsoft AI Skills Navigator and related services.

## Repository Contents

This repository contains the following files:

### Data Files

1. **ClarityData.csv** (708 KB, 4,421 rows)
   - Contains URL data related to Microsoft AI Skills Navigator
   - Includes various tracking parameters and redirect URLs
   - Primary data source for web analytics

2. **export.csv** (608 KB, 3,736 rows)
   - Export data with the following columns:
     - `Url`: Web URLs for various Microsoft services
     - `LandingPageFlag`: Indicates if the URL is a landing page
     - `Sum of DistinctUserCount`: User count metrics
   - Covers data from services including:
     - projectorono.microsoft.com
     - aiskillsnavigator.microsoft.com

3. **Certification.bim** (388 KB)
   - Power BI Model file (BIM format)
   - Contains data model definition for "Certification_Partner"
   - Includes:
     - Data source configurations (SharePoint lists)
     - Table definitions (Account, Segment, etc.)
     - Column metadata and relationships
   - Compatibility Level: 1567
   - Connected to SharePoint: `https://microsoft.sharepoint.com/teams/WWLReporting`

4. **Assignment (1).docx** (784 KB)
   - Microsoft Word document
   - Contains assignment or project documentation

## File Structure

```
.
├── README.md                    # This file
├── ClarityData.csv             # Web analytics URL data
├── export.csv                  # User analytics export data
├── Certification.bim           # Power BI data model
└── Assignment (1).docx         # Project documentation
```

## Purpose

This repository serves as a test environment for:
- Testing GitHub MCP Server functionality
- Storing sample data files for demonstration purposes
- Version controlling data models and analytics exports

## Technologies

- **Data Formats**: CSV (Comma-Separated Values), JSON
- **Business Intelligence**: Power BI (BIM model files)
- **Documentation**: Microsoft Word (DOCX)

## Data Sources

The data in this repository relates to:
- Microsoft AI Skills Navigator (aiskillsnavigator.microsoft.com)
- Project Orono (projectorono.microsoft.com)
- Microsoft SharePoint (teams/WWLReporting)

---

*Note: This is a test repository. The data contained herein is for demonstration and testing purposes.*