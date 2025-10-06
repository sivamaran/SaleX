# Contact Scraper Integration

## Overview
The contact scraper has been successfully integrated into the lead generation pipeline as the final step to enhance leads with contact information (emails and phone numbers).

## Integration Points

### 1. Main Orchestration Flow (`main.py`)
- **Step 7**: Contact enhancement is now the 7th step in the orchestration
- **Location**: After lead filtering and before final report generation
- **Function**: `run_optimized_contact_scraper(limit=0, batch_size=20)`
- **Results**: Enhanced leads are stored back to the `unified_leads` collection

### 2. API Endpoints (`app.py`)
- **Main Pipeline**: Contact enhancement is included in the complete pipeline (`/api/lead-generation/run`)
- **Standalone Endpoint**: New endpoint `/api/contact-enhancement/run` for running contact enhancement independently
- **Parameters**: 
  - `limit`: Number of leads to process (0 = all)
  - `batch_size`: Batch size for processing (default: 20)

### 3. Database Integration (`database/mongodb_manager.py`)
- **New Methods Added**:
  - `get_unified_leads_without_contacts(limit=0)`: Gets leads missing contact information
  - `bulk_update_unified_leads(operations)`: Performs bulk updates for contact data

## How It Works

### 1. Lead Identification
The contact scraper identifies leads that need contact enhancement by querying for leads that:
- Don't have a `contact` field
- Have empty `contact.emails` array
- Have empty `contact.phone_numbers` array

### 2. Contact Extraction
For each lead, the scraper:
- Determines the platform (Instagram, LinkedIn, etc.)
- Uses appropriate extraction methods:
  - **Instagram**: Scrapes profile data and extracts phone numbers from biography
  - **LinkedIn**: Extracts company domain and searches for emails
  - **General**: Uses Hunter.io API for domain-based email discovery

### 3. Data Enhancement
- Emails and phone numbers are extracted and validated
- Contact information is added to the `contact` field in the unified leads collection
- Results are processed in batches for efficiency

## API Usage

### Complete Pipeline
```bash
POST /api/lead-generation/run
{
    "icp_data": {...},
    "selected_scrapers": ["web_scraper", "instagram", "linkedin"]
}
```

### Standalone Contact Enhancement
```bash
POST /api/contact-enhancement/run
{
    "limit": 0,
    "batch_size": 20
}
```

## Configuration

### Environment Variables
- `HUNTER_API_KEY`: Required for Hunter.io email discovery
- `APIFY_TOKEN`: Required for Instagram profile scraping

### Performance Settings
- **Batch Size**: Default 20 leads per batch
- **Rate Limiting**: Built-in delays to avoid API limits
- **Concurrent Processing**: Async processing with semaphore limits

## Results Structure

### Contact Enhancement Results
```json
{
    "enhanced_leads": 15,
    "leads_with_emails": 8,
    "leads_with_phones": 5,
    "enhancement_data": [
        {
            "lead_id": "ObjectId",
            "emails": ["email@example.com"],
            "phone_numbers": ["+1234567890"],
            "platform": "instagram"
        }
    ]
}
```

## Error Handling
- Graceful handling of API failures
- Individual lead processing errors don't stop the batch
- Comprehensive logging for debugging
- Fallback mechanisms for different platforms

## Testing
Run the integration test:
```bash
python test_contact_integration.py
```

This will:
1. Test MongoDB connection
2. Check for leads without contact info
3. Run contact scraper on a small batch
4. Verify results

## Benefits
1. **Complete Lead Data**: All leads now have contact information when available
2. **Efficient Processing**: Batch processing with rate limiting
3. **Platform Agnostic**: Works with Instagram, LinkedIn, and general web leads
4. **API Integration**: Seamlessly integrated into existing API endpoints
5. **Scalable**: Handles large volumes of leads efficiently

## Next Steps
1. Monitor performance and adjust batch sizes as needed
2. Add more contact extraction methods for different platforms
3. Implement contact validation and verification
4. Add metrics and monitoring for contact enhancement success rates
