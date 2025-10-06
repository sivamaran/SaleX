import asyncio
import json
import time
from datetime import datetime
from web.crl import run_web_crawler_async, get_mongodb_manager

async def test_crawler_simple(icp_data: str, icp_identifier: str = None):
    """
    Simple test function for the web crawler
    
    Args:
        icp_data: Path to ICP JSON file
        icp_identifier: Campaign identifier (auto-generated if None)
    
    Returns:
        Dict with test results
    """
    
    print(f"Testing web crawler with ICP file: {icp_data}")
    
    # # Load ICP data
    # try:
    #     with open(icp_json_file, 'r', encoding='utf-8') as f:
    #         icp_data = json.load(f)
    # except Exception as e:
    #     print(f"Error loading ICP file: {e}")
    #     return {"success": False, "error": f"Failed to load ICP file: {e}"}

    # Generate identifier if not provided
    if not icp_identifier:
        product_name = icp_data.get("product_details", {}).get("product_name", "test")
        timestamp = int(time.time())
        icp_identifier = f"{product_name.lower().replace(' ', '_')}_{timestamp}"
    
    print(f"Campaign ID: {icp_identifier}")
    print(f"Product: {icp_data.get('product_details', {}).get('product_name', 'Unknown')}")
    
    # Run crawler
    start_time = time.time()
    result = await run_web_crawler_async(icp_data, icp_identifier)
    end_time = time.time()
    
    # Check results
    if result["success"]:
        leads_found = result["summary"]["total_leads_found"]
        leads_stored = result["summary"]["leads_stored"]
        duplicates = result["summary"]["duplicates_found"]
        
        print(f"SUCCESS: Found {leads_found} leads, stored {leads_stored}, filtered {duplicates} duplicates")
        print(f"Execution time: {end_time - start_time:.2f} seconds")
        
        # Verify in database
        try:
            mongodb_manager = get_mongodb_manager()
            db_count = mongodb_manager.get_collection('unified_leads').count_documents({
                "icp_identifier": icp_identifier
            })
            print(f"Verified {db_count} leads in database")
        except Exception as e:
            print(f"Database verification failed: {e}")
        
        return {
            "success": True,
            "leads_found": leads_found,
            "leads_stored": leads_stored,
            "duplicates": duplicates,
            "execution_time": end_time - start_time,
            "icp_identifier": icp_identifier
        }
    
    else:
        print(f"FAILED: {result.get('error', 'Unknown error')}")
        return {
            "success": False,
            "error": result.get('error', 'Unknown error'),
            "icp_identifier": icp_identifier
        }

def test_crawler(icp_json_file: str, icp_identifier: str = None):
    """Synchronous wrapper for the test function"""
    
    return asyncio.run(test_crawler_simple(icp_json_file, icp_identifier))

if __name__ == "__main__":
    import sys
    
    icp_file = {
            "product_details": {
                "product_name": "Premium Bus Travel & Group Tour Services",
                "product_category": "Travel & Tourism/Transportation Services",
                "usps": [
                    "Luxury bus fleet with premium amenities",
                    "Custom corporate group travel packages",
                    "Exclusive high-end travel experiences",
                    "Professional tour planning and coordination",
                    "Cost-effective group travel solutions",
                    "24/7 customer support during travel"
                ],
                "pain_points_solved": [
                    "Complicated group travel logistics",
                    "Expensive individual travel arrangements",
                    "Lack of customized corporate travel options",
                    "Poor coordination for large group events",
                    "Safety concerns in group transportation",
                    "Time-consuming travel planning process"
                ]
            },
            "icp_information": {
                "target_industry": [
                    "Corporate Companies",
                    "Educational Institutions",
                    "Wedding Planners",
                    "Event Management",
                    "Religious Organizations",
                    "Sports Teams/Clubs",
                    "Family Reunion Organizers",
                    "Travel Influencers"
                ],
                "competitor_companies": [
                    "RedBus",
                    "MakeMyTrip",
                    "Yatra",
                    "Local tour operators",
                    "Private bus operators",
                    "Luxury Bus Company", 
                    "Premium Tour Operator", 
                    "Corporate Travel Agency"
                ],
                "company_size": "10-1000+ employees/members",
                "decision_maker_persona": [
                    "HR Manager",
                    "Event Coordinator",
                    "Travel Manager",
                    "Family Head/Organizer",
                    "Wedding Planner",
                    "School/College Administrator",
                    "Corporate Executive",
                    "Travel Influencer",
                    "Religious Leader/Organizer"
                ],
                "region": ["India", "Major Cities", "Tourist Destinations"],
                "budget_range": "$5,000-$50,000 annually",
                "occasions": [
                    "Corporate offsites",
                    "Wedding functions",
                    "Family vacations",
                    "Educational tours",
                    "Religious pilgrimages",
                    "Adventure trips",
                    "Destination weddings",
                    "Sports events"
                ]
            }
        }
    
    icp_id = "sample_crl"
    result = test_crawler(icp_file, icp_id)
    
    if result["success"]:
        print(f"\nTest completed successfully!")
        print(f"Campaign: {result['icp_identifier']}")
        print(f"Results: {result['leads_stored']} leads stored")
    else:
        print(f"\nTest failed: {result['error']}")