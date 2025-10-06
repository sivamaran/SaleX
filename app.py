#!/usr/bin/env python3
"""
Flask API for Lead Generation Backend
Provides essential endpoints for the complete lead generation pipeline.
"""

import asyncio
import json
import os
import sys
from datetime import datetime
from typing import List, Dict, Any, Optional
from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.exceptions import BadRequest
import logging

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed, continue without it

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the orchestrator from main.py
from main import LeadGenerationOrchestrator
from filter_web_lead import MongoDBLeadProcessor
from contact_scraper import run_optimized_contact_scraper
# from web.crl import run_web_crawler_async  # Commented out - crl.py removed from flow
from web_url_scraper.database_service import (
    get_unprocessed_urls_by_type, 
    mark_urls_as_processed, 
    get_available_url_counts,
    get_urls_by_type_and_icp
)

# Scraper registry
from scraper_registry import (
    get_scrapers_info,
    is_valid_scraper,
    get_url_type_map,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Global orchestrator instance
orchestrator = None

def get_orchestrator():
    """Get or create orchestrator instance"""
    global orchestrator
    if orchestrator is None:
        orchestrator = LeadGenerationOrchestrator()
    return orchestrator

def run_async(coro):
    """Helper function to run async code in Flask"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "Lead Generation Backend"
    })

@app.route('/api/scrapers', methods=['GET'])
def get_available_scrapers():
    """Get list of available scrapers"""
    try:
        orch = get_orchestrator()
        return jsonify({
            "success": True,
            "data": {
                "available_scrapers": list(orch.available_scrapers.keys()),
                "scrapers_info": get_scrapers_info()
            }
        })
    except Exception as e:
        logger.error(f"Error getting scrapers: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/icp/template', methods=['GET'])
def get_icp_template():
    """Get ICP (Ideal Customer Profile) template"""
    try:
        orch = get_orchestrator()
        template = orch.get_hardcoded_icp()
        
        return jsonify({
            "success": True,
            "data": {
                "icp_template": template,
                "description": "Use this template to structure your ICP data"
            }
        })
    except Exception as e:
        logger.error(f"Error getting ICP template: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/lead-generation/run', methods=['POST'])
def run_lead_generation():
    """
    Main endpoint to run the complete lead generation pipeline
    
    Expected payload:
    {
        "icp_data": {
            "product_details": {...},
            "icp_information": {...}
        },
        "selected_scrapers": ["web_scraper", "instagram", "linkedin", "youtube", "facebook"]  # company_directory commented out
    }
    """
    try:
        # Validate request
        if not request.is_json:
            raise BadRequest("Request must be JSON")
        
        data = request.get_json()
        
        # Extract and validate required fields
        icp_data = data.get('icp_data')
        selected_scrapers = data.get('selected_scrapers', ['web_scraper'])
        
        if not icp_data:
            return jsonify({
                "success": False,
                "error": "icp_data is required"
            }), 400
        
        if not isinstance(selected_scrapers, list) or not selected_scrapers:
            return jsonify({
                "success": False,
                "error": "selected_scrapers must be a non-empty list"
            }), 400
        
        logger.info(f"Starting lead generation pipeline with scrapers: {selected_scrapers}")
        
        # Get orchestrator instance
        orch = get_orchestrator()
        
        # Generate ICP identifier
        icp_identifier = orch.generate_icp_identifier(icp_data)
        logger.info(f"🏷️ Generated ICP identifier: {icp_identifier}")
        
        # Run the complete pipeline asynchronously
        result = run_async(run_pipeline_async(orch, icp_data, selected_scrapers, icp_identifier))
        
        return jsonify({
            "success": True,
            "data": result,
            "icp_identifier": icp_identifier,
            "message": "Lead generation pipeline completed successfully"
        })
        
    except BadRequest as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400
    except Exception as e:
        logger.error(f"Error in lead generation pipeline: {e}")
        return jsonify({
            "success": False,
            "error": f"Pipeline failed: {str(e)}"
        }), 500

async def run_pipeline_async(orch, icp_data, selected_scrapers, icp_identifier, platform_override: str = None):
    """
    Run the complete lead generation pipeline asynchronously
    """
    pipeline_start = datetime.now()
    
    try:
        # Step 1: Ensure scraped_urls collection exists
        logger.info("🗄️ Step 1: Ensuring scraped_urls collection exists...")
        try:
            from web_url_scraper.database_service import ensure_collection_exists
            ensure_collection_exists()
            logger.info("✅ scraped_urls collection ready")
        except Exception as e:
            logger.error(f"❌ Failed to ensure scraped_urls collection exists: {e}")
        
        # Step 2: Generate search queries with Gemini AI
        logger.info("🤖 Step 2: Generating search queries...")
        if platform_override and len(selected_scrapers) == 1:
            # Generate platform-specific queries only
            queries = await orch.generate_platform_queries(icp_data, platform_override)
        else:
            queries = await orch.generate_search_queries(icp_data, selected_scrapers)
        
        if not queries:
            raise Exception("No search queries were generated")
        
        logger.info(f"✅ Generated {len(queries)} search queries")
        
        # Step 3: Collect URLs using web_url_scraper
        logger.info("🔍 Step 3: Collecting URLs...")
        classified_urls = await orch.collect_urls_from_queries(queries, icp_identifier)
        
        total_urls = sum(len(urls) for urls in classified_urls.values())
        if total_urls == 0:
            raise Exception("No URLs were collected from the search queries")
        
        logger.info(f"✅ Collected {total_urls} URLs")
        
        # Step 4: Run selected scrapers
        logger.info("🚀 Step 4: Running scrapers...")
        scraper_results = await orch.run_selected_scrapers(classified_urls, selected_scrapers, icp_identifier, icp_data)
        
        # Add web crawler results to scraper results
        # COMMENTED OUT - crl.py removed from flow
        # if web_crawler_results:
        #     scraper_results['web_crawler'] = web_crawler_results
        
        # Step 5: Filter and process leads using MongoDBLeadProcessor
        # logger.info("🧹 Step 5: Filtering and processing leads...")
        # lead_filtering_results = {}
        # try:
        #     lead_processor = MongoDBLeadProcessor()
            
        #     # Create indexes for the target collection
        #     lead_processor.create_indexes()
            
        #     # Process all leads from web_leads collection to leadgen_leads collection
        #     filtering_results = lead_processor.process_leads(batch_size=50)
            
        #     # Get processing statistics
        #     processing_stats = lead_processor.get_processing_stats()
            
        #     lead_filtering_results = {
        #         'filtering_stats': filtering_results,
        #         'processing_stats': processing_stats
        #     }
            
        #     lead_processor.close_connection()
            
        # except Exception as e:
        #     logger.error(f"❌ Error in lead filtering: {e}")
        #     lead_filtering_results = {'error': str(e)}
        
        # # Add filtering results to scraper results
        # scraper_results['lead_filtering'] = lead_filtering_results

        # Step 6: Enhance leads with contact information using contact scraper
        logger.info("📞 Step 6: Enhancing leads with contact information...")
        contact_enhancement_results = {}
        try:
            contact_enhancement_data = await run_optimized_contact_scraper(
                limit=0,  # Process all leads without contact info
                batch_size=20
            )
            
            # Count leads with emails and phone numbers
            leads_with_emails = sum(1 for lead in contact_enhancement_data if lead.get('emails'))
            leads_with_phones = sum(1 for lead in contact_enhancement_data if lead.get('phone_numbers'))
            
            contact_enhancement_results = {
                'enhanced_leads': len(contact_enhancement_data),
                'leads_with_emails': leads_with_emails,
                'leads_with_phones': leads_with_phones,
                'enhancement_data': contact_enhancement_data
            }
            
        except Exception as e:
            logger.error(f"❌ Error in contact enhancement: {e}")
            contact_enhancement_results = {'error': str(e)}
        
        # Add contact enhancement results to scraper results
        scraper_results['contact_enhancement'] = contact_enhancement_results
        
        # Step 7: Generate final report
        logger.info("📊 Step 7: Generating final report...")
        report_file = orch.generate_final_report(icp_data, selected_scrapers, scraper_results)
        
        pipeline_end = datetime.now()
        execution_time = (pipeline_end - pipeline_start).total_seconds()
        
        # Prepare response
        response_data = {
            "pipeline_metadata": {
                "execution_time_seconds": execution_time,
                "start_time": pipeline_start.isoformat(),
                "end_time": pipeline_end.isoformat(),
                "selected_scrapers": selected_scrapers,
                "icp_identifier": icp_identifier,
                "total_queries_generated": len(queries),
                "total_urls_collected": total_urls
            },
            "url_collection": {
                "classified_urls_count": {k: len(v) for k, v in classified_urls.items()},
                "total_urls": total_urls
            },
            "scraper_results_summary": {},
            "report_file": report_file,
            "queries_used": queries
        }
        
        # Generate summary for each scraper
        successful_scrapers = 0
        for scraper, result in scraper_results.items():
            # COMMENTED OUT - crl.py removed from flow
            # if scraper == 'web_crawler':
            #     # Handle web crawler results separately
            #     if result.get('success'):
            #         successful_scrapers += 1
            #         summary = result.get('summary', {})
            #         response_data["scraper_results_summary"][scraper] = {
            #             "status": "success",
            #             "leads_found": summary.get('total_leads_found', 0),
            #             "leads_stored": summary.get('leads_stored', 0),
            #             "duplicates_found": summary.get('duplicates_found', 0),
            #             "urls_crawled": summary.get('urls_crawled', 0),
            #             "execution_time_seconds": summary.get('execution_time_seconds', 0)
            #         }
            #     else:
            #         response_data["scraper_results_summary"][scraper] = {
            #             "status": "failed",
            #             "error": result.get('error', 'Unknown error')
            #         }
            # if scraper == 'lead_filtering':
            #     # Handle lead filtering results separately
            #     if result.get('error'):
            #         response_data["scraper_results_summary"][scraper] = {
            #             "status": "failed", 
            #             "error": result['error']
            #         }
            #     else:
            #         successful_scrapers += 1
            #         filtering_stats = result.get('filtering_stats', {})
            #         response_data["scraper_results_summary"][scraper] = {
            #             "status": "success",
            #             "leads_processed": filtering_stats.get('total', 0),
            #             "leads_filtered": filtering_stats.get('filtered', 0),
            #             "leads_extracted": filtering_stats.get('extracted', 0),
            #             "leads_inserted": filtering_stats.get('inserted', 0),
            #             "email_based_leads": filtering_stats.get('email_based', 0),
            #             "phone_based_leads": filtering_stats.get('phone_based', 0)
            #         }
            if scraper == 'contact_enhancement':
                # Handle contact enhancement results separately
                if result.get('error'):
                    response_data["scraper_results_summary"][scraper] = {
                        "status": "failed", 
                        "error": result['error']
                    }
                else:
                    successful_scrapers += 1
                    response_data["scraper_results_summary"][scraper] = {
                        "status": "success",
                        "enhanced_leads": result.get('enhanced_leads', 0),
                        "leads_with_emails": result.get('leads_with_emails', 0),
                        "leads_with_phones": result.get('leads_with_phones', 0)
                    }
            elif result.get('error'):
                response_data["scraper_results_summary"][scraper] = {
                    "status": "failed", 
                    "error": result['error']
                }
            else:
                successful_scrapers += 1
                if scraper == 'web_scraper':
                    summary = result.get('summary', {})
                    response_data["scraper_results_summary"][scraper] = {
                        "status": "success",
                        "leads_found": summary.get('successful_leads', 0),
                        "urls_processed": summary.get('urls_processed', 0)
                    }
                elif scraper == 'instagram':
                    response_data["scraper_results_summary"][scraper] = {
                        "status": "success",
                        "profiles_found": len(result.get('data', [])),
                        "success_rate": result.get('summary', {}).get('success_rate', 0)
                    }
                elif scraper == 'linkedin':
                    metadata = result.get('scraping_metadata', {})
                    response_data["scraper_results_summary"][scraper] = {
                        "status": "success",
                        "profiles_found": metadata.get('successful_scrapes', 0),
                        "failed_scrapes": metadata.get('failed_scrapes', 0)
                    }
                elif scraper == 'youtube':
                    response_data["scraper_results_summary"][scraper] = {
                        "status": "success" if result.get('success') else "failed"
                    }
                elif scraper == 'facebook':
                    summary = result.get('summary', {})
                    performance_metrics = summary.get('performance_metrics', {})
                    unified_storage = result.get('unified_storage', {})
                    response_data["scraper_results_summary"][scraper] = {
                        "status": "success",
                        "profiles_found": len(result.get('data', [])),
                        "success_rate": summary.get('success_rate', 0),
                        "total_time_seconds": summary.get('total_time_seconds', 0),
                        "throughput_per_second": performance_metrics.get('throughput_per_second', 0),
                        "unified_leads_stored": unified_storage.get('success_count', 0)
                    }
        
        # Count successful scrapers (excluding lead_filtering and contact_enhancement)
        actual_successful_scrapers = len([r for r in scraper_results.items() 
                                        if not r[1].get('error') and r[0] not in ['lead_filtering', 'contact_enhancement']])
        
        response_data["pipeline_metadata"]["successful_scrapers"] = actual_successful_scrapers
        response_data["pipeline_metadata"]["total_scrapers"] = len(selected_scrapers)
        #response_data["pipeline_metadata"]["lead_filtering_successful"] = not lead_filtering_results.get('error')
        response_data["pipeline_metadata"]["contact_enhancement_successful"] = not contact_enhancement_results.get('error')
        
        logger.info(f"✅ Pipeline completed successfully in {execution_time:.2f} seconds")
        return response_data
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        raise

async def run_single_scraper_pipeline_async(orch, icp_data, scraper_name: str):
    """
    Run the pipeline for a single scraper only.
    """
    icp_identifier = orch.generate_icp_identifier(icp_data)
    return await run_pipeline_async(orch, icp_data, [scraper_name], icp_identifier, platform_override=scraper_name)

async def run_direct_pipeline_async(orch, scraper_selections, icp_identifier='default'):
    """
    Run the direct lead generation pipeline using URLs from scraped_urls collection
    """
    pipeline_start = datetime.now()
    
    try:
        # Step 1: Ensure scraped_urls collection exists
        logger.info("🗄️ Step 1: Ensuring scraped_urls collection exists...")
        try:
            from web_url_scraper.database_service import ensure_collection_exists
            ensure_collection_exists()
            logger.info("✅ scraped_urls collection ready")
        except Exception as e:
            logger.error(f"❌ Failed to ensure scraped_urls collection exists: {e}")
        
        # Step 2: Get URLs from scraped_urls collection based on scraper selections
        logger.info("🔍 Step 2: Collecting URLs from scraped_urls collection...")
        classified_urls = {}
        used_urls = []  # Track URLs that will be used for processing
        
        # Map scraper names to URL types
        scraper_to_url_type = get_url_type_map()
        
        for scraper, count in scraper_selections.items():
            if count > 0:
                url_type = scraper_to_url_type[scraper]
                logger.info(f"Getting {count} unprocessed URLs for {scraper} (type: {url_type}) with ICP: {icp_identifier}")
                
                # Get unprocessed URLs for this type and ICP identifier
                url_docs = get_urls_by_type_and_icp(url_type, icp_identifier, limit=count)
                
                if url_docs:
                    # Extract URLs from documents
                    urls = [doc['url'] for doc in url_docs]
                    classified_urls[url_type] = urls
                    used_urls.extend(urls)
                    
                    logger.info(f"✅ Found {len(urls)} URLs for {scraper}")
                else:
                    classified_urls[url_type] = []
                    logger.warning(f"⚠️ No unprocessed URLs found for {scraper}")
        
        total_urls = sum(len(urls) for urls in classified_urls.values())
        if total_urls == 0:
            raise Exception("No unprocessed URLs found for any selected scrapers")
        
        logger.info(f"✅ Collected {total_urls} URLs from scraped_urls collection")
        
        # Step 3: Mark URLs as processed to prevent duplicate processing
        logger.info("🏷️ Step 3: Marking URLs as processed...")
        marked_count = mark_urls_as_processed(used_urls)
        logger.info(f"✅ Marked {marked_count} URLs as processed")
        
        # Step 4: Run selected scrapers
        logger.info("🚀 Step 4: Running scrapers...")
        selected_scrapers = list(scraper_selections.keys())
        
        # Get ICP data for the scrapers
        if icp_identifier == 'default':
            icp_data = orch.get_hardcoded_icp()
            icp_identifier = orch.generate_icp_identifier(icp_data)
        else:
            # If a specific ICP identifier is provided, we need to get the ICP data
            # For now, use hardcoded ICP as fallback
            icp_data = orch.get_hardcoded_icp()
        
        scraper_results = await orch.run_selected_scrapers(classified_urls, selected_scrapers, icp_identifier, icp_data)
        
        """
        # Step 5: Filter and process leads using MongoDBLeadProcessor
        logger.info("🧹 Step 5: Filtering and processing leads...")
        lead_filtering_results = {}
        try:
            lead_processor = MongoDBLeadProcessor()
            
            # Create indexes for the target collection
            lead_processor.create_indexes()
            
            # Process all leads from web_leads collection to leadgen_leads collection
            filtering_results = lead_processor.process_leads(batch_size=50)
            
            # Get processing statistics
            processing_stats = lead_processor.get_processing_stats()
            
            lead_filtering_results = {
                'filtering_stats': filtering_results,
                'processing_stats': processing_stats
            }
            
            lead_processor.close_connection()
            
        except Exception as e:
            logger.error(f"❌ Error in lead filtering: {e}")
            lead_filtering_results = {'error': str(e)}
        
        # Add filtering results to scraper results
        scraper_results['lead_filtering'] = lead_filtering_results
        """
        # Step 6: Enhance leads with contact information using contact scraper
        logger.info("📞 Step 6: Enhancing leads with contact information...")
        contact_enhancement_results = {}
        try:
            contact_enhancement_data = await run_optimized_contact_scraper(
                limit=0,  # Process all leads without contact info
                batch_size=20
            )
            
            # Count leads with emails and phone numbers
            leads_with_emails = sum(1 for lead in contact_enhancement_data if lead.get('emails'))
            leads_with_phones = sum(1 for lead in contact_enhancement_data if lead.get('phone_numbers'))
            
            contact_enhancement_results = {
                'enhanced_leads': len(contact_enhancement_data),
                'leads_with_emails': leads_with_emails,
                'leads_with_phones': leads_with_phones,
                'enhancement_data': contact_enhancement_data
            }
            
        except Exception as e:
            logger.error(f"❌ Error in contact enhancement: {e}")
            contact_enhancement_results = {'error': str(e)}
        
        # Add contact enhancement results to scraper results
        scraper_results['contact_enhancement'] = contact_enhancement_results
        
        # Step 7: Generate final report
        logger.info("📊 Step 7: Generating final report...")
        # Create dummy ICP data for report generation
        dummy_icp_data = orch.get_hardcoded_icp()
        report_file = orch.generate_final_report(dummy_icp_data, selected_scrapers, scraper_results)
        
        pipeline_end = datetime.now()
        execution_time = (pipeline_end - pipeline_start).total_seconds()
        
        # Prepare response
        response_data = {
            "pipeline_metadata": {
                "execution_time_seconds": execution_time,
                "start_time": pipeline_start.isoformat(),
                "end_time": pipeline_end.isoformat(),
                "selected_scrapers": selected_scrapers,
                "total_urls_processed": total_urls,
                "urls_marked_processed": marked_count,
                "pipeline_type": "direct"
            },
            "url_collection": {
                "classified_urls_count": {k: len(v) for k, v in classified_urls.items()},
                "total_urls": total_urls,
                "urls_used": used_urls
            },
            "scraper_results_summary": {},
            "report_file": report_file
        }
        
        # Generate summary for each scraper
        successful_scrapers = 0
        for scraper, result in scraper_results.items():
            # COMMENTED OUT - crl.py removed from flow
            # if scraper == 'web_crawler':
            #     # Handle web crawler results separately
            #     if result.get('success'):
            #         successful_scrapers += 1
            #         summary = result.get('summary', {})
            #         response_data["scraper_results_summary"][scraper] = {
            #             "status": "success",
            #             "leads_found": summary.get('total_leads_found', 0),
            #             "leads_stored": summary.get('leads_stored', 0),
            #             "duplicates_found": summary.get('duplicates_found', 0),
            #             "urls_crawled": summary.get('urls_crawled', 0),
            #             "execution_time_seconds": summary.get('execution_time_seconds', 0)
            #         }
            #     else:
            #         response_data["scraper_results_summary"][scraper] = {
            #             "status": "failed",
            #             "error": result.get('error', 'Unknown error')
            #         }
            """
            if scraper == 'lead_filtering':
                # Handle lead filtering results separately
                if result.get('error'):
                    response_data["scraper_results_summary"][scraper] = {
                        "status": "failed", 
                        "error": result['error']
                    }
                else:
                    successful_scrapers += 1
                    filtering_stats = result.get('filtering_stats', {})
                    response_data["scraper_results_summary"][scraper] = {
                        "status": "success",
                        "leads_processed": filtering_stats.get('total', 0),
                        "leads_filtered": filtering_stats.get('filtered', 0),
                        "leads_extracted": filtering_stats.get('extracted', 0),
                        "leads_inserted": filtering_stats.get('inserted', 0),
                        "email_based_leads": filtering_stats.get('email_based', 0),
                        "phone_based_leads": filtering_stats.get('phone_based', 0)
                    }
            """
            if scraper == 'contact_enhancement':
                # Handle contact enhancement results separately
                if result.get('error'):
                    response_data["scraper_results_summary"][scraper] = {
                        "status": "failed", 
                        "error": result['error']
                    }
                else:
                    successful_scrapers += 1
                    response_data["scraper_results_summary"][scraper] = {
                        "status": "success",
                        "enhanced_leads": result.get('enhanced_leads', 0),
                        "leads_with_emails": result.get('leads_with_emails', 0),
                        "leads_with_phones": result.get('leads_with_phones', 0)
                    }
            elif result.get('error'):
                response_data["scraper_results_summary"][scraper] = {
                    "status": "failed", 
                    "error": result['error']
                }
            else:
                successful_scrapers += 1
                if scraper == 'web_scraper':
                    summary = result.get('summary', {})
                    response_data["scraper_results_summary"][scraper] = {
                        "status": "success",
                        "leads_found": summary.get('successful_leads', 0),
                        "urls_processed": summary.get('urls_processed', 0)
                    }
                elif scraper == 'instagram':
                    response_data["scraper_results_summary"][scraper] = {
                        "status": "success",
                        "profiles_found": len(result.get('data', [])),
                        "success_rate": result.get('summary', {}).get('success_rate', 0)
                    }
                elif scraper == 'linkedin':
                    metadata = result.get('scraping_metadata', {})
                    response_data["scraper_results_summary"][scraper] = {
                        "status": "success",
                        "profiles_found": metadata.get('successful_scrapes', 0),
                        "failed_scrapes": metadata.get('failed_scrapes', 0)
                    }
                elif scraper == 'youtube':
                    response_data["scraper_results_summary"][scraper] = {
                        "status": "success" if result.get('success') else "failed"
                    }
                elif scraper == 'facebook':
                    summary = result.get('summary', {})
                    performance_metrics = summary.get('performance_metrics', {})
                    unified_storage = result.get('unified_storage', {})
                    response_data["scraper_results_summary"][scraper] = {
                        "status": "success",
                        "profiles_found": len(result.get('data', [])),
                        "success_rate": summary.get('success_rate', 0),
                        "total_time_seconds": summary.get('total_time_seconds', 0),
                        "throughput_per_second": performance_metrics.get('throughput_per_second', 0),
                        "unified_leads_stored": unified_storage.get('success_count', 0)
                    }
        
        # Count successful scrapers (excluding lead_filtering and contact_enhancement)
        actual_successful_scrapers = len([r for r in scraper_results.items() 
                                        if not r[1].get('error') and r[0] not in ['lead_filtering', 'contact_enhancement']])
        
        response_data["pipeline_metadata"]["successful_scrapers"] = actual_successful_scrapers
        response_data["pipeline_metadata"]["total_scrapers"] = len(selected_scrapers)
        #response_data["pipeline_metadata"]["lead_filtering_successful"] = not lead_filtering_results.get('error')
        response_data["pipeline_metadata"]["contact_enhancement_successful"] = not contact_enhancement_results.get('error')
        
        logger.info(f"✅ Direct pipeline completed successfully in {execution_time:.2f} seconds")
        return response_data
        
    except Exception as e:
        logger.error(f"❌ Direct pipeline failed: {e}")
        raise

@app.route('/api/queries/generate', methods=['POST'])
def generate_queries_only():
    """
    Generate search queries only (without running scrapers)
    
    Expected payload:
    {
        "icp_data": {...},
        "selected_scrapers": [...]
    }
    """
    try:
        if not request.is_json:
            raise BadRequest("Request must be JSON")
        
        data = request.get_json()
        icp_data = data.get('icp_data')
        selected_scrapers = data.get('selected_scrapers', ['web_scraper'])
        
        if not icp_data:
            return jsonify({
                "success": False,
                "error": "icp_data is required"
            }), 400
        
        orch = get_orchestrator()
        
        # Generate queries asynchronously
        queries = run_async(orch.generate_search_queries(icp_data, selected_scrapers))
        
        return jsonify({
            "success": True,
            "data": {
                "queries": queries,
                "total_queries": len(queries),
                "selected_scrapers": selected_scrapers
            }
        })
        
    except BadRequest as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400
    except Exception as e:
        logger.error(f"Error generating queries: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/scraper/<scraper_name>/run', methods=['POST'])
def run_single_scraper(scraper_name):
    """
    Run lead generation pipeline for a single scraper.
    Supported scraper_name values: instagram, linkedin, web_scraper, youtube, facebook  # company_directory commented out
    
    Expected payload:
    {
        "icp_data": {...}
    }
    """
    try:
        # Validate request
        if not request.is_json:
            raise BadRequest("Request must be JSON")

        data = request.get_json()
        icp_data = data.get('icp_data')

        if not is_valid_scraper(scraper_name):
            return jsonify({
                "success": False,
                "error": f"Invalid scraper: {scraper_name}."
            }), 400

        if not icp_data:
            return jsonify({
                "success": False,
                "error": "icp_data is required"
            }), 400

        logger.info(f"Starting single-scraper pipeline for: {scraper_name}")

        # Get orchestrator instance - use fresh instance for testing
        from main import LeadGenerationOrchestrator
        orch = LeadGenerationOrchestrator()

        # Run pipeline asynchronously for one scraper
        result = run_async(run_single_scraper_pipeline_async(orch, icp_data, scraper_name))
        
        return jsonify({
            "success": True,
            "data": result,
            "icp_identifier": result.get('pipeline_metadata', {}).get('icp_identifier') or result.get('icp_identifier'),
            "message": f"{scraper_name} pipeline completed successfully"
        })

    except BadRequest as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400
    except Exception as e:
        logger.error(f"Error in single-scraper pipeline ({scraper_name}): {e}")
        return jsonify({
            "success": False,
            "error": f"{scraper_name} pipeline failed: {str(e)}"
        }), 500

@app.route('/api/lead-filtering/run', methods=['POST'])
def run_lead_filtering():
    """
    Run only the lead filtering process
    
    Expected payload:
    {
        "query_filter": {},  # Optional MongoDB query filter
        "batch_size": 50     # Optional batch size
    }
    """
    try:
        # Validate request
        if not request.is_json:
            raise BadRequest("Request must be JSON")
        
        data = request.get_json() or {}
        query_filter = data.get('query_filter', {})
        batch_size = data.get('batch_size', 50)
        
        logger.info(f"Starting lead filtering process with batch_size: {batch_size}")
        
        # Initialize lead processor
        lead_processor = MongoDBLeadProcessor()
        
        # Create indexes
        lead_processor.create_indexes()
        
        # Process leads
        filtering_results = lead_processor.process_leads(
            query_filter=query_filter, 
            batch_size=batch_size
        )
        
        # Get processing statistics
        processing_stats = lead_processor.get_processing_stats()
        
        # Close connection
        lead_processor.close_connection()
        
        return jsonify({
            "success": True,
            "data": {
                "filtering_results": filtering_results,
                "processing_stats": processing_stats,
                "timestamp": datetime.now().isoformat()
            },
            "message": "Lead filtering completed successfully"
        })
        
    except BadRequest as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400
    except Exception as e:
        logger.error(f"Error in lead filtering: {e}")
        return jsonify({
            "success": False,
            "error": f"Lead filtering failed: {str(e)}"
        }), 500

@app.route('/api/contact-enhancement/run', methods=['POST'])
def run_contact_enhancement():
    """
    Run only the contact enhancement process
    
    Expected payload:
    {
        "limit": 0,          # Optional limit on number of leads to process (0 = all)
        "batch_size": 20     # Optional batch size for processing
    }
    """
    try:
        # Validate request
        if not request.is_json:
            raise BadRequest("Request must be JSON")
        
        data = request.get_json() or {}
        limit = data.get('limit', 0)
        batch_size = data.get('batch_size', 20)
        
        logger.info(f"Starting contact enhancement process with limit: {limit}, batch_size: {batch_size}")
        
        # Run contact enhancement asynchronously
        contact_enhancement_data = run_async(run_optimized_contact_scraper(
            limit=limit,
            batch_size=batch_size
        ))
        
        # Count leads with emails and phone numbers
        leads_with_emails = sum(1 for lead in contact_enhancement_data if lead.get('emails'))
        leads_with_phones = sum(1 for lead in contact_enhancement_data if lead.get('phone_numbers'))
        
        return jsonify({
            "success": True,
            "data": {
                "enhanced_leads": len(contact_enhancement_data),
                "leads_with_emails": leads_with_emails,
                "leads_with_phones": leads_with_phones,
                "enhancement_data": contact_enhancement_data,
                "timestamp": datetime.now().isoformat()
            },
            "message": "Contact enhancement completed successfully"
        })
        
    except BadRequest as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400
    except Exception as e:
        logger.error(f"Error in contact enhancement: {e}")
        return jsonify({
            "success": False,
            "error": f"Contact enhancement failed: {str(e)}"
        }), 500

@app.route('/api/status', methods=['GET'])
def get_system_status():
    """Get system status and capabilities"""
    try:
        orch = get_orchestrator()
        
        # Check Gemini AI availability
        gemini_available = orch.gemini_model is not None
        
        # Check MongoDB availability  
        mongodb_available = orch.mongodb_manager is not None
        
        return jsonify({
            "success": True,
            "data": {
                "system_status": "operational",
                "components": {
                    "gemini_ai": {
                        "available": gemini_available,
                        "status": "connected" if gemini_available else "not_configured"
                    },
                    "mongodb": {
                        "available": mongodb_available,
                        "status": "connected" if mongodb_available else "not_connected"
                    },
                    "scrapers": orch.available_scrapers
                },
                "timestamp": datetime.now().isoformat()
            }
        })
        
    except Exception as e:
        logger.error(f"Error getting system status: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/urls/available', methods=['GET'])
def get_available_urls():
    """Get count of available unprocessed URLs by type"""
    try:
        available_counts = get_available_url_counts()
        
        return jsonify({
            "success": True,
            "data": {
                "available_urls": available_counts,
                "total_available": sum(available_counts.values()),
                "timestamp": datetime.now().isoformat()
            }
        })
        
    except Exception as e:
        logger.error(f"Error getting available URLs: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/leads/by-icp/<icp_identifier>', methods=['GET'])
def get_leads_by_icp(icp_identifier):
    """Get leads filtered by ICP identifier"""
    try:
        # Get query parameters
        limit = request.args.get('limit', 100, type=int)
        skip = request.args.get('skip', 0, type=int)
        
        # Validate parameters
        if limit < 0 or limit > 1000:
            return jsonify({
                "success": False,
                "error": "Limit must be between 0 and 1000"
            }), 400
        
        if skip < 0:
            return jsonify({
                "success": False,
                "error": "Skip must be non-negative"
            }), 400
        
        # Get MongoDB manager
        orch = get_orchestrator()
        mongodb_manager = orch.mongodb_manager
        
        # Get leads by ICP identifier
        leads = mongodb_manager.get_leads_by_icp_identifier(icp_identifier, limit, skip)
        
        return jsonify({
            "success": True,
            "data": {
                "icp_identifier": icp_identifier,
                "leads": leads,
                "total_returned": len(leads),
                "limit": limit,
                "skip": skip,
                "timestamp": datetime.now().isoformat()
            }
        })
        
    except Exception as e:
        logger.error(f"Error getting leads by ICP: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/leads/icp-stats/<icp_identifier>', methods=['GET'])
def get_icp_statistics(icp_identifier):
    """Get statistics for a specific ICP identifier"""
    try:
        # Get MongoDB manager
        orch = get_orchestrator()
        mongodb_manager = orch.mongodb_manager
        
        # Get ICP statistics
        stats = mongodb_manager.get_icp_statistics(icp_identifier)
        
        if 'error' in stats:
            return jsonify({
                "success": False,
                "error": stats['error']
            }), 500
        
        return jsonify({
            "success": True,
            "data": stats
        })
        
    except Exception as e:
        logger.error(f"Error getting ICP statistics: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/lead-generation/direct', methods=['POST'])
def run_direct_lead_generation():
    """
    Run lead generation directly from scraped URLs without ICP/query generation
    
    Expected payload:
    {
        "scraper_selections": {
            "web_scraper": 10,
            "instagram": 5,
            "linkedin": 6,
            "youtube": 5,
            "facebook": 5
        },
        "icp_identifier": "premium-bus-travel_20241201_1430_a1b2c3d4"  # Optional, defaults to "default"
    }
    """
    try:
        # Validate request
        if not request.is_json:
            raise BadRequest("Request must be JSON")
        
        data = request.get_json()
        scraper_selections = data.get('scraper_selections', {})
        icp_identifier = data.get('icp_identifier', 'default')
        
        if not scraper_selections:
            return jsonify({
                "success": False,
                "error": "scraper_selections is required"
            }), 400
        
        # Validate scraper selections
        valid_scrapers = list(get_url_type_map().keys())
        for scraper, count in scraper_selections.items():
            if scraper not in valid_scrapers:
                return jsonify({
                    "success": False,
                    "error": f"Invalid scraper: {scraper}. Must be one of {valid_scrapers}"
                }), 400
            
            if not isinstance(count, int) or count < 0:
                return jsonify({
                    "success": False,
                    "error": f"Invalid count for {scraper}: {count}. Must be a non-negative integer"
                }), 400
        
        logger.info(f"Starting direct lead generation with scraper selections: {scraper_selections}")
        
        # Get orchestrator instance
        orch = get_orchestrator()
        
        # Run the direct pipeline asynchronously
        result = run_async(run_direct_pipeline_async(orch, scraper_selections, icp_identifier))
        
        return jsonify({
            "success": True,
            "data": result,
            "icp_identifier": icp_identifier,
            "message": "Direct lead generation pipeline completed successfully"
        })
        
    except BadRequest as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400
    except Exception as e:
        logger.error(f"Error in direct lead generation pipeline: {e}")
        return jsonify({
            "success": False,
            "error": f"Direct pipeline failed: {str(e)}"
        }), 500

@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({
        "success": False,
        "error": "Endpoint not found",
        "available_endpoints": [
            "GET /health - Health check",
            "GET /api/scrapers - Get available scrapers", 
            "GET /api/icp/template - Get ICP template",
            "POST /api/lead-generation/run - Run complete pipeline",
            "POST /api/lead-generation/direct - Run direct pipeline from scraped URLs",
            "POST /api/queries/generate - Generate queries only",
            "POST /api/lead-filtering/run - Run lead filtering only",
            "POST /api/contact-enhancement/run - Run contact enhancement only",
            "GET /api/urls/available - Get available unprocessed URLs count",
            "GET /api/leads/by-icp/<icp_identifier> - Get leads by ICP identifier",
            "GET /api/leads/icp-stats/<icp_identifier> - Get ICP statistics",
            "POST /api/scraper/<scraper_name>/run - Run single scraper pipeline (instagram|linkedin|web_scraper|youtube|facebook)",  # company_directory commented out
            "GET /api/status - Get system status"
        ]
    }), 404

@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    return jsonify({
        "success": False,
        "error": "Internal server error",
        "message": "An unexpected error occurred"
    }), 500

if __name__ == '__main__':
    # Development server
    print("🚀 Starting Lead Generation Flask API...")
    print("📋 Available endpoints:")
    print("  GET  /health - Health check")
    print("  GET  /api/scrapers - Get available scrapers")
    print("  GET  /api/icp/template - Get ICP template") 
    print("  POST /api/lead-generation/run - Run complete pipeline")
    print("  POST /api/lead-generation/direct - Run direct pipeline from scraped URLs")
    print("  POST /api/queries/generate - Generate queries only")
    print("  POST /api/lead-filtering/run - Run lead filtering only")
    print("  POST /api/contact-enhancement/run - Run contact enhancement only")
    print("  GET  /api/urls/available - Get available unprocessed URLs count")
    print("  GET  /api/leads/by-icp/<icp_identifier> - Get leads by ICP identifier")
    print("  GET  /api/leads/icp-stats/<icp_identifier> - Get ICP statistics")
    print("  GET  /api/status - Get system status")
    print("")
    
    # Run the app
    app.run(
        host='0.0.0.0',
        port=int(os.environ.get('PORT', 5000)),
        debug=os.environ.get('FLASK_ENV') == 'development'
    )