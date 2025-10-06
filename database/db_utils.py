"""
Database Utilities for Lead Generation Application
Provides utility functions to manage and query the MongoDB database
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from database.mongodb_manager import get_mongodb_manager


class DatabaseUtils:
    """Utility class for database operations"""
    
    def __init__(self):
        self.mongodb_manager = get_mongodb_manager()
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get comprehensive database statistics"""
        stats = self.mongodb_manager.get_database_stats()
        
        # Add additional statistics
        for source in ['instagram', 'linkedin', 'web', 'youtube', 'company_directory']:
            try:
                collection = self.mongodb_manager.db[self.mongodb_manager.collections[source]]
                
                # Get recent activity (last 7 days)
                week_ago = datetime.utcnow() - timedelta(days=7)
                recent_count = collection.count_documents({
                    'scraped_at': {'$gte': week_ago}
                })
                
                # Get unique domains/username counts
                if source == 'web':
                    unique_domains = len(collection.distinct('domain'))
                    stats[f'{source}_unique_domains'] = unique_domains
                else:
                    unique_usernames = len(collection.distinct('username'))
                    stats[f'{source}_unique_usernames'] = unique_usernames
                
                stats[f'{source}_recent_leads'] = recent_count
                
            except Exception as e:
                print(f"Error getting stats for {source}: {e}")
        
        return stats
    
    def search_leads(self, 
                    query: str = None, 
                    source: str = None, 
                    limit: int = 100,
                    date_from: str = None,
                    date_to: str = None) -> List[Dict[str, Any]]:
        """
        Search leads with various filters
        
        Args:
            query: Text to search in business names, usernames, etc.
            source: Specific source ('instagram', 'linkedin', 'web', 'youtube', 'company_directory')
            limit: Maximum number of results
            date_from: Start date (YYYY-MM-DD format)
            date_to: End date (YYYY-MM-DD format)
        """
        search_query = {}
        
        # Add text search
        if query:
            search_query['$or'] = [
                {'business_name': {'$regex': query, '$options': 'i'}},
                {'username': {'$regex': query, '$options': 'i'}},
                {'full_name': {'$regex': query, '$options': 'i'}},
                {'contact_person': {'$regex': query, '$options': 'i'}},
                {'email': {'$regex': query, '$options': 'i'}}
            ]
        
        # Add date range filter
        if date_from or date_to:
            date_filter = {}
            if date_from:
                date_filter['$gte'] = datetime.strptime(date_from, '%Y-%m-%d')
            if date_to:
                date_filter['$lte'] = datetime.strptime(date_to, '%Y-%m-%d') + timedelta(days=1)
            search_query['scraped_at'] = date_filter
        
        return self.mongodb_manager.search_leads(search_query, source, limit)
    
    def export_leads(self, 
                    source: str = None, 
                    format: str = 'json',
                    output_file: str = None,
                    date_from: str = None,
                    date_to: str = None) -> str:
        """
        Export leads to file
        
        Args:
            source: Specific source or None for all
            format: Export format ('json', 'csv')
            output_file: Output file path
            date_from: Start date (YYYY-MM-DD format)
            date_to: End date (YYYY-MM-DD format)
        """
        if not output_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            source_suffix = f"_{source}" if source else "_all"
            output_file = f"leads_export{source_suffix}_{timestamp}.{format}"
        
        # Get leads
        leads = self.search_leads(source=source, date_from=date_from, date_to=date_to, limit=10000)
        
        if format.lower() == 'json':
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(leads, f, indent=2, ensure_ascii=False, default=str)
        elif format.lower() == 'csv':
            import csv
            if leads:
                with open(output_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=leads[0].keys())
                    writer.writeheader()
                    writer.writerows(leads)
        
        print(f"✅ Exported {len(leads)} leads to {output_file}")
        return output_file
    
    def get_recent_leads(self, hours: int = 24, source: str = None) -> List[Dict[str, Any]]:
        """Get leads from the last N hours"""
        time_threshold = datetime.utcnow() - timedelta(hours=hours)
        query = {'scraped_at': {'$gte': time_threshold}}
        return self.mongodb_manager.search_leads(query, source, limit=1000)
    
    def get_duplicate_leads(self, source: str = None) -> List[Dict[str, Any]]:
        """Find potential duplicate leads"""
        if source:
            collection = self.mongodb_manager.db[self.mongodb_manager.collections[source]]
        else:
            # Search across all collections
            duplicates = []
            for source_name in ['instagram', 'linkedin', 'web', 'youtube', 'company_directory']:
                collection = self.mongodb_manager.db[self.mongodb_manager.collections[source_name]]
                # Find documents with same email or phone
                pipeline = [
                    {
                        '$match': {
                            '$or': [
                                {'email': {'$ne': None, '$ne': ''}},
                                {'phone': {'$ne': None, '$ne': ''}},
                                {'business_phone_number': {'$ne': None, '$ne': ''}}
                            ]
                        }
                    },
                    {
                        '$group': {
                            '_id': {
                                'email': '$email',
                                'phone': '$phone',
                                'business_phone_number': '$business_phone_number'
                            },
                            'count': {'$sum': 1},
                            'documents': {'$push': '$$ROOT'}
                        }
                    },
                    {
                        '$match': {'count': {'$gt': 1}}
                    }
                ]
                duplicates.extend(list(collection.aggregate(pipeline)))
            return duplicates
        
        # For specific source
        pipeline = [
            {
                '$match': {
                    '$or': [
                        {'email': {'$ne': None, '$ne': ''}},
                        {'phone': {'$ne': None, '$ne': ''}},
                        {'business_phone_number': {'$ne': None, '$ne': ''}}
                    ]
                }
            },
            {
                '$group': {
                    '_id': {
                        'email': '$email',
                        'phone': '$phone',
                        'business_phone_number': '$business_phone_number'
                    },
                    'count': {'$sum': 1},
                    'documents': {'$push': '$$ROOT'}
                }
            },
            {
                '$match': {'count': {'$gt': 1}}
            }
        ]
        
        return list(collection.aggregate(pipeline))
    
    def cleanup_old_leads(self, days: int = 30, source: str = None) -> int:
        """Remove leads older than specified days"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        query = {'scraped_at': {'$lt': cutoff_date}}
        
        deleted_count = 0
        if source:
            collection = self.mongodb_manager.db[self.mongodb_manager.collections[source]]
            result = collection.delete_many(query)
            deleted_count = result.deleted_count
        else:
            for source_name in ['instagram', 'linkedin', 'web', 'youtube', 'company_directory']:
                collection = self.mongodb_manager.db[self.mongodb_manager.collections[source_name]]
                result = collection.delete_many(query)
                deleted_count += result.deleted_count
        
        print(f"✅ Deleted {deleted_count} leads older than {days} days")
        return deleted_count


def main():
    """CLI interface for database utilities"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Database Utilities for Lead Generation")
    parser.add_argument("--action", choices=['stats', 'search', 'export', 'recent', 'duplicates', 'cleanup'], 
                       required=True, help="Action to perform")
    parser.add_argument("--source", choices=['instagram', 'linkedin', 'web', 'youtube', 'company_directory'], 
                       help="Specific source to query")
    parser.add_argument("--query", help="Search query")
    parser.add_argument("--limit", type=int, default=100, help="Maximum results")
    parser.add_argument("--format", choices=['json', 'csv'], default='json', help="Export format")
    parser.add_argument("--output", help="Output file path")
    parser.add_argument("--date-from", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--date-to", help="End date (YYYY-MM-DD)")
    parser.add_argument("--hours", type=int, default=24, help="Hours for recent leads")
    parser.add_argument("--days", type=int, default=30, help="Days for cleanup")
    
    args = parser.parse_args()
    
    db_utils = DatabaseUtils()
    
    try:
        if args.action == 'stats':
            stats = db_utils.get_database_stats()
            print(json.dumps(stats, indent=2, default=str))
            
        elif args.action == 'search':
            results = db_utils.search_leads(
                query=args.query,
                source=args.source,
                limit=args.limit,
                date_from=args.date_from,
                date_to=args.date_to
            )
            print(f"Found {len(results)} results")
            print(json.dumps(results, indent=2, default=str))
            
        elif args.action == 'export':
            output_file = db_utils.export_leads(
                source=args.source,
                format=args.format,
                output_file=args.output,
                date_from=args.date_from,
                date_to=args.date_to
            )
            print(f"Exported to: {output_file}")
            
        elif args.action == 'recent':
            results = db_utils.get_recent_leads(args.hours, args.source)
            print(f"Found {len(results)} recent leads")
            print(json.dumps(results, indent=2, default=str))
            
        elif args.action == 'duplicates':
            results = db_utils.get_duplicate_leads(args.source)
            print(f"Found {len(results)} duplicate groups")
            print(json.dumps(results, indent=2, default=str))
            
        elif args.action == 'cleanup':
            deleted = db_utils.cleanup_old_leads(args.days, args.source)
            print(f"Deleted {deleted} leads")
            
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
