from __future__ import annotations

import json
import csv
import gzip
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import zipfile
import io

from loguru import logger
from web_scraper.storage.storage import LeadModel, LeadStorage, LeadStatus


class ExportMetadata(dict):
    """Export metadata for tracking and validation"""
    
    def __init__(self, export_type: str, total_records: int, filters: Dict[str, Any] = None):
        super().__init__()
        self.update({
            "export_type": export_type,
            "export_timestamp": datetime.now().isoformat(),
            "total_records": total_records,
            "filters_applied": filters or {},
            "schema_version": "1.0",
            "generated_by": "web_scraper_v1.0"
        })


class JSONExporter:
    """JSON export functionality with metadata and schema validation"""
    
    def __init__(self, compress: bool = False):
        self.compress = compress
        
    def export_leads(self, 
                    leads: List[LeadModel], 
                    output_path: str,
                    include_metadata: bool = True,
                    filters: Dict[str, Any] = None) -> str:
        """Export leads to JSON format with metadata and schema validation"""
        
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Prepare export data
        export_data = {
            "leads": [lead.dict() for lead in leads]
        }
        
        if include_metadata:
            export_data["metadata"] = ExportMetadata(
                export_type="json",
                total_records=len(leads),
                filters=filters
            )
            
        # Add schema information
        export_data["schema"] = {
            "version": "1.0",
            "lead_model_fields": list(LeadModel.__fields__.keys()),
            "required_fields": [
                field for field, info in LeadModel.__fields__.items() 
                if info.is_required()
            ]
        }
        
        # Write to file
        if self.compress:
            output_file = output_file.with_suffix(output_file.suffix + '.gz')
            with gzip.open(output_file, 'wt', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False, default=str)
        else:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False, default=str)
                
        logger.info(f"Exported {len(leads)} leads to JSON: {output_file}")
        return str(output_file)
    
    def validate_schema(self, json_file: str) -> Dict[str, Any]:
        """Validate exported JSON against schema"""
        
        try:
            if json_file.endswith('.gz'):
                with gzip.open(json_file, 'rt', encoding='utf-8') as f:
                    data = json.load(f)
            else:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
            validation_result = {
                "valid": True,
                "errors": [],
                "warnings": []
            }
            
            # Check required structure
            if "leads" not in data:
                validation_result["valid"] = False
                validation_result["errors"].append("Missing 'leads' field")
                
            if "metadata" not in data:
                validation_result["warnings"].append("Missing metadata field")
                
            if "schema" not in data:
                validation_result["warnings"].append("Missing schema field")
                
            # Validate individual leads
            if "leads" in data:
                for i, lead_data in enumerate(data["leads"]):
                    try:
                        LeadModel(**lead_data)
                    except Exception as e:
                        validation_result["valid"] = False
                        validation_result["errors"].append(f"Lead {i}: {str(e)}")
                        
            return validation_result
            
        except Exception as e:
            return {
                "valid": False,
                "errors": [f"Failed to validate file: {str(e)}"],
                "warnings": []
            }


class CSVExporter:
    """CSV export with flattened structure and Excel compatibility"""
    
    def __init__(self, excel_compatible: bool = True):
        self.excel_compatible = excel_compatible
        
    def export_leads(self, 
                    leads: List[LeadModel], 
                    output_path: str,
                    include_metadata_sheet: bool = True,
                    custom_fields: List[str] = None,
                    filters: Dict[str, Any] = None) -> str:
        """Export leads to CSV format with Excel compatibility"""
        
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        if not leads:
            logger.warning("No leads to export")
            return str(output_file)
            
        # Get flattened data
        flattened_leads = [lead.to_flat_dict() for lead in leads]
        
        # Determine fields to export
        all_fields = set()
        for lead_dict in flattened_leads:
            all_fields.update(lead_dict.keys())
            
        if custom_fields:
            # Use only specified fields that exist
            export_fields = [f for f in custom_fields if f in all_fields]
        else:
            # Use all fields, sorted for consistency
            export_fields = sorted(all_fields)
            
        # Write CSV file
        with open(output_file, 'w', newline='', encoding='utf-8-sig' if self.excel_compatible else 'utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=export_fields, extrasaction='ignore')
            writer.writeheader()
            
            for lead_dict in flattened_leads:
                # Fill missing fields with empty strings
                row = {field: lead_dict.get(field, '') for field in export_fields}
                writer.writerow(row)
                
        logger.info(f"Exported {len(leads)} leads to CSV: {output_file}")
        
        # Create metadata file if requested
        if include_metadata_sheet:
            metadata_file = output_file.with_suffix('.metadata.json')
            metadata = ExportMetadata(
                export_type="csv",
                total_records=len(leads),
                filters=filters
            )
            metadata["exported_fields"] = export_fields
            metadata["field_count"] = len(export_fields)
            
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)
                
        return str(output_file)
    
    def export_to_excel_sheets(self, 
                              leads: List[LeadModel], 
                              output_path: str,
                              filters: Dict[str, Any] = None) -> str:
        """Export to multiple CSV files simulating Excel sheets"""
        
        output_dir = Path(output_path).parent / f"{Path(output_path).stem}_sheets"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Main leads sheet
        main_file = output_dir / "leads.csv"
        self.export_leads(leads, str(main_file), include_metadata_sheet=False, filters=filters)
        
        # Summary sheet
        summary_file = output_dir / "summary.csv"
        self._create_summary_sheet(leads, summary_file)
        
        # Statistics sheet
        stats_file = output_dir / "statistics.csv"
        self._create_statistics_sheet(leads, stats_file)
        
        # Create a zip file containing all sheets
        zip_file = Path(output_path).with_suffix('.zip')
        with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            for csv_file in output_dir.glob("*.csv"):
                zf.write(csv_file, csv_file.name)
                
        logger.info(f"Exported {len(leads)} leads to Excel-style sheets: {zip_file}")
        return str(zip_file)
    
    def _create_summary_sheet(self, leads: List[LeadModel], output_file: Path):
        """Create summary statistics sheet"""
        
        # Calculate summary statistics
        total_leads = len(leads)
        status_counts = {}
        industry_counts = {}
        scores = [lead.lead_score for lead in leads if lead.lead_score is not None]
        
        for lead in leads:
            status_counts[lead.status.value] = status_counts.get(lead.status.value, 0) + 1
            if lead.industry:
                industry_counts[lead.industry] = industry_counts.get(lead.industry, 0) + 1
        
        summary_data = [
            {"Metric", "Value"},
            {"Total Leads", total_leads},
            {"Average Score", f"{sum(scores) / len(scores):.2f}" if scores else "N/A"},
            {"Min Score", f"{min(scores):.2f}" if scores else "N/A"},
            {"Max Score", f"{max(scores):.2f}" if scores else "N/A"},
            {"", ""},
            {"Status Distribution", ""},
        ]
        
        for status, count in status_counts.items():
            summary_data.append({status.title(), count})
            
        summary_data.extend([{"", ""}, {"Industry Distribution", ""}])
        
        for industry, count in sorted(industry_counts.items()):
            summary_data.append({industry, count})
            
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            for row in summary_data:
                writer.writerow(row)
    
    def _create_statistics_sheet(self, leads: List[LeadModel], output_file: Path):
        """Create detailed statistics sheet"""
        
        stats_data = [
            ["Field", "Filled Count", "Fill Rate %", "Avg Confidence"],
            ["Email", 0, 0, 0],
            ["Phone", 0, 0, 0],
            ["Address", 0, 0, 0],
            ["Website", 0, 0, 0],
            ["Industry", 0, 0, "N/A"],
            ["Services", 0, 0, "N/A"],
        ]
        
        total_leads = len(leads)
        if total_leads == 0:
            return
            
        # Calculate field statistics
        field_stats = {
            "email": {"count": 0, "confidence": []},
            "phone": {"count": 0, "confidence": []},
            "address": {"count": 0, "confidence": []},
            "website": {"count": 0, "confidence": []},
            "industry": {"count": 0},
            "services": {"count": 0},
        }
        
        for lead in leads:
            if lead.email:
                field_stats["email"]["count"] += 1
                if "email" in lead.confidence_scores:
                    field_stats["email"]["confidence"].append(lead.confidence_scores["email"])
                    
            if lead.phone:
                field_stats["phone"]["count"] += 1
                if "phone" in lead.confidence_scores:
                    field_stats["phone"]["confidence"].append(lead.confidence_scores["phone"])
                    
            if lead.address:
                field_stats["address"]["count"] += 1
                if "address" in lead.confidence_scores:
                    field_stats["address"]["confidence"].append(lead.confidence_scores["address"])
                    
            if lead.website:
                field_stats["website"]["count"] += 1
                if "website" in lead.confidence_scores:
                    field_stats["website"]["confidence"].append(lead.confidence_scores["website"])
                    
            if lead.industry:
                field_stats["industry"]["count"] += 1
                
            if lead.services:
                field_stats["services"]["count"] += 1
        
        # Update stats data
        for i, field in enumerate(["email", "phone", "address", "website", "industry", "services"], 1):
            count = field_stats[field]["count"]
            fill_rate = (count / total_leads) * 100
            
            stats_data[i][1] = count
            stats_data[i][2] = f"{fill_rate:.1f}%"
            
            if "confidence" in field_stats[field] and field_stats[field]["confidence"]:
                avg_conf = sum(field_stats[field]["confidence"]) / len(field_stats[field]["confidence"])
                stats_data[i][3] = f"{avg_conf:.2f}"
        
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerows(stats_data)


class ExportManager:
    """Unified export management system"""
    
    def __init__(self, storage: LeadStorage):
        self.storage = storage
        self.json_exporter = JSONExporter()
        self.csv_exporter = CSVExporter()
        
    def export_filtered_leads(self,
                             output_path: str,
                             export_format: str = "json",
                             min_score: Optional[float] = None,
                             max_score: Optional[float] = None,
                             status: Optional[LeadStatus] = None,
                             industry: Optional[str] = None,
                             start_date: Optional[datetime] = None,
                             end_date: Optional[datetime] = None,
                             **export_kwargs) -> str:
        """Export leads with filtering capabilities"""
        
        # Apply filters
        leads = self.storage.filter_leads(
            min_score=min_score,
            max_score=max_score,
            status=status,
            industry=industry,
            start_date=start_date,
            end_date=end_date
        )
        
        filters = {
            "min_score": min_score,
            "max_score": max_score,
            "status": status.value if status else None,
            "industry": industry,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
        }
        
        logger.info(f"Exporting {len(leads)} filtered leads to {export_format}")
        
        if export_format.lower() == "json":
            return self.json_exporter.export_leads(leads, output_path, filters=filters, **export_kwargs)
        elif export_format.lower() == "csv":
            return self.csv_exporter.export_leads(leads, output_path, filters=filters, **export_kwargs)
        elif export_format.lower() == "excel":
            return self.csv_exporter.export_to_excel_sheets(leads, output_path, filters=filters)
        else:
            raise ValueError(f"Unsupported export format: {export_format}")
    
    def batch_export(self, 
                    output_dir: str,
                    formats: List[str] = ["json", "csv"],
                    batch_size: int = 1000) -> Dict[str, List[str]]:
        """Export all leads in batches across multiple formats"""
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        all_leads = self.storage.load_all_leads()
        exported_files = {fmt: [] for fmt in formats}
        
        # Process in batches
        for i in range(0, len(all_leads), batch_size):
            batch = all_leads[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            for fmt in formats:
                batch_file = output_path / f"leads_batch_{batch_num:03d}.{fmt}"
                
                if fmt == "json":
                    file_path = self.json_exporter.export_leads(batch, str(batch_file))
                elif fmt == "csv":
                    file_path = self.csv_exporter.export_leads(batch, str(batch_file))
                elif fmt == "excel":
                    file_path = self.csv_exporter.export_to_excel_sheets(batch, str(batch_file))
                else:
                    continue
                    
                exported_files[fmt].append(file_path)
        
        logger.info(f"Batch export completed: {len(all_leads)} leads in {len(exported_files['json'])} batches")
        return exported_files
