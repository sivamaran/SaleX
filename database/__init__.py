"""
Database module for Lead Generation Application
"""

from .mongodb_manager import MongoDBManager, get_mongodb_manager

__all__ = ['MongoDBManager', 'get_mongodb_manager']
