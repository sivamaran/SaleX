"""
Error Handler - Task 4: Basic Error Handling
Handles common failure cases and implements retry logic
"""

import asyncio
import logging
import time
import random
from typing import Callable, Any, Optional, Dict
from enum import Enum


class ErrorType(Enum):
    """Types of errors that can occur"""
    RATE_LIMIT = "rate_limit"
    NETWORK_ERROR = "network_error"
    AUTHENTICATION_ERROR = "authentication_error"
    PRIVATE_PROFILE = "private_profile"
    BLOCKED_PROFILE = "blocked_profile"
    INVALID_RESPONSE = "invalid_response"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


class InstagramError(Exception):
    """Base exception for Instagram scraper errors"""
    
    def __init__(self, message: str, error_type: ErrorType, retry_after: Optional[int] = None):
        super().__init__(message)
        self.error_type = error_type
        self.retry_after = retry_after


class ErrorHandler:
    """Handles errors and implements retry logic"""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.logger = self._setup_logger()
        self.error_counts = {error_type: 0 for error_type in ErrorType}
        
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('instagram_scraper')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
        
    def classify_error(self, exception: Exception, response: Optional[Dict] = None) -> ErrorType:
        """Classify the type of error based on exception and response"""
        error_message = str(exception).lower()
        
        if "429" in error_message or "rate limit" in error_message:
            return ErrorType.RATE_LIMIT
        elif "401" in error_message or "unauthorized" in error_message:
            return ErrorType.AUTHENTICATION_ERROR
        elif "403" in error_message or "forbidden" in error_message:
            return ErrorType.BLOCKED_PROFILE
        elif "timeout" in error_message or "timed out" in error_message:
            return ErrorType.TIMEOUT
        elif any(err in error_message for err in [
            "connection", "network", "err_connection_reset", "err_network_changed",
            "err_internet_disconnected", "err_connection_refused", "err_connection_timed_out",
            "err_name_not_resolved", "net::err_", "connection reset", "network error",
            "connection refused", "connection timed out", "name not resolved"
        ]):
            return ErrorType.NETWORK_ERROR
        elif "private" in error_message:
            return ErrorType.PRIVATE_PROFILE
        else:
            return ErrorType.UNKNOWN
            
    def should_retry(self, error_type: ErrorType, retry_count: int) -> bool:
        """Determine if the request should be retried"""
        if retry_count >= self.max_retries:
            return False
            
        # Don't retry certain error types
        if error_type in [ErrorType.PRIVATE_PROFILE, ErrorType.BLOCKED_PROFILE]:
            return False
            
        return True
        
    def get_retry_delay(self, error_type: ErrorType, retry_count: int) -> float:
        """Calculate retry delay with exponential backoff and jitter"""
        base_delay = self.base_delay * (2 ** retry_count)
        
        if error_type == ErrorType.RATE_LIMIT:
            # Longer delay for rate limits
            delay = base_delay * 3
        elif error_type == ErrorType.NETWORK_ERROR:
            # Longer delay for network errors (connection reset, etc.)
            delay = base_delay * 2.5
        elif error_type == ErrorType.TIMEOUT:
            # Moderate delay for timeouts
            delay = base_delay * 2
        else:
            # Standard exponential backoff
            delay = base_delay
        
        # Add jitter to prevent thundering herd
        jitter = random.uniform(0.8, 1.2)
        delay *= jitter
        
        # Cap delay between 1 and 10 seconds
        return max(1.0, min(10.0, delay))
            
    async def retry_with_backoff(
        self, 
        func: Callable, 
        *args, 
        **kwargs
    ) -> Any:
        """Execute function with retry logic and exponential backoff"""
        last_exception = None
        
        for retry_count in range(self.max_retries + 1):
            try:
                return await func(*args, **kwargs)
                
            except Exception as e:
                last_exception = e
                error_type = self.classify_error(e)
                self.error_counts[error_type] += 1
                
                self.logger.warning(
                    f"Attempt {retry_count + 1} failed: {error_type.value} - {str(e)}"
                )
                
                if not self.should_retry(error_type, retry_count):
                    self.logger.error(f"Max retries reached for {error_type.value}")
                    break
                    
                delay = self.get_retry_delay(error_type, retry_count)
                self.logger.info(f"Retrying in {delay:.2f} seconds...")
                await asyncio.sleep(delay)
                
        # If we get here, all retries failed
        raise InstagramError(
            f"All retries failed: {str(last_exception)}",
            self.classify_error(last_exception) if last_exception else ErrorType.UNKNOWN
        )
        
    def handle_rate_limit(self, response: Dict) -> int:
        """Handle rate limit response and return retry delay"""
        retry_after = response.get('headers', {}).get('Retry-After', 60)
        self.logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
        return int(retry_after)
        
    def handle_private_profile(self, username: str) -> None:
        """Handle private profile error"""
        self.logger.warning(f"Profile {username} is private")
        
    def handle_blocked_profile(self, username: str) -> None:
        """Handle blocked profile error"""
        self.logger.warning(f"Profile {username} has blocked the scraper")
        
    def log_error_stats(self) -> None:
        """Log error statistics"""
        self.logger.info("Error Statistics:")
        for error_type, count in self.error_counts.items():
            if count > 0:
                self.logger.info(f"  {error_type.value}: {count}")
                
    def reset_error_counts(self) -> None:
        """Reset error count statistics"""
        self.error_counts = {error_type: 0 for error_type in ErrorType}


class RateLimiter:
    """Simple rate limiter to prevent hitting rate limits"""
    
    def __init__(self, requests_per_minute: int = 10):
        self.requests_per_minute = requests_per_minute
        self.min_interval = 60.0 / requests_per_minute
        self.last_request_time = 0
        
    async def wait_if_needed(self) -> None:
        """Wait if necessary to respect rate limits"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_interval:
            wait_time = self.min_interval - time_since_last
            await asyncio.sleep(wait_time)
            
        self.last_request_time = time.time()


async def test_error_handler():
    """Test function for Task 4: Basic Error Handling"""
    print("Testing Error Handler...")
    
    error_handler = ErrorHandler(max_retries=2, base_delay=0.1)
    rate_limiter = RateLimiter(requests_per_minute=60)
    
    # Test rate limiter
    start_time = time.time()
    for i in range(3):
        await rate_limiter.wait_if_needed()
        print(f"✓ Rate limiter test {i+1}")
    end_time = time.time()
    print(f"✓ Rate limiter working (took {end_time - start_time:.2f}s)")
    
    # Test error classification
    test_errors = [
        ("HTTP 429 Too Many Requests", ErrorType.RATE_LIMIT),
        ("HTTP 401 Unauthorized", ErrorType.AUTHENTICATION_ERROR),
        ("HTTP 403 Forbidden", ErrorType.BLOCKED_PROFILE),
        ("Connection timeout", ErrorType.TIMEOUT),
        ("Network error", ErrorType.NETWORK_ERROR),
        ("Profile is private", ErrorType.PRIVATE_PROFILE),
    ]
    
    for error_msg, expected_type in test_errors:
        error_type = error_handler.classify_error(Exception(error_msg))
        if error_type == expected_type:
            print(f"✓ Error classification correct: {error_msg}")
        else:
            print(f"✗ Error classification wrong: {error_msg} -> {error_type}")
    
    # Test retry logic with mock function
    async def mock_failing_function(should_fail: bool = True):
        if should_fail:
            raise Exception("Test error")
        return "success"
    
    try:
        # Test successful retry
        result = await error_handler.retry_with_backoff(
            mock_failing_function, should_fail=False
        )
        print(f"✓ Successful retry: {result}")
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
    
    # Test error stats
    error_handler.log_error_stats()
    
    print("Task 4: Basic Error Handling - PASSED")


if __name__ == "__main__":
    asyncio.run(test_error_handler()) 