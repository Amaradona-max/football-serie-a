import logging
import sys
from pythonjsonlogger import jsonlogger
from app.core.config import settings

def configure_logging():
    """Configure structured JSON logging"""
    
    # Remove default handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Create JSON formatter
    formatter = jsonlogger.JsonFormatter(
        '%(asctime)s %(levelname)s %(name)s %(message)s'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # Set log level based on environment
    log_level = logging.DEBUG if settings.DEBUG else logging.INFO
    
    # Configure root logger
    logging.basicConfig(
        level=log_level,
        handlers=[console_handler],
        force=True
    )
    
    # Set specific log levels for noisy libraries
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('httpcore').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    
    # Add Sentry integration if configured
    if settings.SENTRY_DSN:
        import sentry_sdk
        from sentry_sdk.integrations.logging import LoggingIntegration
        
        sentry_logging = LoggingIntegration(
            level=logging.INFO,
            event_level=logging.ERROR
        )
        
        sentry_sdk.init(
            dsn=settings.SENTRY_DSN,
            integrations=[sentry_logging],
            environment=settings.ENVIRONMENT,
            traces_sample_rate=1.0 if settings.DEBUG else 0.1
        )
        
        logging.info("Sentry logging configured")
    
    logging.info("Logging configured successfully", extra={
        "environment": settings.ENVIRONMENT,
        "debug": settings.DEBUG
    })