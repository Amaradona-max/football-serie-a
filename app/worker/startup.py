#!/usr/bin/env python3
"""
Startup script for Celery worker with adaptive configuration.
This script handles different running modes based on environment.
"""

import os
import sys
from datetime import datetime
from app.worker.celery_app import celery_app
from app.core.config import settings

def start_worker():
    """Start Celery worker with appropriate configuration"""
    
    # Set up environment
    os.environ.setdefault('FORKED_BY_MULTIPROCESSING', '1')
    
    # Determine worker type based on arguments or environment
    worker_type = "default"
    if len(sys.argv) > 1:
        worker_type = sys.argv[1]
    
    # Configure based on worker type
    if worker_type == "beat":
        # Start Celery beat scheduler
        print(f"Starting Celery beat scheduler at {datetime.now()}")
        celery_app.start(argv=['celery', 'beat', '-l', 'info'])
    
    elif worker_type == "live_data":
        # Start worker for live data queue
        print(f"Starting live data worker at {datetime.now()}")
        celery_app.start(argv=[
            'celery', 'worker', 
            '--queues=live_data',
            '--loglevel=info',
            '--hostname=live_data_worker@%h'
        ])
    
    elif worker_type == "static_data":
        # Start worker for static data queue
        print(f"Starting static data worker at {datetime.now()}")
        celery_app.start(argv=[
            'celery', 'worker', 
            '--queues=static_data',
            '--loglevel=info',
            '--hostname=static_data_worker@%h'
        ])
    
    else:
        # Start default worker
        print(f"Starting default worker at {datetime.now()}")
        celery_app.start(argv=['celery', 'worker', '--loglevel=info'])

if __name__ == '__main__':
    start_worker()