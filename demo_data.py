"""
Demo data for Monday.com BI Agent
Sample data for testing and demonstrations
"""
import pandas as pd
from datetime import datetime, timedelta
import random

def create_demo_deals():
    """Create sample deals data for demo."""
    sectors = ['Mining', 'Powerline', 'Renewables', 'DSP', 'Railways', 'Tender']
    stages = ['Prospecting', 'Qualification', 'Proposal', 'Negotiation', 'Closed Won', 'Closed Lost']
    statuses = ['Open', 'On Hold', 'Closed Won', 'Closed Lost']
    products = ['Product A', 'Product B', 'Product C', 'Service Package']
    
    deals = []
    for i in range(50):
        deal = {
            'Deal Name': f'Deal {i+1:03d}',
            'Deal Status': random.choice(statuses),
            'Deal Stage': random.choice(stages),
            'Sector/service': random.choice(sectors),
            'Product deal': random.choice(products),
            'Masked Deal value': random.randint(10000, 500000),
            'Created Date': datetime.now() - timedelta(days=random.randint(1, 365)),
            'Close Date (A)': datetime.now() + timedelta(days=random.randint(30, 180)),
            'Tentative Close Date': datetime.now() + timedelta(days=random.randint(60, 240))
        }
        deals.append(deal)
    
    return pd.DataFrame(deals)


def create_demo_work_orders():
    """Create sample work orders data for demo."""
    statuses = ['Billed', 'Not Billed', 'Partially Billed', 'Cancelled']
    collection_statuses = ['Collected', 'Pending', 'Overdue', 'Not Applicable']
    
    work_orders = []
    for i in range(30):
        wo = {
            'WO Number': f'WO-{i+1:05d}',
            'Deal name masked': f'Deal {random.randint(1, 50):03d}',
            'WO Status (billed)': random.choice(statuses),
            'Collection status': random.choice(collection_statuses),
            'Billing Status': random.choice(statuses),
            'WO Value': random.randint(1000, 50000),
            'Created Date': datetime.now() - timedelta(days=random.randint(1, 180))
        }
        work_orders.append(wo)
    
    return pd.DataFrame(work_orders)


# Export dataframes
DEMO_DEALS = create_demo_deals()
DEMO_WORK_ORDERS = create_demo_work_orders()
