"""
Query Processor Module for Monday.com BI Agent
Parses natural language queries and generates business intelligence insights.
"""
import re
from data_loader import (
    get_deals_data, get_work_orders_data, query_deals_by_sector,
    query_deals_by_stage, get_pipeline_summary, get_sector_performance,
    get_data_quality_notes
)

class QueryProcessor:
    def __init__(self):
        self.action_log = []
    
    def log_action(self, action):
        """Log an action for visibility."""
        self.action_log.append(action)
        print(f"[ACTION] {action}")
    
    def parse_query(self, query):
        """Parse a natural language query to extract intent and entities."""
        query = query.lower().strip()
        self.log_action(f"Parsing query: '{query}'")
        
        # Define sector keywords
        sectors = ['mining', 'powerline', 'renewables', 'dsp', 'railways', 'tender', 'energy']
        
        # Define stage keywords
        stages = ['sales qualified', 'feasibility', 'proposal', 'negotiations', 
                  'work order', 'on hold', 'closed']
        
        # Define time keywords
        quarters = ['q1', 'q2', 'q3', 'q4', 'quarter']
        years = ['2024', '2025', '2026']
        
        # Extract entities
        found_sectors = [s for s in sectors if s in query]
        found_stages = [s for s in stages if s in query]
        found_quarters = [q for q in quarters if q in query]
        found_years = [y for y in years if y in query]
        
        # Determine intent
        intent = 'general'
        if any(w in query for w in ['pipeline', 'deals', 'opportunities']):
            intent = 'pipeline'
        elif any(w in query for w in ['revenue', 'value', 'worth', 'money']):
            intent = 'revenue'
        elif any(w in query for w in ['sector', 'industry', 'vertical']):
            intent = 'sector'
        elif any(w in query for w in ['performance', 'how', 'looking']):
            intent = 'performance'
        
        result = {
            'intent': intent,
            'sectors': found_sectors,
            'stages': found_stages,
            'quarters': found_quarters,
            'years': found_years,
            'original_query': query
        }
        
        self.log_action(f"Parsed intent: {intent}, sectors: {found_sectors}, stages: {found_stages}")
        return result
    
    def process_query(self, query):
        """Process a query and return insights."""
        self.action_log = []  # Reset log
        parsed = self.parse_query(query)
        
        deals_df = get_deals_data()
        if deals_df is None or len(deals_df) == 0:
            return {
                'error': 'No deal data available. Please ensure data files are loaded.',
                'actions': self.action_log
            }
        
        # Route based on intent
        if parsed['intent'] == 'pipeline':
            return self.handle_pipeline_query(parsed, deals_df)
        elif parsed['intent'] == 'revenue':
            return self.handle_revenue_query(parsed, deals_df)
        elif parsed['intent'] == 'sector':
            return self.handle_sector_query(parsed, deals_df)
        elif parsed['intent'] == 'performance':
            return self.handle_performance_query(parsed, deals_df)
        else:
            return self.handle_general_query(parsed, deals_df)
    
    def handle_pipeline_query(self, parsed, df):
        """Handle pipeline-related queries."""
        self.log_action("Executing pipeline query handler")
        
        # Check if filtering by sector
        if parsed['sectors']:
            sector = parsed['sectors'][0]
            # Map energy to renewables for this dataset
            if sector == 'energy':
                sector = 'renewables'
            filtered = query_deals_by_sector(df, sector)
            summary = get_pipeline_summary(filtered)
            response = f"**Pipeline Overview for {sector.title()} Sector:**\n\n"
            response += f"- Total Deals: {summary['total_deals']}\n"
            response += f"- Total Value: ${summary['total_value']:,.2f}\n"
            response += f"- Open Deals: {summary['open_deals']}\n"
            response += f"- On Hold: {summary['on_hold']}\n"
            response += f"- Closed: {summary['closed']}"
        else:
            summary = get_pipeline_summary(df)
            response = f"**Overall Pipeline Overview:**\n\n"
            response += f"- Total Deals: {summary['total_deals']}\n"
            response += f"- Total Value: ${summary['total_value']:,.2f}\n"
            response += f"- Open Deals: {summary['open_deals']}\n"
            response += f"- On Hold: {summary['on_hold']}\n"
            response += f"- Closed: {summary['closed']}"
        
        return {'response': response, 'actions': self.action_log}
    
    def handle_revenue_query(self, parsed, df):
        """Handle revenue-related queries."""
        self.log_action("Executing revenue query handler")
        
        if parsed['sectors']:
            sector = parsed['sectors'][0]
            if sector == 'energy':
                sector = 'renewables'
            filtered = query_deals_by_sector(df, sector)
            total_value = filtered['Masked Deal value'].sum()
            response = f"**Revenue Analysis for {sector.title()} Sector:**\n\n"
            response += f"- Total Pipeline Value: ${total_value:,.2f}\n"
            response += f"- Number of Deals: {len(filtered)}\n"
            if len(filtered) > 0:
                response += f"- Average Deal Value: ${total_value/len(filtered):,.2f}"
        else:
            total_value = df['Masked Deal value'].sum()
            response = f"**Overall Revenue Analysis:**\n\n"
            response += f"- Total Pipeline Value: ${total_value:,.2f}\n"
            response += f"- Number of Deals: {len(df)}\n"
            response += f"- Average Deal Value: ${total_value/len(df):,.2f}"
        
        return {'response': response, 'actions': self.action_log}
    
    def handle_sector_query(self, parsed, df):
        """Handle sector-related queries."""
        self.log_action("Executing sector query handler")
        
        if parsed['sectors']:
            sector = parsed['sectors'][0]
            if sector == 'energy':
                sector = 'renewables'
            filtered = query_deals_by_sector(df, sector)
            summary = get_pipeline_summary(filtered)
            response = f"**{sector.title()} Sector Performance:**\n\n"
            response += f"- Total Deals: {summary['total_deals']}\n"
            response += f"- Total Value: ${summary['total_value']:,.2f}\n"
            response += f"- Open: {summary['open_deals']}, On Hold: {summary['on_hold']}, Closed: {summary['closed']}"
        else:
            sector_perf = get_sector_performance(df)
            response = "**Sector Performance Summary:**\n\n"
            for _, row in sector_perf.iterrows():
                response += f"**{row['Sector/service']}:**\n"
                response += f"  - Total Value: ${row['Total Value']:,.2f}\n"
                response += f"  - Deal Count: {int(row['Deal Count'])}\n"
                response += f"  - Open Deals: {int(row['Open Deals'])}\n\n"
        
        return {'response': response, 'actions': self.action_log}
    
    def handle_performance_query(self, parsed, df):
        """Handle performance-related queries."""
        self.log_action("Executing performance query handler")
        
        # If sector specified, show sector performance
        if parsed['sectors']:
            sector = parsed['sectors'][0]
            if sector == 'energy':
                sector = 'renewables'
            filtered = query_deals_by_sector(df, sector)
            stage_dist = filtered['Deal Stage'].value_counts()
            response = f"**Performance - {sector.title()} Sector:**\n\n"
            response += f"Deal Stage Distribution:\n"
            for stage, count in stage_dist.items():
                response += f"- {stage}: {count} deals\n"
        else:
            sector_perf = get_sector_performance(df)
            response = "**Overall Business Performance:**\n\n"
            response += f"Total Pipeline: ${df['Masked Deal value'].sum():,.2f}\n"
            response += f"Total Deals: {len(df)}\n\n"
            response += "**Top Sectors by Value:**\n"
            top_sectors = sector_perf.nlargest(3, 'Total Value')
            for _, row in top_sectors.iterrows():
                response += f"- {row['Sector/service']}: ${row['Total Value']:,.2f}\n"
        
        return {'response': response, 'actions': self.action_log}
    
    def handle_general_query(self, parsed, df):
        """Handle general queries."""
        self.log_action("Executing general query handler")
        
        summary = get_pipeline_summary(df)
        sector_perf = get_sector_performance(df)
        
        response = f"**Business Intelligence Summary:**\n\n"
        response += f"Here's what I found:\n\n"
        response += f"**Pipeline:** {summary['total_deals']} total deals worth ${summary['total_value']:,.2f}\n"
        response += f"**Open Deals:** {summary['open_deals']}\n"
        response += f"**On Hold:** {summary['on_hold']}\n\n"
        
        if not parsed['sectors']:
            response += "**Available Sectors:**\n"
            for sector in sector_perf['Sector/service'].unique():
                response += f"- {sector}\n"
            response += "\n*Try asking about a specific sector like 'Mining' or 'Energy'*"
        
        return {'response': response, 'actions': self.action_log}
    
    def get_help(self):
        """Return help information."""
        return {
            'response': """**Monday.com BI Agent - Available Queries:**

Here are some examples of questions you can ask:

**Pipeline Questions:**
- "How's our pipeline looking?"
- "Show me all open deals"
- "What's our pipeline for the energy sector?"

**Revenue Questions:**
- "What's our total revenue?"
- "How much is the mining sector worth?"
- "What's the average deal value?"

**Sector Questions:**
- "How is the renewables sector performing?"
- "Show sector breakdown"
- "Which sector has the most deals?"

**Performance Questions:**
- "How's business looking this quarter?"
- "Show performance by sector"
- "What's the status of all deals?"

**Tips:**
- Specify sectors: Mining, Powerline, Renewables, DSP, Railways, Tender
- Ask about specific stages or statuses
- Request specific time periods (2024, 2025, Q1, Q2, etc.)
""",
            'actions': self.action_log
        }
